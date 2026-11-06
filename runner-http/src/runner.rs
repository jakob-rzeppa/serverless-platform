//! Wires the deploy consumer and the HTTP server together.

use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};

use common::{
    bus::{Subscriber, messages::http_deploy::HttpDeploy},
    storage::{ObjectStorage, objects::artifact::Artifact},
};
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::{rt::TokioIo, server::graceful::GracefulShutdown};
use runner_core::{ArtifactCache, Runtime};
use tokio::{net::TcpListener, sync::watch};
use tokio_util::task::TaskTracker;

use crate::{apps::AppTable, config::Limits, deploy::Deployer, guest, server::Server};

/// Adapters injected into the runner (Kafka/S3 in prod, mocks in tests).
pub struct Deps {
    pub deploys: Subscriber<HttpDeploy>,
    pub artifacts: ObjectStorage<Artifact>,
    pub cache_dir: PathBuf,
}

/// Non-adapter settings.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Apps are served at `<subdomain>.<apps_domain>`.
    pub apps_domain: String,
    pub limits: Limits,
}

pub struct Runner {
    server: Arc<Server>,
    deployer: Deployer,
}

impl Runner {
    /// Must be called inside a Tokio runtime.
    pub fn new(deps: Deps, settings: Settings) -> anyhow::Result<Self> {
        let runtime = Runtime::new()?;
        let apps = Arc::new(AppTable::default());
        let deployer = Deployer {
            deploys: deps.deploys,
            artifacts: ArtifactCache::new(deps.artifacts, deps.cache_dir, &runtime),
            linker: guest::linker(runtime.engine())?,
            apps: apps.clone(),
        };
        let server = Arc::new(Server {
            runtime,
            apps,
            apps_domain: settings.apps_domain.trim_matches('.').to_ascii_lowercase(),
            limits: settings.limits,
            guests: TaskTracker::new(),
        });
        Ok(Self { server, deployer })
    }

    /// Serve `listener` and apply deploys until `shutdown` resolves.
    ///
    /// On shutdown: stop accepting connections, let in-flight requests finish
    /// (up to the request timeout plus a grace period), then stop consuming
    /// deploys.
    pub async fn run(
        self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let (stop_tx, stop_rx) = watch::channel(false);
        let stopped = move || {
            let mut rx = stop_rx.clone();
            async move {
                let _ = rx.wait_for(|stop| *stop).await;
            }
        };

        let deployer = tokio::spawn({
            let stop = stopped();
            async move { self.deployer.run(stop).await }
        });

        let graceful = GracefulShutdown::new();
        tokio::pin!(shutdown);
        loop {
            let stream = tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        // e.g. too many open files: back off instead of spinning.
                        tracing::warn!(%error, "accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                },
            };
            let server = self.server.clone();
            let conn = http1::Builder::new().serve_connection(
                TokioIo::new(stream),
                service_fn(move |req| server.clone().handle(req)),
            );
            let conn = graceful.watch(conn);
            tokio::spawn(async move {
                if let Err(error) = conn.await {
                    tracing::debug!(%error, "connection closed with error");
                }
            });
        }
        drop(listener);
        tracing::info!("shutting down: waiting for in-flight requests");

        let grace = self.server.limits.request_timeout + Duration::from_secs(10);
        if tokio::time::timeout(grace, graceful.shutdown())
            .await
            .is_err()
        {
            tracing::warn!("connections still open after {grace:?}, closing them");
        }
        let guests = &self.server.guests;
        guests.close();
        if tokio::time::timeout(grace, guests.wait()).await.is_err() {
            tracing::warn!("guest tasks still running after {grace:?}");
        }

        let _ = stop_tx.send(true);
        let _ = deployer.await;
        Ok(())
    }
}
