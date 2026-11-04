//! Applies `http.deploy` messages to the routing table.

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::Context;
use common::bus::{Delivery, Subscriber, messages::http_deploy::HttpDeploy};
use runner_core::ArtifactCache;
use wasmtime::component::Linker;

use crate::{
    apps::{App, AppTable, valid_subdomain},
    guest::{self, GuestState},
};

pub struct Deployer {
    pub deploys: Subscriber<HttpDeploy>,
    pub artifacts: ArtifactCache,
    pub linker: Linker<GuestState>,
    pub apps: Arc<AppTable>,
}

impl Deployer {
    /// Apply messages one at a time until `shutdown` resolves. Errors never
    /// stop the loop: receive errors are retried, a deploy that fails is
    /// logged and the app keeps serving its previous version (if any).
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        const RECV_BACKOFF: Duration = Duration::from_secs(1);
        tokio::pin!(shutdown);
        loop {
            let msg = tokio::select! {
                _ = &mut shutdown => return,
                msg = self.deploys.recv() => msg,
            };
            match msg {
                Ok(delivery) => self.handle(&delivery).await,
                Err(error) => {
                    tracing::warn!(%error, "receive failed, retrying");
                    tokio::select! {
                        _ = &mut shutdown => return,
                        _ = tokio::time::sleep(RECV_BACKOFF) => {}
                    }
                }
            }
        }
    }

    /// Apply one message, then commit it.
    pub async fn handle(&self, delivery: &Delivery<HttpDeploy>) {
        match &delivery.message {
            Ok(msg) => {
                let span = tracing::info_span!("deploy", app_id = %msg.app_id());
                let _enter = span.enter();
                if let Err(error) = self.apply(msg).await {
                    tracing::error!(
                        error = format!("{error:#}"),
                        "deploy failed, keeping previous version"
                    );
                }
            }
            // Poison message: retrying cannot help. TODO: dead-letter topic.
            Err(error) => {
                tracing::error!(topic = delivery.topic(), offset = delivery.offset(), %error, "skipping undecodable message")
            }
        }
        // Each replica replays the topic from the start on boot, so a failed
        // commit only means this message may be seen again; not fatal.
        if let Err(error) = self.deploys.commit(delivery).await {
            tracing::warn!(%error, "commit failed");
        }
    }

    async fn apply(&self, msg: &HttpDeploy) -> anyhow::Result<()> {
        match msg {
            HttpDeploy::Deploy {
                app_id,
                version_id,
                subdomain,
                artifact_key,
                sha256,
                env,
            } => {
                anyhow::ensure!(
                    valid_subdomain(subdomain),
                    "invalid subdomain {subdomain:?}"
                );
                let component = self.artifacts.component(artifact_key, sha256).await?;
                let pre = guest::prepare(&self.linker, &component)
                    .map_err(|e| anyhow::anyhow!("{e:#}"))
                    .context("not a wasi:http/proxy component")?;
                self.apps.insert(App {
                    app_id: *app_id,
                    version_id: *version_id,
                    subdomain: subdomain.clone(),
                    env: env.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                    pre,
                });
                tracing::info!(%version_id, %subdomain, "app deployed");
            }
            HttpDeploy::Remove { app_id } => {
                if self.apps.remove(*app_id) {
                    tracing::info!("app removed");
                }
            }
        }
        Ok(())
    }
}
