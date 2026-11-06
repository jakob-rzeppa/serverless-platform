use anyhow::Context;
use common::{
    bus::{Subscriber, messages::http_deploy::HttpDeploy},
    storage::ObjectStorage,
};
use runner_http::{Config, Deps, Runner, Settings};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let loaded = common::env::load_dotenv(env!("CARGO_MANIFEST_DIR"));
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    for path in loaded {
        tracing::debug!(path = %path.display(), "loaded env file");
    }

    let cfg = Config::from_env().context("invalid configuration")?;
    // Unique per process start: a restarted container must not share a
    // consumer group (it has to replay every deploy).
    let instance = format!(
        "{}.{}",
        cfg.pod_name,
        &Uuid::new_v4().simple().to_string()[..8]
    );
    let group_id = format!("runner-http.{instance}");
    let deps = Deps {
        deploys: Subscriber::<HttpDeploy>::kafka(&cfg.kafka_brokers, &group_id)?,
        artifacts: ObjectStorage::s3(&cfg.artifacts)?,
        cache_dir: cfg.cache_dir.clone(),
    };
    let runner = Runner::new(
        deps,
        Settings {
            apps_domain: cfg.apps_domain.clone(),
            limits: cfg.limits.clone(),
        },
    )?;
    let listener = TcpListener::bind(cfg.listen_addr)
        .await
        .with_context(|| format!("bind {}", cfg.listen_addr))?;

    tracing::info!(addr = %cfg.listen_addr, apps_domain = %cfg.apps_domain, %instance, group = %group_id, "runner-http started");
    runner.run(listener, shutdown_signal()).await?;
    tracing::info!("runner-http stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}
