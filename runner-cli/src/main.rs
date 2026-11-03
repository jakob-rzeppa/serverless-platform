use anyhow::Context;
use common::{
    bus::{Publisher, Subscriber, backend::kafka::KafkaProducer, messages::cli_run::CliRun},
    storage::ObjectStorage,
};
use runner_cli::{Config, Deps, Runner};
use tracing_subscriber::EnvFilter;

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
    let deps = Deps {
        runs: Subscriber::<CliRun>::kafka(&cfg.kafka_brokers, &cfg.kafka_group_id)?,
        // One producer connection, shared by all publishers.
        events: Publisher::kafka(&KafkaProducer::new(&cfg.kafka_brokers)?),
        artifacts: ObjectStorage::s3(&cfg.artifacts)?,
        logs: ObjectStorage::s3(&cfg.logs)?,
        cache_dir: cfg.cache_dir.clone(),
    };
    let runner = Runner::new(deps, cfg.limits.clone())?;

    tracing::info!(brokers = %cfg.kafka_brokers, group = %cfg.kafka_group_id, "runner-cli started");
    runner.run(shutdown_signal()).await?;
    tracing::info!("runner-cli stopped");
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
