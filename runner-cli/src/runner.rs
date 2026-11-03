//! The consume → execute → report loop.

use std::{future::Future, path::PathBuf, time::Duration};

use common::{
    bus::{
        Delivery, Publisher, Subscriber,
        messages::{
            cli_run::CliRun,
            run_event::{RunEvent, RunEventKind},
        },
    },
    storage::{
        ObjectStorage,
        objects::{artifact::Artifact, logs::CliLogChunk},
    },
};
use runner_core::ArtifactCache;

use crate::{config::Limits, logs::LogShipper, sandbox::Sandbox};

/// Adapters injected into the runner (Kafka/S3 in prod, mocks in tests).
pub struct Deps {
    pub runs: Subscriber<CliRun>,
    pub events: Publisher<RunEvent>,
    pub artifacts: ObjectStorage<Artifact>,
    pub logs: ObjectStorage<CliLogChunk>,
    pub cache_dir: PathBuf,
}

pub struct Runner {
    runs: Subscriber<CliRun>,
    events: Publisher<RunEvent>,
    logs: ObjectStorage<CliLogChunk>,
    artifacts: ArtifactCache,
    sandbox: Sandbox,
    limits: Limits,
}

impl Runner {
    /// Must be called inside a Tokio runtime.
    pub fn new(deps: Deps, limits: Limits) -> anyhow::Result<Self> {
        let sandbox = Sandbox::new()?;
        let artifacts = ArtifactCache::new(deps.artifacts, deps.cache_dir, sandbox.runtime());
        Ok(Self {
            runs: deps.runs,
            events: deps.events,
            logs: deps.logs,
            artifacts,
            sandbox,
            limits,
        })
    }

    /// Process messages one at a time until `shutdown` resolves. A run that
    /// is in progress when shutdown is requested is finished first.
    ///
    /// Receive errors (broker unreachable, rebalances, …) are transient and
    /// retried with a short backoff. Failing to publish an event or commit is
    /// returned as an error: the message is then left uncommitted, so it is
    /// redelivered after the process restarts.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
        const RECV_BACKOFF: Duration = Duration::from_secs(1);
        tokio::pin!(shutdown);
        loop {
            let msg = tokio::select! {
                _ = &mut shutdown => return Ok(()),
                msg = self.runs.recv() => msg,
            };
            match msg {
                Ok(msg) => self.handle(&msg).await?,
                Err(error) => {
                    tracing::warn!(%error, "receive failed, retrying");
                    tokio::select! {
                        _ = &mut shutdown => return Ok(()),
                        _ = tokio::time::sleep(RECV_BACKOFF) => {}
                    }
                }
            }
        }
    }

    /// Handle one message end to end: publish `started`, execute, publish
    /// `finished`/`failed` (after the logs are uploaded), then commit. An
    /// undecodable message is committed without publishing anything.
    pub async fn handle(&self, delivery: &Delivery<CliRun>) -> anyhow::Result<()> {
        let run = match &delivery.message {
            Ok(run) => run,
            Err(error) => {
                // Poison message: retrying cannot help. TODO: dead-letter topic.
                tracing::error!(topic = delivery.topic(), offset = delivery.offset(), %error, "skipping undecodable message");
                self.runs.commit(delivery).await?;
                return Ok(());
            }
        };
        let span = tracing::info_span!("run", run_id = %run.run_id, app_id = %run.app_id);
        let _enter = span.enter();

        self.events
            .publish(&RunEvent::new(run, RunEventKind::Started))
            .await?;
        let kind = self.execute(run).await;
        match &kind {
            RunEventKind::Finished { exit_code } => tracing::info!(exit_code, "run finished"),
            RunEventKind::Failed { error } => tracing::warn!(%error, "run failed"),
            RunEventKind::Started => {}
        }
        self.events.publish(&RunEvent::new(run, kind)).await?;
        self.runs.commit(delivery).await?;
        Ok(())
    }

    async fn execute(&self, run: &CliRun) -> RunEventKind {
        let component = match self
            .artifacts
            .component(&run.artifact_key, &run.sha256)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return RunEventKind::Failed {
                    error: e.to_string(),
                };
            }
        };

        let logs = LogShipper::start(
            self.logs.clone(),
            run.app_id,
            run.run_id,
            self.limits.log_max_bytes,
            self.limits.log_flush_interval,
        );
        let kind = self
            .sandbox
            .run(&component, run, &self.limits, logs.stdout(), logs.stderr())
            .await;
        // Upload the rest before reporting the final status, so logs are
        // complete once the run shows as done.
        logs.finish().await;
        kind
    }
}
