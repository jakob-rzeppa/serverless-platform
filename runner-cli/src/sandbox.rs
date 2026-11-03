//! Runs a component's `wasi:cli/run` export in a fresh sandbox.
//!
//! Engine, store limits and timeout handling come from `runner_core`; this
//! module adds the `wasi:cli/run` linker, args and exit-code handling.

use common::bus::messages::{cli_run::CliRun, run_event::RunEventKind};
use runner_core::{CapturePipe, GuestError, HostState, Runtime, run_guest};
use wasmtime::component::{Component, Linker};
use wasmtime_wasi::{I32Exit, WasiCtx, p2::bindings::Command};

use crate::config::Limits;

pub struct Sandbox {
    runtime: Runtime,
    linker: Linker<HostState>,
}

impl Sandbox {
    /// Must be called inside a Tokio runtime (spawns the epoch ticker).
    pub fn new() -> anyhow::Result<Self> {
        let runtime = Runtime::new()?;
        let mut linker = Linker::new(runtime.engine());
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        Ok(Self { runtime, linker })
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Run `component` with the args and env of `run`, writing its output to
    /// `stdout`/`stderr`. Returns `Finished` with the guest's exit code, or
    /// `Failed` on trap, timeout or instantiation failure.
    pub async fn run(
        &self,
        component: &Component,
        run: &CliRun,
        limits: &Limits,
        stdout: CapturePipe,
        stderr: CapturePipe,
    ) -> RunEventKind {
        let env: Vec<_> = run.env.iter().collect();
        let ctx = WasiCtx::builder()
            .envs(&env)
            .stdout(stdout)
            .stderr(stderr)
            .arg(run.app_id.to_string())
            .args(&run.args)
            .build();
        let timeout = limits.timeout_for(run);
        let mut store = self
            .runtime
            .new_store(HostState::new(ctx, limits.memory_bytes), timeout);

        let result = run_guest(timeout, async {
            let command = Command::instantiate_async(&mut store, component, &self.linker).await?;
            command.wasi_cli_run().call_run(&mut store).await
        })
        .await;

        let exit_code = match &result {
            Ok(Ok(())) => 0,
            Ok(Err(())) => 1,
            // `std::process::exit(n)` in the guest surfaces as an `I32Exit` error.
            Err(GuestError::Error(e)) if let Some(I32Exit(code)) = e.downcast_ref() => *code,
            Err(e) => {
                return RunEventKind::Failed {
                    error: e.to_string(),
                };
            }
        };
        RunEventKind::Finished { exit_code }
    }
}
