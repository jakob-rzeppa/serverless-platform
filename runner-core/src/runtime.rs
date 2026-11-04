//! Wasmtime engine, per-instance stores and guest timeouts.
//!
//! Every invocation (CLI run or HTTP request) gets a fresh `Store`: no state
//! is shared between invocations. Limits: linear memory cap, epoch-based
//! wall-clock timeout, no filesystem preopens, no network (wasmtime-wasi
//! denies sockets by default).
//!
//! Timeouts use epoch interruption: a background thread increments the
//! engine's epoch every [`EPOCH_TICK`]. On every tick a running guest yields
//! to the async executor (so a CPU-bound guest cannot monopolize a Tokio
//! worker), and once its deadline (in ticks) has passed it traps.
//!
//! The ticker is an OS thread, not a Tokio task: a task could be queued on
//! the very worker a spinning guest occupies and then never run, so the
//! deadline would never arrive.

use std::{
    future::Future,
    sync::mpsc::{self, RecvTimeoutError},
    time::Duration,
};

use wasmtime::{
    Config, Engine, Store, StoreLimits, StoreLimitsBuilder, Trap, UpdateDeadline,
    component::ResourceTable,
};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

/// Epoch tick; timeouts are rounded up to a multiple of this.
const EPOCH_TICK: Duration = Duration::from_millis(100);

/// Extra time [`run_guest`] waits past the epoch deadline before giving up
/// on a guest stuck in a host call that never reaches an epoch check.
const BACKSTOP_GRACE: Duration = Duration::from_secs(5);

/// Wasmtime engine plus the epoch ticker that enforces store timeouts.
/// Create one per process and share it.
pub struct Runtime {
    engine: Engine,
    _ticker: EpochTicker,
}

impl Runtime {
    /// Starts the epoch ticker thread; it stops when the runtime is dropped.
    pub fn new() -> wasmtime::Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;
        let _ticker = EpochTicker::start(&engine);
        Ok(Self { engine, _ticker })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Fresh store with the memory limiter installed and an epoch deadline of
    /// `timeout`, after which the guest traps. The guest yields to the
    /// executor on every epoch tick until then, so it must be called through
    /// wasmtime's `*_async` methods. Run the guest with [`run_guest`] so the
    /// trap is reported as [`GuestError::Timeout`].
    pub fn new_store<T: HasHostState + Send + 'static>(
        &self,
        data: T,
        timeout: Duration,
    ) -> Store<T> {
        let mut store = Store::new(&self.engine, data);
        store.limiter(|s| &mut s.host_state().limits);
        let mut remaining = deadline_ticks(timeout);
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_| {
            remaining -= 1;
            Ok(if remaining == 0 {
                UpdateDeadline::Interrupt
            } else {
                UpdateDeadline::Yield(1)
            })
        });
        store
    }
}

/// WASI context, resource table and memory limits of one instance.
pub struct HostState {
    /// WASI context (env, stdio) exposed to the guest.
    pub ctx: WasiCtx,
    /// Resource handles owned by the guest.
    pub table: ResourceTable,
    limits: StoreLimits,
}

impl HostState {
    /// Host state whose guest linear memory may grow to at most
    /// `memory_bytes`.
    pub fn new(ctx: WasiCtx, memory_bytes: usize) -> Self {
        Self {
            ctx,
            table: ResourceTable::new(),
            limits: StoreLimitsBuilder::new().memory_size(memory_bytes).build(),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

/// Store data that contains a [`HostState`]. Runners that need more host
/// state (e.g. `WasiHttpCtx`) wrap `HostState` in their own type and
/// implement this, so [`Runtime::new_store`] still installs the limits.
pub trait HasHostState {
    fn host_state(&mut self) -> &mut HostState;
}

impl HasHostState for HostState {
    fn host_state(&mut self) -> &mut HostState {
        self
    }
}

/// Why a guest invocation did not complete.
#[derive(Debug, thiserror::Error)]
pub enum GuestError {
    /// The epoch deadline passed, or the guest got stuck past it.
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    /// Trap, instantiation failure, explicit exit (`I32Exit`), …
    #[error("{0:#}")]
    Error(wasmtime::Error),
}

/// Await a guest invocation on a store made by [`Runtime::new_store`] with
/// the same `timeout`. Handles both ways a timeout shows up: the epoch trap,
/// and a wall-clock backstop for guests blocked in a host call.
pub async fn run_guest<R>(
    timeout: Duration,
    fut: impl Future<Output = wasmtime::Result<R>>,
) -> Result<R, GuestError> {
    match tokio::time::timeout(timeout + BACKSTOP_GRACE, fut).await {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) => {
            Err(GuestError::Timeout(timeout))
        }
        Ok(Err(e)) => Err(GuestError::Error(e)),
        Err(_) => Err(GuestError::Timeout(timeout)),
    }
}

/// Number of epoch ticks covering `timeout` (at least one).
fn deadline_ticks(timeout: Duration) -> u64 {
    timeout.as_millis().div_ceil(EPOCH_TICK.as_millis()).max(1) as u64
}

/// Thread that increments the engine's epoch every [`EPOCH_TICK`] until
/// dropped (dropping the sender wakes it and ends the loop).
struct EpochTicker(#[allow(dead_code)] mpsc::Sender<()>);

impl EpochTicker {
    fn start(engine: &Engine) -> Self {
        let engine = engine.clone();
        let (stop, ticks) = mpsc::channel::<()>();
        std::thread::Builder::new()
            .name("epoch-ticker".into())
            .spawn(move || {
                while let Err(RecvTimeoutError::Timeout) = ticks.recv_timeout(EPOCH_TICK) {
                    engine.increment_epoch();
                }
            })
            .expect("spawn epoch ticker thread");
        Self(stop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_rounds_up() {
        assert_eq!(deadline_ticks(Duration::ZERO), 1);
        assert_eq!(deadline_ticks(Duration::from_millis(100)), 1);
        assert_eq!(deadline_ticks(Duration::from_millis(101)), 2);
        assert_eq!(deadline_ticks(Duration::from_secs(5)), 50);
    }
}
