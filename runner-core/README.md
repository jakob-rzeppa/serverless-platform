# runner-core

Wasmtime host library shared by `runner-cli` and `runner-http`. It knows nothing about Kafka topics, log key layouts or which export a component has; those stay in the runner crates.

- `Runtime`: Wasmtime engine with epoch interruption plus the ticker that drives it (100 ms). `new_store(data, timeout)` makes a fresh store with the memory limiter and epoch deadline installed.
- `HostState`: WASI context, resource table and memory cap for one instance.
- `run_guest(timeout, fut)`: awaits a guest invocation, with a wall-clock backstop. Both kinds of timeout come back as `GuestError::Timeout`, and everything else as `GuestError::Error`.
- `ArtifactCache`: object store → sha256 check → disk cache (`<sha256>.wasm`) → compiled `Component`. It compiles on every call, so callers keep the `Component` if they reuse it.
- `CapturePipe`: capped stdout/stderr that drops excess output instead of trapping. `drain_log_lines` turns it into `LogLine`s (with an optional `request_id`).

A runner that needs more store data, like `WasiHttpCtx` for `wasi:http`, wraps `HostState` in its own type and implements `HasHostState` for it, so `Runtime::new_store` still installs the limits.

```sh
cargo test -p runner-core
```
