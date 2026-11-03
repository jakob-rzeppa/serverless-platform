# Serverless Platform

A Lambda-like platform for running WebAssembly components.

- Users upload a WASM component and configure it (env vars, subdomain, schedule).
- Two workload kinds:
  - **CLI** (`wasi:cli`) – runs once to completion, either on demand or on a schedule.
  - **HTTP** (`wasi:http`) – long-lived, reachable via its own subdomain over HTTPS.
- Execution is done by Wasmtime (see README).

## Runtime

For running the wasm code [Wasmtime](https://github.com/bytecodealliance/wasmtime) is used. Every event is handled in a separate wasm process, which is spawned by the runtime.
