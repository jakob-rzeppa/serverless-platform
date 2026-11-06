# Serverless Platform

A Lambda-like platform for running WebAssembly components.

- Users upload a WASM component and configure it (env vars, subdomain).
- Apps are **HTTP** components (`wasi:http`): long-lived, reachable via their own subdomain over HTTPS.
- Execution is done by Wasmtime (see README).

## Runtime

For running the wasm code [Wasmtime](https://github.com/bytecodealliance/wasmtime) is used. Every event is handled in a separate wasm process, which is spawned by the runtime.

## Roadmap

- **App logging**: capture guest stdout/stderr per request (tagged with the request id), cap it per request and per app, upload it in chunks to a separate logs bucket in the object store, and let users read it through the control-plane API. Until then, runner-http discards guest output; platform-side failures only show up in the runner's own `tracing` output.
