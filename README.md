# Serverless Platform

A Lambda-like platform for running WebAssembly components.

- Users upload a WASM component and configure it (env vars, subdomain).
- Apps are **HTTP** components (`wasi:http`): long-lived, reachable via their own subdomain over HTTPS.
- Execution is done by Wasmtime (see README).

## Runtime

For running the wasm code [Wasmtime](https://github.com/bytecodealliance/wasmtime) is used. Every event is handled in a separate wasm process, which is spawned by the runtime.
