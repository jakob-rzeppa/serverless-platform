# Serverless Platform

A aws-lambda like serverless platform that allows you to run wasm applications in a serverless environment.

## Runtime

For running the wasm code [Wasmtime](https://github.com/bytecodealliance/wasmtime) is used. Every event is handled in a separate wasm process, which is spawned by the runtime.
