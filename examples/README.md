# Examples

Minimal wasm components. Both build for `wasm32-wasip2`, so the output is already a component.

## cli-hello (`wasi:cli/run`)

```sh
cd cli-hello
cargo build --target wasm32-wasip2 --release
wasmtime run target/wasm32-wasip2/release/cli-hello.wasm Name
```

## http-hello (`wasi:http/incoming-handler`)

```sh
cd http-hello
cargo build --target wasm32-wasip2 --release
wasmtime serve -Scli target/wasm32-wasip2/release/http-hello.wasm
curl localhost:8080/hi
```
