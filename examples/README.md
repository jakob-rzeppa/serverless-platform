# Examples

Minimal wasm components. They build for `wasm32-wasip2`, so the output is already a component.

## http-hello (`wasi:http/incoming-handler`)

```sh
cd http-hello
cargo build --target wasm32-wasip2 --release
wasmtime serve -Scli target/wasm32-wasip2/release/http-hello.wasm
curl localhost:8080/hi
```
