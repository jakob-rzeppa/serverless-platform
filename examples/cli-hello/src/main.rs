// A plain `main` compiled for wasm32-wasip2 becomes a component that
// exports `wasi:cli/run`. The host calls it once and it exits.
fn main() {
    let name = std::env::args().nth(1).unwrap_or_else(|| "world".to_string());
    println!("Hello, {name}!");
}
