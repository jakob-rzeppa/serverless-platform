use wstd::http::{Body, Request, Response};

// `#[wstd::http_server]` turns this function into the component's
// `wasi:http/incoming-handler` export. The host calls it once per request.
#[wstd::http_server]
async fn main(req: Request<Body>) -> Result<Response<Body>, wstd::http::Error> {
    let body = format!("Hello from wasm! You requested {}\n", req.uri().path());
    Ok(Response::new(body.into()))
}
