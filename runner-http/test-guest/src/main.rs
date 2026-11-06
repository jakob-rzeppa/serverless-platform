//! `wasi:http/incoming-handler` component used by runner-http's tests. The
//! path picks a behavior, so one artifact covers every outcome the runner
//! handles.

use std::{hint, time::Duration};

use wstd::http::{Body, Client, Error, Request, Response, StatusCode};

#[wstd::http_server]
async fn main(mut req: Request<Body>) -> Result<Response<Body>, Error> {
    let path = req.uri().path().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let num = |i: usize| -> u64 { parts[i].parse().expect("numeric path segment") };

    let body = match parts[..] {
        // Value of an env var.
        ["env", name] => std::env::var(name).unwrap_or_default(),
        // Value of a request header.
        ["header", name] => req
            .headers()
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default(),
        // Method and request body.
        ["echo"] => {
            let method = req.method().to_string();
            let body = req.body_mut().str_contents().await?.to_string();
            format!("{method} {body}")
        }
        // Empty response with this status.
        ["status", code] => {
            return Ok(Response::builder()
                .status(code.parse::<u16>().unwrap())
                .body(Body::empty())?);
        }
        // Sleep in a host call, then answer.
        ["sleep", _] => {
            wstd::task::sleep(Duration::from_millis(num(1)).into()).await;
            "slept".into()
        }
        // Busy-loop forever (only an epoch interrupt stops it).
        ["spin"] => loop {
            hint::spin_loop();
        },
        // Panic (prints to stderr, then traps).
        ["panic"] => panic!("boom"),
        // Allocate and touch <mib> MiB of memory.
        ["alloc", _] => {
            let v = vec![1u8; (num(1) as usize) << 20];
            hint::black_box(&v);
            "allocated".into()
        }
        // Handler error (wstd turns it into an error response code).
        ["error"] => return Err(wstd::http::Error::msg("handler failed")),
        // Outgoing request; the runner must deny it.
        ["fetch"] => {
            let out = Request::get("http://example.com/").body(Body::empty())?;
            return Ok(match Client::new().send(out).await {
                Ok(_) => Response::new("fetched".into()),
                Err(e) => Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body(format!("{e:?}").into())?,
            });
        }
        _ => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())?);
        }
    };
    Ok(Response::new(body.into()))
}
