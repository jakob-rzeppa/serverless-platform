//! HTTP front end: resolves the `Host` header to an app and runs the request
//! in a fresh instance of it.

use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    Request, Response, StatusCode,
    body::Incoming,
    header::{HOST, HeaderValue},
};
use runner_core::{GuestError, HostState, Runtime, run_guest};
use tokio_util::task::TaskTracker;
use uuid::Uuid;
use wasmtime_wasi::WasiCtx;
use wasmtime_wasi_http::{
    WasiHttpView,
    p2::{bindings::http::types::Scheme, body::HyperOutgoingBody},
};

use crate::{
    apps::{App, AppTable},
    config::Limits,
    guest::GuestState,
};

/// Header carrying the request id, set on the request the guest sees and on
/// the response. A client-supplied value is replaced.
pub const REQUEST_ID: &str = "x-request-id";

/// Everything a request needs; shared by all connections.
pub struct Server {
    pub runtime: Runtime,
    pub apps: Arc<AppTable>,
    pub apps_domain: String,
    pub limits: Limits,
    /// Guest tasks, so shutdown can wait for them to finish.
    pub guests: TaskTracker,
}

enum Route {
    App(Arc<App>),
    Health,
    NotFound,
}

impl Server {
    pub async fn handle(
        self: Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<HyperOutgoingBody>, Infallible> {
        let request_id = Uuid::new_v4().to_string();
        let mut resp = match self.route(&req) {
            Route::App(app) => self.invoke(app, req, &request_id).await,
            Route::Health => text(StatusCode::OK, "ok"),
            Route::NotFound => text(StatusCode::NOT_FOUND, "no app is deployed at this host"),
        };
        resp.headers_mut().insert(
            REQUEST_ID,
            HeaderValue::from_str(&request_id).expect("uuid is a valid header"),
        );
        Ok(resp)
    }

    /// `<subdomain>.<apps_domain>` → that app. Any other host only answers
    /// `/healthz` (for probes that address the pod directly).
    fn route(&self, req: &Request<Incoming>) -> Route {
        let host = req
            .uri()
            .host()
            .map(str::to_string)
            .or_else(|| {
                let h = req.headers().get(HOST)?.to_str().ok()?;
                // Strip the port (IPv6 literals are never app hosts anyway).
                Some(h.rsplit_once(':').map_or(h, |(h, _)| h).to_string())
            })
            .unwrap_or_default()
            .to_ascii_lowercase();
        let host = host.trim_end_matches('.');
        match host
            .strip_suffix(&self.apps_domain)
            .and_then(|s| s.strip_suffix('.'))
        {
            Some(sub) => self.apps.get(sub).map_or(Route::NotFound, Route::App),
            None if req.uri().path() == "/healthz" => Route::Health,
            None => Route::NotFound,
        }
    }

    async fn invoke(
        &self,
        app: Arc<App>,
        mut req: Request<Incoming>,
        request_id: &str,
    ) -> Response<HyperOutgoingBody> {
        let timeout = self.limits.request_timeout;
        // No stdout/stderr configured: guest output is discarded.
        let ctx = WasiCtx::builder().envs(&app.env).build();
        let mut store = self.runtime.new_store(
            GuestState::new(HostState::new(ctx, self.limits.memory_bytes)),
            timeout,
        );

        let scheme = match req.headers().get("x-forwarded-proto") {
            Some(v) if v == "https" => Scheme::Https,
            _ => Scheme::Http,
        };
        req.headers_mut().insert(
            REQUEST_ID,
            HeaderValue::from_str(request_id).expect("uuid is a valid header"),
        );
        let (tx, rx) = tokio::sync::oneshot::channel();
        let resources = store
            .data_mut()
            .http()
            .new_incoming_request(scheme, req)
            .and_then(|req| Ok((req, store.data_mut().http().new_response_outparam(tx)?)));
        let (wasi_req, out) = match resources {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(app_id = %app.app_id, request_id, "invalid request: {e:#}");
                return text(StatusCode::BAD_REQUEST, "invalid request");
            }
        };

        // The guest runs on its own task: it may keep streaming the body
        // after the response head is sent.
        let guest = self.guests.spawn({
            let app = app.clone();
            let request_id = request_id.to_string();
            async move {
                let result = run_guest(timeout, async {
                    let proxy = app.pre.instantiate_async(&mut store).await?;
                    proxy
                        .wasi_http_incoming_handler()
                        .call_handle(&mut store, wasi_req, out)
                        .await
                })
                .await;
                if let Err(error) = &result {
                    tracing::warn!(app_id = %app.app_id, request_id, %error, "request failed");
                }
                result
            }
        });

        match rx.await {
            Ok(Ok(resp)) => resp,
            Ok(Err(code)) => {
                tracing::warn!(app_id = %app.app_id, request_id, "guest returned error: {code:?}");
                text(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
            // The guest finished (or was stopped) without sending a response.
            Err(_) => match guest.await {
                Ok(Err(GuestError::Timeout(_))) => {
                    text(StatusCode::GATEWAY_TIMEOUT, "request timed out")
                }
                Ok(Ok(())) => {
                    tracing::warn!(app_id = %app.app_id, request_id, "guest sent no response");
                    text(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
                }
                Ok(Err(GuestError::Error(_))) | Err(_) => {
                    text(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
                }
            },
        }
    }
}

/// Plain-text response from the platform itself (not the guest).
fn text(status: StatusCode, body: &'static str) -> Response<HyperOutgoingBody> {
    let body = Full::new(Bytes::from(format!("{body}\n")))
        .map_err(|never| match never {})
        .boxed_unsync();
    let mut resp = Response::new(body);
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}
