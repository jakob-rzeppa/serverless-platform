//! Store data and linker for `wasi:http/proxy` components.
//!
//! The guest gets the WASI interfaces from `runner_core` plus `wasi:http`.
//! Outgoing HTTP requests (`wasi:http/outgoing-handler`) are linked, so
//! components that import it still instantiate, but every request is denied:
//! guests have no network access.

use runner_core::{HasHostState, HostState};
use wasmtime::{
    Engine,
    component::{Component, Linker},
};
use wasmtime_wasi::{WasiCtxView, WasiView};
use wasmtime_wasi_http::{
    Error as HttpError, RequestOptions, WasiBody, WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks,
    WasiHttpView, p2::bindings::ProxyPre,
};

/// Store data of one request's instance.
pub struct GuestState {
    host: HostState,
    http: WasiHttpCtx,
    hooks: DenyOutgoing,
}

impl GuestState {
    pub fn new(host: HostState) -> Self {
        Self {
            host,
            http: WasiHttpCtx::new(),
            hooks: DenyOutgoing,
        }
    }
}

impl HasHostState for GuestState {
    fn host_state(&mut self) -> &mut HostState {
        &mut self.host
    }
}

impl WasiView for GuestState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.host.ctx,
            table: &mut self.host.table,
        }
    }
}

impl WasiHttpView for GuestState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.host.table,
            hooks: &mut self.hooks,
        }
    }
}

/// Rejects every outgoing request with `HTTP-request-denied`.
struct DenyOutgoing;

impl WasiHttpHooks for DenyOutgoing {
    fn send_request(
        &mut self,
        _request: http::Request<WasiBody>,
        _options: Option<RequestOptions>,
        _fut: Box<dyn Future<Output = Result<(), HttpError>> + Send>,
    ) -> Box<
        dyn Future<
                Output = Result<
                    (
                        http::Response<WasiBody>,
                        Box<dyn Future<Output = Result<(), HttpError>> + Send>,
                    ),
                    HttpError,
                >,
            > + Send,
    > {
        Box::new(async { Err(HttpError::HttpRequestDenied) })
    }
}

/// Linker with WASI and `wasi:http` for [`GuestState`].
pub fn linker(engine: &Engine) -> wasmtime::Result<Linker<GuestState>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;
    Ok(linker)
}

/// Pre-instantiate `component`, so each request only needs a cheap
/// instantiation. Fails if it imports something the linker does not provide
/// or does not export `wasi:http/incoming-handler`.
pub fn prepare(
    linker: &Linker<GuestState>,
    component: &Component,
) -> wasmtime::Result<ProxyPre<GuestState>> {
    ProxyPre::new(linker.instantiate_pre(component)?)
}
