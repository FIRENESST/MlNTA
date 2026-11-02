//! C ABI over `minta-beep`. See `include/beep.h` for the contract.

#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use minta_beep::rpc::{ClientOptions, RpcClient, RpcError, RpcFailure, RpcHandler, RpcProfile, RpcRequest, RpcServerRequest, RpcServerResponse, RPC_PROFILE};
use minta_beep::{Listener, Profiles};

pub const BEEP_OK: c_int = 0;
pub const BEEP_E_ARG: c_int = -1;
pub const BEEP_E_CONNECT: c_int = -2;
pub const BEEP_E_REMOTE: c_int = -3;
pub const BEEP_E_TIMEOUT: c_int = -4;
pub const BEEP_E_TRANSPORT: c_int = -5;
pub const BEEP_E_INTERNAL: c_int = -6;

/// Keep this library mapped for the life of the process. Hosts such as PHP unload FFI
/// libraries at shutdown; unmapping code that runtime worker threads are executing crashes.
fn pin_library() {
    #[cfg(windows)]
    {
        #[link(name = "kernel32")]
        extern "system" {
            fn GetModuleHandleExW(flags: u32, name: *const u16, module: *mut *mut c_void) -> i32;
        }
        const PIN: u32 = 0x1; // GET_MODULE_HANDLE_EX_FLAG_PIN
        const FROM_ADDRESS: u32 = 0x4; // GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS
        let mut module = ptr::null_mut();
        unsafe {
            GetModuleHandleExW(PIN | FROM_ADDRESS, pin_library as *const u16, &mut module);
        }
    }
    #[cfg(unix)]
    {
        #[repr(C)]
        struct DlInfo {
            fname: *const c_char,
            fbase: *mut c_void,
            sname: *const c_char,
            saddr: *mut c_void,
        }
        extern "C" {
            fn dladdr(addr: *const c_void, info: *mut DlInfo) -> c_int;
            fn dlopen(file: *const c_char, mode: c_int) -> *mut c_void;
        }
        #[cfg(target_os = "macos")]
        const NODELETE: c_int = 0x80;
        #[cfg(not(target_os = "macos"))]
        const NODELETE: c_int = 0x1000;
        const NOW: c_int = 2;
        unsafe {
            let mut info: DlInfo = std::mem::zeroed();
            if dladdr(pin_library as *const c_void, &mut info) != 0 && !info.fname.is_null() {
                dlopen(info.fname, NOW | NODELETE);
            }
        }
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        pin_library();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(std::thread::available_parallelism().map(|n| n.get().min(4)).unwrap_or(2))
            .thread_name("libbeep")
            .enable_all()
            .build()
            .expect("libbeep: cannot start runtime")
    })
}

/// Run a future to completion from any thread, including runtime worker threads.
fn block<F: std::future::Future + Send + 'static>(f: F) -> F::Output
where
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(f.await);
    });
    rx.recv().expect("libbeep: runtime task vanished")
}

thread_local! {
    static LAST: RefCell<(CString, c_int)> = RefCell::new((CString::default(), 0));
}

fn set_error(msg: impl Into<String>, code: c_int) {
    let msg = CString::new(msg.into().replace('\0', " ")).unwrap_or_default();
    LAST.with(|l| *l.borrow_mut() = (msg, code));
}

fn clear_error() {
    set_error("", 0);
}

fn guard<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => {
            set_error("internal panic in libbeep", 0);
            fallback
        }
    }
}

unsafe fn cstr<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        None
    } else {
        CStr::from_ptr(p).to_str().ok()
    }
}

unsafe fn bytes_arg(p: *const u8, len: usize) -> Bytes {
    if p.is_null() || len == 0 {
        Bytes::new()
    } else {
        Bytes::copy_from_slice(std::slice::from_raw_parts(p, len))
    }
}

fn parse_headers(s: Option<&str>) -> Vec<(String, String)> {
    s.unwrap_or("")
        .lines()
        .filter_map(|l| l.trim_end_matches('\r').split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .filter(|(k, _)| !k.is_empty())
        .collect()
}

fn to_c(s: &str) -> CString {
    CString::new(s.replace('\0', "")).unwrap_or_default()
}

fn header_block(h: &[(String, String)]) -> CString {
    let mut s = String::new();
    for (k, v) in h {
        s.push_str(k);
        s.push_str(": ");
        s.push_str(v);
        s.push_str("\r\n");
    }
    to_c(&s)
}

struct HeaderSet {
    block: CString,
    pairs: Vec<(String, CString)>,
}

impl HeaderSet {
    fn new(h: &[(String, String)]) -> Self {
        HeaderSet { block: header_block(h), pairs: h.iter().map(|(k, v)| (k.to_ascii_lowercase(), to_c(v))).collect() }
    }
    unsafe fn get(&self, name: *const c_char) -> *const c_char {
        match cstr(name) {
            Some(n) => {
                let n = n.to_ascii_lowercase();
                self.pairs.iter().find(|(k, _)| *k == n).map(|(_, v)| v.as_ptr()).unwrap_or(ptr::null())
            }
            None => ptr::null(),
        }
    }
}

/// Owned body with a trailing NUL so text bodies can be used as C strings.
fn nul_terminated(b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(b.len() + 1);
    v.extend_from_slice(b);
    v.push(0);
    v
}

// ---------------------------------------------------------------------------
// diagnostics

#[no_mangle]
pub extern "C" fn beep_version() -> *const c_char {
    static V: OnceLock<CString> = OnceLock::new();
    V.get_or_init(|| to_c(concat!("libbeep ", env!("CARGO_PKG_VERSION"), " (", "urn:minta:beep:rpc:1", ")"))).as_ptr()
}

#[no_mangle]
pub extern "C" fn beep_last_error() -> *const c_char {
    LAST.with(|l| l.borrow().0.as_ptr())
}

#[no_mangle]
pub extern "C" fn beep_last_error_code() -> c_int {
    LAST.with(|l| l.borrow().1)
}

// ---------------------------------------------------------------------------
// client

pub struct BeepClient {
    rpc: RpcClient,
}

pub struct BeepResponse {
    status: c_int,
    headers: HeaderSet,
    body: Vec<u8>,
}

fn new_client(address: Option<&str>, sessions: c_int, max_channels: c_int, timeout_ms: c_int, tls: Option<minta_beep::tls::TlsClientOptions>) -> *mut BeepClient {
    let Some(addr) = address.filter(|a| !a.is_empty()) else {
        set_error("address is required", 0);
        return ptr::null_mut();
    };
    let opts = ClientOptions {
        sessions: if sessions > 0 { sessions as usize } else { 1 },
        max_channels: if max_channels > 0 { max_channels as usize } else { 64 },
        request_timeout: if timeout_ms > 0 { Some(Duration::from_millis(timeout_ms as u64)) } else { None },
        tls,
        ..Default::default()
    };
    let _rt = runtime().enter();
    clear_error();
    Box::into_raw(Box::new(BeepClient { rpc: RpcClient::new(addr, opts) }))
}

#[no_mangle]
pub unsafe extern "C" fn beep_client_new(address: *const c_char, sessions: c_int, max_channels: c_int, timeout_ms: c_int) -> *mut BeepClient {
    guard(ptr::null_mut(), || new_client(cstr(address), sessions, max_channels, timeout_ms, None))
}

#[no_mangle]
pub unsafe extern "C" fn beep_client_new_tls(
    address: *const c_char,
    sessions: c_int,
    max_channels: c_int,
    timeout_ms: c_int,
    server_name: *const c_char,
    ca_file: *const c_char,
) -> *mut BeepClient {
    guard(ptr::null_mut(), || {
        let Some(name) = cstr(server_name) else {
            set_error("server_name is required for TLS", 0);
            return ptr::null_mut();
        };
        let tls = minta_beep::tls::TlsClientOptions { server_name: name.to_string(), ca_file: cstr(ca_file).map(Into::into) };
        new_client(cstr(address), sessions, max_channels, timeout_ms, Some(tls))
    })
}

#[no_mangle]
pub unsafe extern "C" fn beep_client_free(client: *mut BeepClient) {
    if client.is_null() {
        return;
    }
    guard((), || {
        let c = Box::from_raw(client);
        let rpc = c.rpc.clone();
        drop(c);
        block(async move { rpc.close().await });
    })
}

fn map_err(e: &RpcError) -> c_int {
    let code = match e {
        RpcError::Remote { code, .. } => *code as c_int,
        other => other.beep_code() as c_int,
    };
    set_error(e.to_string(), code);
    match e {
        RpcError::Remote { .. } => BEEP_E_REMOTE,
        RpcError::Timeout => BEEP_E_TIMEOUT,
        RpcError::Transport(minta_beep::Error::Io(_)) | RpcError::Transport(minta_beep::Error::Refused { .. }) => BEEP_E_CONNECT,
        RpcError::Transport(minta_beep::Error::Timeout) => BEEP_E_TIMEOUT,
        _ => BEEP_E_TRANSPORT,
    }
}

unsafe fn do_call(client: *mut BeepClient, method: *const c_char, target: *const c_char, headers: *const c_char, body: *const u8, body_len: usize) -> Result<BeepResponse, c_int> {
    let Some(c) = client.as_ref() else {
        set_error("client is NULL", 0);
        return Err(BEEP_E_ARG);
    };
    let (Some(method), Some(target)) = (cstr(method), cstr(target)) else {
        set_error("method and target are required (UTF-8)", 0);
        return Err(BEEP_E_ARG);
    };
    let req = RpcRequest { method: method.to_string(), target: target.to_string(), headers: parse_headers(cstr(headers)), body: bytes_arg(body, body_len) };
    let rpc = c.rpc.clone();
    match block(async move { rpc.call(req).await }) {
        Ok(r) => {
            clear_error();
            Ok(BeepResponse { status: r.status as c_int, headers: HeaderSet::new(&r.headers), body: nul_terminated(&r.body) })
        }
        Err(e) => Err(map_err(&e)),
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_call(
    client: *mut BeepClient,
    method: *const c_char,
    target: *const c_char,
    headers: *const c_char,
    body: *const u8,
    body_len: usize,
    out: *mut *mut BeepResponse,
) -> c_int {
    guard(BEEP_E_INTERNAL, || {
        if out.is_null() {
            set_error("out is NULL", 0);
            return BEEP_E_ARG;
        }
        *out = ptr::null_mut();
        match do_call(client, method, target, headers, body, body_len) {
            Ok(r) => {
                *out = Box::into_raw(Box::new(r));
                BEEP_OK
            }
            Err(code) => code,
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn beep_call_simple(
    client: *mut BeepClient,
    method: *const c_char,
    target: *const c_char,
    headers: *const c_char,
    body: *const u8,
    body_len: usize,
    out_body: *mut *mut u8,
    out_len: *mut usize,
) -> c_int {
    guard(BEEP_E_INTERNAL, || {
        if !out_body.is_null() {
            *out_body = ptr::null_mut();
        }
        if !out_len.is_null() {
            *out_len = 0;
        }
        match do_call(client, method, target, headers, body, body_len) {
            Ok(r) => {
                if !out_len.is_null() {
                    *out_len = r.body.len() - 1;
                }
                if !out_body.is_null() {
                    *out_body = alloc_buf(&r.body);
                }
                r.status
            }
            Err(code) => code,
        }
    })
}

/// Allocation with a length prefix so `beep_free` can release it without a size argument.
unsafe fn alloc_buf(data: &[u8]) -> *mut u8 {
    let total = data.len() + 16;
    let layout = std::alloc::Layout::from_size_align(total, 16).unwrap();
    let p = std::alloc::alloc(layout);
    if p.is_null() {
        return ptr::null_mut();
    }
    (p as *mut usize).write(total);
    ptr::copy_nonoverlapping(data.as_ptr(), p.add(16), data.len());
    p.add(16)
}

#[no_mangle]
pub unsafe extern "C" fn beep_free(p: *mut c_void) {
    if p.is_null() {
        return;
    }
    let base = (p as *mut u8).sub(16);
    let total = (base as *const usize).read();
    std::alloc::dealloc(base, std::alloc::Layout::from_size_align(total, 16).unwrap());
}

#[no_mangle]
pub unsafe extern "C" fn beep_response_status(r: *const BeepResponse) -> c_int {
    r.as_ref().map(|r| r.status).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn beep_response_body(r: *const BeepResponse, len: *mut usize) -> *const u8 {
    match r.as_ref() {
        Some(r) => {
            if !len.is_null() {
                *len = r.body.len() - 1;
            }
            r.body.as_ptr()
        }
        None => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_response_headers(r: *const BeepResponse) -> *const c_char {
    r.as_ref().map(|r| r.headers.block.as_ptr()).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_response_header(r: *const BeepResponse, name: *const c_char) -> *const c_char {
    r.as_ref().map(|r| r.headers.get(name)).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_response_free(r: *mut BeepResponse) {
    if !r.is_null() {
        drop(Box::from_raw(r));
    }
}

// ---------------------------------------------------------------------------
// server

pub type BeepHandler = unsafe extern "C" fn(user: *mut c_void, req: *const BeepRequest, reply: *mut BeepReply);

pub struct BeepRequest {
    method: CString,
    target: CString,
    headers: HeaderSet,
    body: Vec<u8>,
}

pub struct BeepReply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    error: Option<(u32, String)>,
}

pub struct BeepServer {
    port: c_int,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Copy)]
struct UserPtr(*mut c_void);
// The C caller promises the handler and user data are safe to use from worker threads.
unsafe impl Send for UserPtr {}
unsafe impl Sync for UserPtr {}

struct CHandler {
    f: BeepHandler,
    user: UserPtr,
}

#[async_trait::async_trait]
impl RpcHandler for CHandler {
    async fn handle(&self, req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let f = self.f;
        let user = self.user;
        let creq = BeepRequest { method: to_c(&req.method), target: to_c(&req.target), headers: HeaderSet::new(&req.headers), body: nul_terminated(&req.body) };
        let reply = tokio::task::spawn_blocking(move || {
            let user = user;
            let mut reply = BeepReply { status: 200, headers: vec![], body: vec![], error: None };
            let ok = catch_unwind(AssertUnwindSafe(|| unsafe { f(user.0, &creq, &mut reply) })).is_ok();
            (ok, reply)
        })
        .await;
        match reply {
            Ok((true, r)) => match r.error {
                Some((code, message)) => Err(RpcFailure::new(code, message)),
                None => Ok(RpcServerResponse::full(r.status, r.headers, r.body)),
            },
            _ => Err(RpcFailure::new(554, "handler crashed")),
        }
    }
}

fn start_server(listen: Option<&str>, handler: Option<BeepHandler>, user: *mut c_void, tls: Option<minta_beep::tls::TlsServerOptions>) -> *mut BeepServer {
    let (Some(listen), Some(f)) = (listen, handler) else {
        set_error("listen address and handler are required", 0);
        return ptr::null_mut();
    };
    let h = CHandler { f, user: UserPtr(user) };
    let listen = listen.to_string();
    let bound = block(async move {
        let profiles = Profiles::new().with(RPC_PROFILE, RpcProfile::new(h));
        let l = Listener::bind(listen.as_str(), minta_beep::Config::default(), profiles).await?;
        match tls {
            Some(t) => l.with_tls(&t),
            None => Ok(l),
        }
    });
    match bound {
        Ok(l) => {
            let port = l.local_addr().map(|a| a.port() as c_int).unwrap_or(0);
            let task = runtime().spawn(async move {
                let _ = l.serve().await;
            });
            clear_error();
            Box::into_raw(Box::new(BeepServer { port, task }))
        }
        Err(e) => {
            set_error(e.to_string(), 0);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_server_start(listen: *const c_char, handler: Option<BeepHandler>, user: *mut c_void) -> *mut BeepServer {
    guard(ptr::null_mut(), || start_server(cstr(listen), handler, user, None))
}

#[no_mangle]
pub unsafe extern "C" fn beep_server_start_tls(
    listen: *const c_char,
    handler: Option<BeepHandler>,
    user: *mut c_void,
    cert_file: *const c_char,
    key_file: *const c_char,
) -> *mut BeepServer {
    guard(ptr::null_mut(), || {
        let (Some(c), Some(k)) = (cstr(cert_file), cstr(key_file)) else {
            set_error("cert_file and key_file are required", 0);
            return ptr::null_mut();
        };
        start_server(cstr(listen), handler, user, Some(minta_beep::tls::TlsServerOptions { cert_file: c.into(), key_file: k.into() }))
    })
}

#[no_mangle]
pub unsafe extern "C" fn beep_server_port(s: *const BeepServer) -> c_int {
    s.as_ref().map(|s| s.port).unwrap_or(0)
}

#[no_mangle]
pub unsafe extern "C" fn beep_server_stop(s: *mut BeepServer) {
    if !s.is_null() {
        let s = Box::from_raw(s);
        s.task.abort();
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_request_method(r: *const BeepRequest) -> *const c_char {
    r.as_ref().map(|r| r.method.as_ptr()).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_request_target(r: *const BeepRequest) -> *const c_char {
    r.as_ref().map(|r| r.target.as_ptr()).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_request_headers(r: *const BeepRequest) -> *const c_char {
    r.as_ref().map(|r| r.headers.block.as_ptr()).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_request_header(r: *const BeepRequest, name: *const c_char) -> *const c_char {
    r.as_ref().map(|r| r.headers.get(name)).unwrap_or(ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn beep_request_body(r: *const BeepRequest, len: *mut usize) -> *const u8 {
    match r.as_ref() {
        Some(r) => {
            if !len.is_null() {
                *len = r.body.len() - 1;
            }
            r.body.as_ptr()
        }
        None => ptr::null(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_reply_status(r: *mut BeepReply, status: c_int) {
    if let Some(r) = r.as_mut() {
        if (100..=999).contains(&status) {
            r.status = status as u16;
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_reply_header(r: *mut BeepReply, name: *const c_char, value: *const c_char) {
    if let (Some(r), Some(n), Some(v)) = (r.as_mut(), cstr(name), cstr(value)) {
        r.headers.push((n.to_string(), v.to_string()));
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_reply_body(r: *mut BeepReply, body: *const u8, len: usize) {
    if let Some(r) = r.as_mut() {
        r.body = bytes_arg(body, len).to_vec();
    }
}

#[no_mangle]
pub unsafe extern "C" fn beep_reply_error(r: *mut BeepReply, code: c_int, message: *const c_char) {
    if let Some(r) = r.as_mut() {
        r.error = Some((code.max(0) as u32, cstr(message).unwrap_or("error").to_string()));
    }
}
