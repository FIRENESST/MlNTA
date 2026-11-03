use serde::Deserialize;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::call::Timers;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub upstream: Vec<Upstream>,
    #[serde(default)]
    pub http: Vec<HttpListener>,
    #[serde(default)]
    pub grpc: Vec<GrpcListener>,
    /// On SIGINT/SIGTERM, in-flight calls get this long to finish.
    #[serde(default = "shutdown_grace_ms")]
    pub shutdown_grace_ms: u64,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub name: String,
    /// Standard gRPC endpoint: http:// for h2c, https:// for TLS.
    pub address: String,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub server_name: Option<String>,
    #[serde(default)]
    pub token_env: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub prefix: String,
    pub upstream: String,
    #[serde(default)]
    pub strip_prefix: bool,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpListener {
    pub listen: String,
    pub upstream: String,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default = "body_limit")]
    pub max_body_bytes: usize,
    #[serde(default = "body_limit")]
    pub max_response_bytes: usize,
    #[serde(default = "timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    #[serde(default)]
    pub max_duration_ms: u64,
    #[serde(default = "header_timeout_ms")]
    pub header_timeout_ms: u64,
    #[serde(default = "concurrency")]
    pub max_in_flight: usize,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcListener {
    pub listen: String,
    pub backend: String,
    #[serde(default)]
    pub backend_ca_file: Option<PathBuf>,
    #[serde(default)]
    pub cert_file: Option<PathBuf>,
    #[serde(default)]
    pub key_file: Option<PathBuf>,
    #[serde(default)]
    pub token_env: Option<String>,
    #[serde(default = "body_limit")]
    pub max_body_bytes: usize,
    #[serde(default = "timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "idle_timeout_ms")]
    pub idle_timeout_ms: u64,
    #[serde(default)]
    pub max_duration_ms: u64,
    #[serde(default = "concurrency")]
    pub max_in_flight: usize,
}

pub fn body_limit() -> usize {
    64 << 20
}
/// How long a call may go without progress before its response head arrives.
pub fn timeout_ms() -> u64 {
    60_000
}
/// After the response head: how long a stream or tunnel may move no data at all.
pub fn idle_timeout_ms() -> u64 {
    300_000
}
/// Time allowed for a client to send a complete HTTP/1.1 request head.
pub fn header_timeout_ms() -> u64 {
    30_000
}
pub fn shutdown_grace_ms() -> u64 {
    20_000
}
pub fn concurrency() -> usize {
    64
}

impl Default for Config {
    fn default() -> Self {
        Self {
            upstream: vec![],
            http: vec![],
            grpc: vec![],
            shutdown_grace_ms: shutdown_grace_ms(),
        }
    }
}

impl HttpListener {
    pub fn timers(&self) -> Timers {
        Timers::from_ms(self.timeout_ms, self.idle_timeout_ms, self.max_duration_ms)
    }
}

impl GrpcListener {
    pub fn timers(&self) -> Timers {
        Timers::from_ms(self.timeout_ms, self.idle_timeout_ms, self.max_duration_ms)
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        let mut names = HashSet::new();
        for u in &self.upstream {
            if u.name.is_empty() || !names.insert(u.name.as_str()) {
                return Err(format!("empty or duplicate upstream name {:?}", u.name));
            }
            let uri: http::Uri = u
                .address
                .parse()
                .map_err(|e| format!("upstream {}: {e}", u.name))?;
            if !matches!(uri.scheme_str(), Some("http" | "https"))
                || uri.authority().is_none()
                || uri.path() != "/"
                || uri.query().is_some()
            {
                return Err(format!(
                    "upstream {} must be an http:// or https:// gRPC authority without a path",
                    u.name
                ));
            }
            if uri.scheme_str() != Some("https") && (u.ca_file.is_some() || u.server_name.is_some())
            {
                return Err(format!("upstream {}: TLS options require https://", u.name));
            }
        }
        for h in &self.http {
            for name in std::iter::once(h.upstream.as_str())
                .chain(h.routes.iter().map(|r| r.upstream.as_str()))
            {
                if !names.contains(name) {
                    return Err(format!(
                        "HTTP listener {}: unknown upstream {name:?}",
                        h.listen
                    ));
                }
            }
            if h.max_in_flight == 0
                || h.timeout_ms == 0
                || h.idle_timeout_ms == 0
                || h.header_timeout_ms == 0
                || h.max_body_bytes == 0
                || h.max_response_bytes == 0
            {
                return Err(
                    "HTTP limits and timeouts must be positive (max_duration_ms 0 means no cap)"
                        .into(),
                );
            }
            if h.routes.iter().any(|r| !r.prefix.starts_with('/')) {
                return Err("route prefixes must start with /".into());
            }
        }
        for g in &self.grpc {
            if g.cert_file.is_some() != g.key_file.is_some() {
                return Err("gRPC TLS needs both cert_file and key_file".into());
            }
            if g.max_in_flight == 0
                || g.timeout_ms == 0
                || g.idle_timeout_ms == 0
                || g.max_body_bytes == 0
            {
                return Err(
                    "gRPC limits and timeouts must be positive (max_duration_ms 0 means no cap)"
                        .into(),
                );
            }
        }
        if self.http.is_empty() && self.grpc.is_empty() {
            return Err("configuration defines no listeners".into());
        }
        Ok(())
    }
}

pub fn token(var: &Option<String>) -> Result<Option<String>, String> {
    var.as_ref()
        .map(|name| {
            std::env::var(name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("token variable {name} is unset or empty"))
        })
        .transpose()
}
