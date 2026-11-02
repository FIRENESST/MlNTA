use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub upstream: Vec<Upstream>,
    /// Egress: HTTP/1.1 + h2c listeners (apps → BEEP).
    #[serde(default)]
    pub http: Vec<HttpListener>,
    /// Egress: TCP JSON-lines listeners (apps → BEEP).
    #[serde(default)]
    pub jsonl: Vec<JsonlListener>,
    /// Ingress: BEEP listeners forwarding to HTTP backends (BEEP → apps).
    #[serde(default)]
    pub beep: Vec<BeepListener>,
    /// Built-in RPC test servers.
    #[serde(default)]
    pub echo: Vec<EchoListener>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub name: String,
    pub address: String,
    #[serde(default = "d_sessions")]
    pub sessions: usize,
    #[serde(default = "d_channels")]
    pub max_channels: usize,
    #[serde(default = "d_connect_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "d_request_ms")]
    pub request_timeout_ms: u64,
    #[serde(default)]
    pub tls: Option<ClientTls>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    pub server_name: String,
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub prefix: String,
    pub upstream: String,
    #[serde(default)]
    pub strip_prefix: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpListener {
    pub listen: String,
    pub upstream: String,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default = "d_body")]
    pub max_body_bytes: usize,
    /// Add X-Forwarded-For / X-Forwarded-Proto.
    #[serde(default = "d_true")]
    pub forwarded_headers: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlListener {
    pub listen: String,
    pub upstream: String,
    #[serde(default = "d_body")]
    pub max_body_bytes: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeepListener {
    pub listen: String,
    /// Base URL of the HTTP backend, e.g. `http://127.0.0.1:3000` or `http://10.0.0.5/api`.
    pub backend: String,
    #[serde(default = "d_rpy_max")]
    pub rpy_max_bytes: usize,
    #[serde(default = "d_request_ms")]
    pub backend_timeout_ms: u64,
    #[serde(default)]
    pub tls: Option<ServerTls>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EchoListener {
    pub listen: String,
    #[serde(default)]
    pub tls: Option<ServerTls>,
}

fn d_sessions() -> usize {
    1
}
fn d_channels() -> usize {
    64
}
fn d_connect_ms() -> u64 {
    5_000
}
fn d_request_ms() -> u64 {
    30_000
}
fn d_body() -> usize {
    64 << 20
}
fn d_rpy_max() -> usize {
    8 << 20
}
fn d_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let cfg: Config = toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        let names: Vec<&str> = self.upstream.iter().map(|u| u.name.as_str()).collect();
        let check = |n: &str, ctx: &str| {
            if names.contains(&n) {
                Ok(())
            } else {
                Err(format!("{ctx} refers to unknown upstream {n:?}"))
            }
        };
        for h in &self.http {
            check(&h.upstream, &format!("http listener {}", h.listen))?;
            for r in &h.routes {
                check(&r.upstream, &format!("route {}", r.prefix))?;
            }
        }
        for j in &self.jsonl {
            check(&j.upstream, &format!("jsonl listener {}", j.listen))?;
        }
        if self.http.is_empty() && self.jsonl.is_empty() && self.beep.is_empty() && self.echo.is_empty() {
            return Err("configuration defines no listeners".into());
        }
        Ok(())
    }
}
