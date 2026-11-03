use std::collections::HashMap;
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
    /// Authenticate to the upstream with SASL PLAIN.
    #[serde(default)]
    pub sasl: Option<SaslClient>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslClient {
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    /// Read the password from this environment variable instead.
    #[serde(default)]
    pub password_env: Option<String>,
    /// Identity to act as (empty = `user`).
    #[serde(default)]
    pub authzid: String,
}

impl SaslClient {
    pub fn credentials(&self) -> Result<minta_beep::sasl::PlainCredentials, String> {
        let password = match (&self.password, &self.password_env) {
            (Some(p), _) => p.clone(),
            (None, Some(var)) => std::env::var(var).map_err(|_| format!("SASL password variable {var} is not set"))?,
            (None, None) => return Err(format!("SASL user {:?} needs password or password_env", self.user)),
        };
        Ok(minta_beep::sasl::PlainCredentials { authzid: self.authzid.clone(), authcid: self.user.clone(), password })
    }
}

/// SASL PLAIN users a BEEP listener accepts; when present, authentication is required.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ServerAuth {
    /// One `user:password` or `user:sha256:<hex>` per line (`#` comments).
    #[serde(default)]
    pub users_file: Option<PathBuf>,
    /// Inline users, same value syntax.
    #[serde(default)]
    pub users: HashMap<String, String>,
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
    /// Base URL of the HTTP backend: `http://127.0.0.1:3000`, `http://10.0.0.5/api`,
    /// `h2c://127.0.0.1:50051` (HTTP/2 cleartext, e.g. gRPC) or `https://api.internal`.
    pub backend: String,
    /// PEM CA bundle trusted for an `https://` backend (default: Mozilla roots).
    #[serde(default)]
    pub backend_ca_file: Option<PathBuf>,
    #[serde(default = "d_rpy_max")]
    pub rpy_max_bytes: usize,
    #[serde(default = "d_request_ms")]
    pub backend_timeout_ms: u64,
    #[serde(default)]
    pub tls: Option<ServerTls>,
    /// Require SASL PLAIN; the identity reaches the backend as `X-Beep-User`.
    #[serde(default)]
    pub auth: Option<ServerAuth>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EchoListener {
    pub listen: String,
    #[serde(default)]
    pub tls: Option<ServerTls>,
    #[serde(default)]
    pub auth: Option<ServerAuth>,
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
