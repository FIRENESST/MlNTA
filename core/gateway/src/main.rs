//! `beepgw` — bridges HTTP / JSON-lines clients written in any language to RFC 3080 BEEP,
//! and exposes HTTP backends on a BEEP network.

mod config;
mod echo;
mod http_egress;
mod ingress;
mod jsonl;
mod metrics;
mod util;

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use config::{BeepListener, ClientTls, Config, EchoListener, HttpListener, JsonlListener, ServerTls, Upstream};
use minta_beep::rpc::{ClientOptions, RpcClient, RpcProfile, RpcRequest, RPC_PROFILE};
use minta_beep::{Listener, Profiles};

#[derive(Parser)]
#[command(name = "beepgw", version, about = "MINTA BEEP bridge: HTTP / JSON-lines ⇄ RFC 3080 BEEP")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args, Clone, Default)]
struct ClientTlsArgs {
    /// Negotiate the BEEP TLS profile, verifying this server name.
    #[arg(long)]
    tls_server_name: Option<String>,
    /// PEM file with trusted CA certificates (default: Mozilla roots).
    #[arg(long)]
    tls_ca: Option<PathBuf>,
}

impl ClientTlsArgs {
    fn get(&self) -> Option<ClientTls> {
        self.tls_server_name.clone().map(|server_name| ClientTls { server_name, ca_file: self.tls_ca.clone() })
    }
}

#[derive(clap::Args, Clone, Default)]
struct ServerTlsArgs {
    /// Require the BEEP TLS profile using this certificate (PEM).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

impl ServerTlsArgs {
    fn get(&self) -> Option<ServerTls> {
        match (&self.tls_cert, &self.tls_key) {
            (Some(c), Some(k)) => Some(ServerTls { cert_file: c.clone(), key_file: k.clone() }),
            _ => None,
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Run everything described in a TOML config file.
    Run {
        #[arg(short, long, default_value = "beepgw.toml")]
        config: PathBuf,
    },
    /// Egress: accept HTTP (and optionally JSON-lines) and forward over BEEP.
    Proxy {
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,
        /// BEEP peer serving urn:minta:beep:rpc:1.
        #[arg(long, default_value = "127.0.0.1:10288")]
        upstream: String,
        /// Also accept JSON-lines on this address.
        #[arg(long)]
        jsonl: Option<String>,
        #[arg(long, default_value_t = 1)]
        sessions: usize,
        #[arg(long, default_value_t = 64)]
        max_channels: usize,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[command(flatten)]
        tls: ClientTlsArgs,
    },
    /// Ingress: accept BEEP and forward to an HTTP backend.
    Expose {
        #[arg(long, default_value = "0.0.0.0:10288")]
        listen: String,
        /// e.g. http://127.0.0.1:3000
        #[arg(long)]
        backend: String,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
    /// Built-in BEEP RPC test server (/echo, /inspect, /stream/N, /status/N, /err/N, /sleep/MS, /bytes/N).
    Echo {
        #[arg(long, default_value = "127.0.0.1:10288")]
        listen: String,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
    /// Make one RPC call over BEEP (like curl). Body goes to stdout, status/headers to stderr.
    Call {
        /// HTTP-style method.
        method: String,
        /// Request target, e.g. /users/42?x=1
        target: String,
        #[arg(long, default_value = "127.0.0.1:10288")]
        upstream: String,
        /// Header "Name: value" (repeatable).
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,
        /// Request body; `@file` reads a file, `@-` reads stdin.
        #[arg(short, long)]
        data: Option<String>,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        /// Print the status line and headers to stderr.
        #[arg(short, long)]
        include: bool,
        #[command(flatten)]
        tls: ClientTlsArgs,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let result = match cli.cmd {
        Cmd::Run { config } => match Config::load(&config) {
            Ok(cfg) => run(cfg).await,
            Err(e) => Err(e),
        },
        Cmd::Proxy { listen, upstream, jsonl, sessions, max_channels, timeout_ms, tls } => {
            let up = Upstream {
                name: "default".into(),
                address: upstream,
                sessions,
                max_channels,
                connect_timeout_ms: 5_000,
                request_timeout_ms: timeout_ms,
                tls: tls.get(),
            };
            let mut cfg = Config {
                upstream: vec![up],
                http: vec![HttpListener { listen, upstream: "default".into(), routes: vec![], max_body_bytes: 64 << 20, forwarded_headers: true }],
                ..Default::default()
            };
            if let Some(j) = jsonl {
                cfg.jsonl.push(JsonlListener { listen: j, upstream: "default".into(), max_body_bytes: 64 << 20 });
            }
            run(cfg).await
        }
        Cmd::Expose { listen, backend, timeout_ms, tls } => {
            let cfg = Config {
                beep: vec![BeepListener { listen, backend, rpy_max_bytes: 8 << 20, backend_timeout_ms: timeout_ms, tls: tls.get() }],
                ..Default::default()
            };
            run(cfg).await
        }
        Cmd::Echo { listen, tls } => run(Config { echo: vec![EchoListener { listen, tls: tls.get() }], ..Default::default() }).await,
        Cmd::Call { method, target, upstream, headers, data, timeout_ms, include, tls } => {
            call(method, target, upstream, headers, data, timeout_ms, include, tls.get()).await
        }
    };
    if let Err(e) = result {
        eprintln!("beepgw: {e}");
        std::process::exit(1);
    }
}

fn client_for(u: &Upstream) -> RpcClient {
    let opts = ClientOptions {
        sessions: u.sessions.max(1),
        max_channels: u.max_channels.max(1),
        connect_timeout: Duration::from_millis(u.connect_timeout_ms),
        request_timeout: Some(Duration::from_millis(u.request_timeout_ms)),
        tls: u.tls.as_ref().map(|t| minta_beep::tls::TlsClientOptions { server_name: t.server_name.clone(), ca_file: t.ca_file.clone() }),
        ..Default::default()
    };
    RpcClient::new(u.address.clone(), opts)
}

async fn beep_listener(listen: &str, profiles: Profiles, tls: Option<&ServerTls>) -> Result<Listener, String> {
    let l = Listener::bind(listen, minta_beep::Config::default(), profiles).await.map_err(|e| format!("bind {listen}: {e}"))?;
    match tls {
        Some(t) => l
            .with_tls(&minta_beep::tls::TlsServerOptions { cert_file: t.cert_file.clone(), key_file: t.key_file.clone() })
            .map_err(|e| format!("tls for {listen}: {e}")),
        None => Ok(l),
    }
}

async fn run(cfg: Config) -> Result<(), String> {
    cfg.validate()?;
    let metrics = Arc::new(metrics::Metrics::default());
    let upstreams: Arc<HashMap<String, RpcClient>> = Arc::new(cfg.upstream.iter().map(|u| (u.name.clone(), client_for(u))).collect());
    let mut tasks = tokio::task::JoinSet::new();

    for h in cfg.http {
        let counters = metrics.get("http", &h.listen);
        let st = Arc::new(http_egress::HttpState { cfg: h, upstreams: upstreams.clone(), metrics: metrics.clone(), counters });
        tasks.spawn(async move { http_egress::serve(st).await.map_err(|e| format!("http listener: {e}")) });
    }
    for j in cfg.jsonl {
        let counters = metrics.get("jsonl", &j.listen);
        let st = Arc::new(jsonl::JsonlState { cfg: j, upstreams: upstreams.clone(), counters });
        tasks.spawn(async move { jsonl::serve(st).await.map_err(|e| format!("jsonl listener: {e}")) });
    }
    for b in cfg.beep {
        let counters = metrics.get("beep", &b.listen);
        let handler = ingress::Ingress::new(&b.backend, b.rpy_max_bytes, Duration::from_millis(b.backend_timeout_ms), counters)?;
        let listener = beep_listener(&b.listen, Profiles::new().with(RPC_PROFILE, RpcProfile::new(handler)), b.tls.as_ref()).await?;
        tracing::info!("beep ingress listening on {} → {}{}", listener.local_addr().map_err(|e| e.to_string())?, b.backend, if b.tls.is_some() { " (TLS)" } else { "" });
        tasks.spawn(async move { listener.serve().await.map_err(|e| format!("beep listener: {e}")) });
    }
    for e in cfg.echo {
        let listener = beep_listener(&e.listen, Profiles::new().with(RPC_PROFILE, RpcProfile::new(echo::handler())), e.tls.as_ref()).await?;
        tracing::info!("beep echo server listening on {}{}", listener.local_addr().map_err(|e| e.to_string())?, if e.tls.is_some() { " (TLS)" } else { "" });
        tasks.spawn(async move { listener.serve().await.map_err(|e| format!("echo listener: {e}")) });
    }

    tokio::select! {
        r = tasks.join_next() => match r {
            Some(Ok(Err(e))) => Err(e),
            Some(Err(e)) => Err(format!("listener task failed: {e}")),
            _ => Ok(()),
        },
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down");
            for c in upstreams.values() {
                c.close().await;
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn call(method: String, target: String, upstream: String, headers: Vec<String>, data: Option<String>, timeout_ms: u64, include: bool, tls: Option<ClientTls>) -> Result<(), String> {
    let up = Upstream { name: "cli".into(), address: upstream, sessions: 1, max_channels: 1, connect_timeout_ms: 5_000, request_timeout_ms: timeout_ms, tls };
    let client = client_for(&up);
    let mut req = RpcRequest::new(method, target);
    for h in headers {
        let (k, v) = h.split_once(':').ok_or_else(|| format!("bad header {h:?}, expected 'Name: value'"))?;
        req.headers.push((k.trim().to_string(), v.trim().to_string()));
    }
    if let Some(d) = data {
        req.body = match d.strip_prefix('@') {
            Some("-") => {
                let mut buf = Vec::new();
                std::io::Read::read_to_end(&mut std::io::stdin(), &mut buf).map_err(|e| e.to_string())?;
                buf.into()
            }
            Some(path) => std::fs::read(path).map_err(|e| format!("{path}: {e}"))?.into(),
            None => d.into_bytes().into(),
        };
    }
    let (head, mut body) = client.call_streaming(req).await.map_err(|e| e.to_string())?;
    if include {
        eprintln!("status: {}", head.status);
        for (k, v) in &head.headers {
            eprintln!("{k}: {v}");
        }
        eprintln!();
    }
    let mut out = std::io::stdout().lock();
    while let Some(c) = body.chunk().await {
        let c = c.map_err(|e| e.to_string())?;
        out.write_all(&c).map_err(|e| e.to_string())?;
        out.flush().map_err(|e| e.to_string())?;
    }
    drop(out);
    client.close().await;
    if head.status >= 400 {
        std::process::exit(22);
    }
    Ok(())
}
