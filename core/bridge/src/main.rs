use clap::{Parser, Subcommand};
use minta_gateway::{
    body::BoxError,
    config::{self, Config, GrpcListener, HttpListener, Upstream},
    frontend, transport,
};
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    name = "mintagw",
    version,
    about = "Standard HTTP/JSON at the edge, streaming gRPC between gateways"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Accept ordinary HTTP/1.1, h2c or /v1/call JSON from any language.
    Proxy {
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: String,
        #[arg(long, default_value = "http://127.0.0.1:10290")]
        upstream: String,
        #[arg(long)]
        ca_file: Option<PathBuf>,
        #[arg(long)]
        server_name: Option<String>,
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long, default_value_t = config::body_limit())]
        max_body_bytes: usize,
        #[arg(long, default_value_t = config::body_limit())]
        max_response_bytes: usize,
        /// Time allowed to send a complete HTTP/1.1 request head.
        #[arg(long, default_value_t = config::header_timeout_ms())]
        header_timeout_ms: u64,
        /// Without progress before the response head arrives.
        #[arg(long, default_value_t = config::timeout_ms())]
        timeout_ms: u64,
        /// Without data in either direction once the response streams.
        #[arg(long, default_value_t = config::idle_timeout_ms())]
        idle_timeout_ms: u64,
        /// Hard cap on a whole call; 0 means none.
        #[arg(long, default_value_t = 0)]
        max_duration_ms: u64,
        #[arg(long, default_value_t = config::concurrency())]
        max_in_flight: usize,
        #[arg(long, default_value_t = config::shutdown_grace_ms())]
        shutdown_grace_ms: u64,
    },
    /// Accept gRPC from another gateway and forward to an unchanged HTTP backend.
    Expose {
        #[arg(long, default_value = "127.0.0.1:10290")]
        listen: String,
        #[arg(long)]
        backend: String,
        #[arg(long)]
        backend_ca_file: Option<PathBuf>,
        #[arg(long, requires = "key_file")]
        cert_file: Option<PathBuf>,
        #[arg(long, requires = "cert_file")]
        key_file: Option<PathBuf>,
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long, default_value_t = config::body_limit())]
        max_body_bytes: usize,
        /// Without progress before the response head arrives.
        #[arg(long, default_value_t = config::timeout_ms())]
        timeout_ms: u64,
        /// Without data in either direction once the response streams.
        #[arg(long, default_value_t = config::idle_timeout_ms())]
        idle_timeout_ms: u64,
        /// Hard cap on a whole call; 0 means none.
        #[arg(long, default_value_t = 0)]
        max_duration_ms: u64,
        #[arg(long, default_value_t = config::concurrency())]
        max_in_flight: usize,
        #[arg(long, default_value_t = config::shutdown_grace_ms())]
        shutdown_grace_ms: u64,
    },
    /// Run HTTP listeners, gRPC listeners, named upstreams and routes from TOML.
    Run {
        #[arg(short, long, default_value = "mintagw.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    if let Err(e) = run(Cli::parse()).await {
        eprintln!("mintagw: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    let cfg = match cli.command {
        Command::Proxy {
            listen,
            upstream,
            ca_file,
            server_name,
            token_env,
            max_body_bytes,
            max_response_bytes,
            header_timeout_ms,
            timeout_ms,
            idle_timeout_ms,
            max_duration_ms,
            max_in_flight,
            shutdown_grace_ms,
        } => Config {
            upstream: vec![Upstream {
                name: "default".into(),
                address: upstream,
                ca_file,
                server_name,
                token_env,
            }],
            http: vec![HttpListener {
                listen,
                upstream: "default".into(),
                routes: vec![],
                max_body_bytes,
                max_response_bytes,
                timeout_ms,
                idle_timeout_ms,
                max_duration_ms,
                header_timeout_ms,
                max_in_flight,
            }],
            shutdown_grace_ms,
            ..Default::default()
        },
        Command::Expose {
            listen,
            backend,
            backend_ca_file,
            cert_file,
            key_file,
            token_env,
            max_body_bytes,
            timeout_ms,
            idle_timeout_ms,
            max_duration_ms,
            max_in_flight,
            shutdown_grace_ms,
        } => Config {
            grpc: vec![GrpcListener {
                listen,
                backend,
                backend_ca_file,
                cert_file,
                key_file,
                token_env,
                max_body_bytes,
                timeout_ms,
                idle_timeout_ms,
                max_duration_ms,
                max_in_flight,
            }],
            shutdown_grace_ms,
            ..Default::default()
        },
        Command::Run { config } => toml::from_str(&std::fs::read_to_string(config)?)?,
    };
    cfg.validate()?;
    let clients = cfg
        .upstream
        .iter()
        .map(|u| transport::Client::new(u).map(|c| (u.name.clone(), c)))
        .collect::<Result<HashMap<_, _>, _>>()?;
    let grace = Duration::from_millis(cfg.shutdown_grace_ms);
    let shutdown = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    for h in cfg.http {
        let state = frontend::State::new(h, clients.clone(), shutdown.clone());
        tasks.spawn(frontend::serve(state));
    }
    for g in cfg.grpc {
        tasks.spawn(transport::serve(g, shutdown.clone()));
    }
    tokio::select! {
        result = tasks.join_next() => if let Some(result) = result { result??; },
        _ = stop_signal() => {},
    }
    // Every listener stops accepting; in-flight calls get the grace period.
    tracing::info!("shutting down; in-flight calls have {grace:?} to finish");
    shutdown.cancel();
    let drained =
        tokio::time::timeout(grace, async { while tasks.join_next().await.is_some() {} }).await;
    if drained.is_err() {
        tracing::warn!("grace period over; aborting the remaining calls");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}

/// SIGINT or SIGTERM; Ctrl-C or Ctrl-Break on Windows.
async fn stop_signal() {
    #[cfg(unix)]
    let other = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    #[cfg(windows)]
    let other = tokio::signal::windows::ctrl_break();
    match other {
        Ok(mut other) => tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = other.recv() => {},
        },
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_and_toml_share_defaults() {
        let Command::Proxy {
            timeout_ms,
            max_body_bytes,
            max_in_flight,
            ..
        } = Cli::try_parse_from(["mintagw", "proxy"]).unwrap().command
        else {
            unreachable!()
        };
        assert_eq!(
            (timeout_ms, max_body_bytes, max_in_flight),
            (60_000, 64 << 20, 64)
        );
        let Command::Expose { timeout_ms, .. } =
            Cli::try_parse_from(["mintagw", "expose", "--backend", "http://b"])
                .unwrap()
                .command
        else {
            unreachable!()
        };
        assert_eq!(timeout_ms, 60_000);
        let cfg: Config = toml::from_str(
            "[[http]]\nlisten = \"a\"\nupstream = \"u\"\n[[grpc]]\nlisten = \"b\"\nbackend = \"http://b\"\n",
        )
        .unwrap();
        assert_eq!(
            (cfg.http[0].timeout_ms, cfg.grpc[0].timeout_ms),
            (60_000, 60_000)
        );
        let (h, g) = (&cfg.http[0], &cfg.grpc[0]);
        assert_eq!(
            (h.idle_timeout_ms, h.header_timeout_ms, h.max_duration_ms),
            (300_000, 30_000, 0)
        );
        assert_eq!(
            (g.idle_timeout_ms, g.max_duration_ms, cfg.shutdown_grace_ms),
            (300_000, 0, 20_000)
        );
        let Command::Proxy {
            idle_timeout_ms,
            header_timeout_ms,
            shutdown_grace_ms,
            ..
        } = Cli::try_parse_from(["mintagw", "proxy"]).unwrap().command
        else {
            unreachable!()
        };
        assert_eq!(
            (idle_timeout_ms, header_timeout_ms, shutdown_grace_ms),
            (300_000, 30_000, 20_000)
        );
    }
}
