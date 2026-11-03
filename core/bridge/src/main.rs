use clap::{Parser, Subcommand};
use minta_gateway::{
    body::BoxError,
    config::{Config, GrpcListener, HttpListener, Upstream},
    frontend, transport,
};
use std::{collections::HashMap, path::PathBuf};

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
        #[arg(long, default_value_t = 64 << 20)]
        max_body_bytes: usize,
        #[arg(long, default_value_t = 64 << 20)]
        max_response_bytes: usize,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 64)]
        max_in_flight: usize,
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
        #[arg(long, default_value_t = 64 << 20)]
        max_body_bytes: usize,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 64)]
        max_in_flight: usize,
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
            timeout_ms,
            max_in_flight,
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
                max_in_flight,
            }],
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
            max_in_flight,
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
                max_in_flight,
            }],
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
    let mut tasks = tokio::task::JoinSet::new();
    for h in cfg.http {
        tasks.spawn(frontend::serve(frontend::State::new(h, clients.clone())));
    }
    for g in cfg.grpc {
        tasks.spawn(transport::serve(g));
    }
    tokio::select! {
        result = tasks.join_next() => if let Some(result) = result { result??; },
        _ = tokio::signal::ctrl_c() => {},
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}
