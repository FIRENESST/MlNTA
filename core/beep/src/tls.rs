//! TLS tuning profile (RFC 3080 §3.1, `http://iana.org/beep/TLS`).
//!
//! The negotiation happens on a minimal "bootstrap" session before the real session
//! starts: greetings, `<start>` with the TLS profile and piggybacked `<ready />`,
//! `<proceed />`, TLS handshake, then a fresh BEEP session (new greetings) over TLS.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::error::{code, Error, Result};
use crate::frame::{encode_data, DataFrame, Frame, FrameDecoder, FrameType};
use crate::mgmt::{Mgmt, ProfileEl};
use crate::mime::Entity;
use crate::session::{Config, Profiles, Role, Session};

pub const TLS_PROFILE: &str = "http://iana.org/beep/TLS";

#[derive(Debug, Clone)]
pub struct TlsClientOptions {
    /// Name to verify in the server certificate (SNI).
    pub server_name: String,
    /// PEM bundle of trusted CAs; the Mozilla root set is used when absent.
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct TlsServerOptions {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn tls_err(e: impl std::fmt::Display) -> Error {
    Error::Tls(e.to_string())
}

pub fn client_config(opts: &TlsClientOptions) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    match &opts.ca_file {
        Some(path) => {
            let pem = std::fs::read(path)?;
            for cert in rustls_pemfile::certs(&mut &pem[..]) {
                roots.add(cert?).map_err(tls_err)?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let cfg = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

pub fn server_config(opts: &TlsServerOptions) -> Result<Arc<ServerConfig>> {
    let cert_pem = std::fs::read(&opts.cert_file)?;
    let key_pem = std::fs::read(&opts.key_file)?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &cert_pem[..]).collect::<std::result::Result<_, _>>()?;
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut &key_pem[..])?.ok_or_else(|| tls_err("no private key in key file"))?;
    let cfg = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(tls_err)?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(tls_err)?;
    Ok(Arc::new(cfg))
}

/// Minimal frame I/O used before the real session exists.
struct Boot<S> {
    io: S,
    buf: BytesMut,
    dec: FrameDecoder,
    tx_seq: HashMap<u32, u32>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Boot<S> {
    fn new(io: S) -> Self {
        Boot { io, buf: BytesMut::new(), dec: FrameDecoder::new(64 * 1024), tx_seq: HashMap::new() }
    }

    async fn send(&mut self, ftype: FrameType, channel: u32, msgno: u32, xml: String) -> Result<()> {
        let payload = Entity::xml(xml).to_bytes();
        let seq = self.tx_seq.entry(channel).or_insert(0);
        let frame = DataFrame { ftype, channel, msgno, more: false, seqno: *seq, ansno: 0, payload: payload.clone() };
        *seq = seq.wrapping_add(payload.len() as u32);
        self.io.write_all(&encode_data(&frame)).await?;
        self.io.flush().await?;
        Ok(())
    }

    /// Read one complete message (any channel), skipping SEQ frames.
    async fn recv(&mut self) -> Result<(FrameType, u32, u32, Bytes)> {
        let mut acc = BytesMut::new();
        loop {
            while let Some(f) = self.dec.decode(&mut self.buf)? {
                if let Frame::Data(d) = f {
                    acc.extend_from_slice(&d.payload);
                    if acc.len() > 64 * 1024 {
                        return Err(Error::Protocol("bootstrap message too large".into()));
                    }
                    if !d.more {
                        return Ok((d.ftype, d.channel, d.msgno, Entity::parse(acc.freeze()).body));
                    }
                }
            }
            if self.io.read_buf(&mut self.buf).await? == 0 {
                return Err(Error::Closed("connection closed during TLS negotiation".into()));
            }
        }
    }

    fn into_inner(self) -> Result<S> {
        if !self.buf.is_empty() {
            return Err(Error::Protocol("unexpected data before TLS handshake".into()));
        }
        Ok(self.io)
    }
}

fn is_proceed(data: Option<&str>) -> bool {
    data.map(|d| d.contains("<proceed")).unwrap_or(false)
}

/// Connect, negotiate TLS, and establish a BEEP session over it.
pub async fn connect(addr: impl ToSocketAddrs, cfg: Config, profiles: Profiles, opts: &TlsClientOptions) -> Result<Session> {
    let tcp = TcpStream::connect(addr).await?;
    let _ = tcp.set_nodelay(true);
    let tls = start_tls_client(tcp, opts).await?;
    Session::establish(tls, Role::Initiator, cfg, profiles).await
}

/// Initiator side of the TLS tuning exchange on an already-connected stream.
pub async fn start_tls_client<S>(io: S, opts: &TlsClientOptions) -> Result<tokio_rustls::client::TlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut b = Boot::new(io);
    b.send(FrameType::Rpy, 0, 0, Mgmt::Greeting { profiles: vec![], features: None, localize: None }.to_xml()).await?;
    let (t, ch, _, body) = b.recv().await?;
    if t != FrameType::Rpy || ch != 0 {
        return Err(Error::Protocol("expected greeting".into()));
    }
    match Mgmt::parse(&body)? {
        Mgmt::Greeting { profiles, .. } if profiles.iter().any(|p| p == TLS_PROFILE) => {}
        Mgmt::Greeting { .. } => return Err(tls_err("peer does not offer the TLS profile")),
        other => return Err(Error::Protocol(format!("expected greeting, got {other:?}"))),
    }
    let start = Mgmt::Start {
        number: 1,
        server_name: Some(opts.server_name.clone()),
        profiles: vec![ProfileEl { uri: TLS_PROFILE.into(), data: Some("<ready />".into()), base64: false }],
    };
    b.send(FrameType::Msg, 0, 1, start.to_xml()).await?;
    let (t, ch, _, body) = b.recv().await?;
    if ch != 0 {
        return Err(Error::Protocol("unexpected frame during TLS negotiation".into()));
    }
    match (t, Mgmt::parse(&body)?) {
        (FrameType::Rpy, Mgmt::Profile(p)) if p.uri == TLS_PROFILE && is_proceed(p.data.as_deref()) => {}
        (FrameType::Rpy, other) => return Err(tls_err(format!("peer did not proceed: {other:?}"))),
        (_, Mgmt::Error { code, message }) => return Err(Error::Refused { code, message }),
        (_, other) => return Err(Error::Protocol(format!("unexpected reply {other:?}"))),
    }
    let io = b.into_inner()?;
    let name = ServerName::try_from(opts.server_name.clone()).map_err(tls_err)?;
    TlsConnector::from(client_config(opts)?).connect(name, io).await.map_err(tls_err)
}

/// Listener side: require TLS before anything else, then hand back the encrypted stream.
pub async fn start_tls_server<S>(io: S, acceptor: &TlsAcceptor) -> Result<tokio_rustls::server::TlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut b = Boot::new(io);
    b.send(FrameType::Rpy, 0, 0, Mgmt::Greeting { profiles: vec![TLS_PROFILE.into()], features: None, localize: None }.to_xml()).await?;
    let (t, ch, msgno, _) = b.recv().await?;
    if t != FrameType::Rpy || ch != 0 || msgno != 0 {
        return Err(Error::Protocol("expected greeting".into()));
    }
    let mut tls_channel: Option<u32> = None;
    let mut proceeded = false;
    for _ in 0..16 {
        let (t, ch, msgno, body) = b.recv().await?;
        if t != FrameType::Msg {
            return Err(Error::Protocol("expected MSG during TLS negotiation".into()));
        }
        if Some(ch) == tls_channel {
            // `<ready />` sent as its own MSG on the TLS channel.
            if String::from_utf8_lossy(&body).contains("<ready") {
                b.send(FrameType::Rpy, ch, msgno, "<proceed />".into()).await?;
                proceeded = true;
                break;
            }
            b.send(FrameType::Err, ch, msgno, Mgmt::Error { code: code::SYNTAX_ERROR, message: "expected <ready />".into() }.to_xml()).await?;
            continue;
        }
        if ch != 0 {
            return Err(Error::Protocol("frame on unexpected channel".into()));
        }
        match Mgmt::parse(&body)? {
            Mgmt::Start { number, profiles, .. } if number % 2 == 1 && profiles.iter().any(|p| p.uri == TLS_PROFILE) => {
                let p = profiles.iter().find(|p| p.uri == TLS_PROFILE).unwrap();
                if p.data.as_deref().map(|d| d.contains("<ready")).unwrap_or(false) {
                    let reply = Mgmt::Profile(ProfileEl { uri: TLS_PROFILE.into(), data: Some("<proceed />".into()), base64: false });
                    b.send(FrameType::Rpy, 0, msgno, reply.to_xml()).await?;
                    proceeded = true;
                    break;
                }
                b.send(FrameType::Rpy, 0, msgno, Mgmt::Profile(ProfileEl::new(TLS_PROFILE)).to_xml()).await?;
                tls_channel = Some(number);
            }
            Mgmt::Close { number: 0, .. } => {
                b.send(FrameType::Rpy, 0, msgno, Mgmt::Ok.to_xml()).await?;
                return Err(Error::Closed("peer released session before TLS".into()));
            }
            _ => {
                b.send(FrameType::Err, 0, msgno, Mgmt::Error { code: code::ENCRYPTION_REQUIRED, message: "TLS required".into() }.to_xml()).await?;
            }
        }
    }
    if !proceeded {
        return Err(tls_err("peer never requested TLS"));
    }
    let io = b.into_inner()?;
    acceptor.accept(io).await.map_err(tls_err)
}
