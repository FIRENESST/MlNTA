//! SASL authentication over BEEP (RFC 3080 §3.2, RFC 4422). Mechanism: PLAIN (RFC 4616).
//!
//! The client starts a channel of `http://iana.org/beep/SASL/PLAIN` with its credentials
//! piggybacked in a `<blob>` (base64 of `authzid NUL authcid NUL password`); the listener
//! answers `<blob status='complete'/>` and binds the identity to the session, or refuses
//! with `ERR 535`. PLAIN sends the password in clear text, so use it over TLS.

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::{code, Error, Result};
use crate::mgmt::{esc, parse_xml, ProfileEl};
use crate::mime::Entity;
use crate::session::{IncomingMessage, ProfileHandler, Reply, Responder, Session};

pub const SASL_PREFIX: &str = "http://iana.org/beep/SASL/";
pub const SASL_PLAIN: &str = "http://iana.org/beep/SASL/PLAIN";

/// Checks credentials. Returns the identity to bind to the session, or `None` to reject.
/// `authzid` is the identity the client asks to act as; empty means "same as `authcid`".
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, authzid: &str, authcid: &str, password: &str) -> Option<String>;
}

impl<F> Authenticator for F
where
    F: Fn(&str, &str, &str) -> Option<String> + Send + Sync + 'static,
{
    fn authenticate(&self, authzid: &str, authcid: &str, password: &str) -> Option<String> {
        self(authzid, authcid, password)
    }
}

#[derive(Debug, Clone, Default)]
pub struct PlainCredentials {
    pub authzid: String,
    pub authcid: String,
    pub password: String,
}

impl PlainCredentials {
    pub fn new(user: impl Into<String>, password: impl Into<String>) -> Self {
        PlainCredentials { authzid: String::new(), authcid: user.into(), password: password.into() }
    }

    fn message(&self) -> Vec<u8> {
        let mut m = Vec::with_capacity(self.authzid.len() + self.authcid.len() + self.password.len() + 2);
        m.extend_from_slice(self.authzid.as_bytes());
        m.push(0);
        m.extend_from_slice(self.authcid.as_bytes());
        m.push(0);
        m.extend_from_slice(self.password.as_bytes());
        m
    }
}

fn blob_xml(status: Option<&str>, data: &[u8]) -> String {
    let status = status.map(|s| format!(" status='{}'", esc(s))).unwrap_or_default();
    if data.is_empty() {
        format!("<blob{status} />")
    } else {
        format!("<blob{status}>{}</blob>", base64_encode(data))
    }
}

/// `<blob status='…'>base64</blob>` → (status, decoded data).
fn parse_blob(xml: &str) -> std::result::Result<(Option<String>, Vec<u8>), String> {
    let e = parse_xml(xml.trim())?;
    if e.name != "blob" {
        return Err(format!("expected <blob>, got <{}>", e.name));
    }
    let data = base64_decode(e.text().trim()).ok_or("blob is not valid base64")?;
    Ok((e.attr("status").map(str::to_string), data))
}

/// Listener side of SASL PLAIN.
pub struct PlainServer {
    auth: Arc<dyn Authenticator>,
}

impl PlainServer {
    pub fn new(auth: impl Authenticator) -> Self {
        PlainServer { auth: Arc::new(auth) }
    }

    fn step(&self, session: &Session, xml: &str) -> std::result::Result<String, (u32, String)> {
        let (status, data) = parse_blob(xml).map_err(|e| (code::PARAM_SYNTAX_ERROR, e))?;
        if status.as_deref() == Some("abort") {
            return Err((code::AUTH_FAILURE, "authentication aborted".into()));
        }
        let parts: Vec<&[u8]> = data.splitn(3, |b| *b == 0).collect();
        let [authzid, authcid, password] = parts.as_slice() else {
            return Err((code::PARAM_SYNTAX_ERROR, "PLAIN needs authzid NUL authcid NUL password".into()));
        };
        let text = |b: &[u8]| std::str::from_utf8(b).map(str::to_string).map_err(|_| (code::PARAM_SYNTAX_ERROR, "credentials are not UTF-8".to_string()));
        let (authzid, authcid, password) = (text(authzid)?, text(authcid)?, text(password)?);
        match self.auth.authenticate(&authzid, &authcid, &password) {
            Some(id) => {
                session.set_identity(id);
                Ok(blob_xml(Some("complete"), b""))
            }
            None => Err((code::AUTH_FAILURE, "authentication failure".into())),
        }
    }
}

#[async_trait]
impl ProfileHandler for PlainServer {
    async fn on_start(&self, session: &Session, _channel: u32, init: Option<&str>) -> std::result::Result<Option<String>, (u32, String)> {
        if session.identity().is_some() {
            return Err((code::ACTION_NOT_TAKEN, "already authenticated".into()));
        }
        match init {
            Some(xml) if !xml.trim().is_empty() => self.step(session, xml).map(Some),
            _ => Ok(None),
        }
    }

    async fn on_message(&self, session: &Session, msg: IncomingMessage, reply: Responder) {
        let body = match msg.collect(64 << 10).await {
            Ok(b) => b,
            Err(_) => {
                let _ = reply.err_code(code::SYNTAX_ERROR, "blob too large").await;
                return;
            }
        };
        let xml = String::from_utf8_lossy(&Entity::parse(body).body).into_owned();
        let _ = match self.step(session, &xml) {
            Ok(x) => reply.rpy(Entity::xml(x).to_bytes()).await,
            Err((c, m)) => reply.err_code(c, &m).await,
        };
    }
}

/// Initiator side: authenticate `session` with PLAIN. Fails with `Error::Refused { code: 535 }`
/// for bad credentials.
pub async fn authenticate_plain(session: &Session, creds: &PlainCredentials) -> Result<()> {
    let blob = blob_xml(None, &creds.message());
    let mut el = ProfileEl::new(SASL_PLAIN);
    el.data = Some(blob.clone());
    let (ch, reply) = session.start_channel_with(vec![el], None).await?;
    let complete = |xml: &str| parse_blob(xml).map(|(s, _)| s.as_deref() == Some("complete")).unwrap_or(false);
    if !reply.as_deref().is_some_and(complete) {
        // The listener did not process the piggybacked blob: send it on the channel.
        match ch.request(Entity::xml(blob).to_bytes()).await? {
            Reply::Rpy(b) if complete(&String::from_utf8_lossy(&Entity::parse(b.clone()).body)) => {}
            Reply::Err(b) => {
                let (code, message) = crate::rpc::parse_err_payload(b);
                return Err(Error::Refused { code, message });
            }
            other => return Err(Error::Protocol(format!("unexpected SASL reply {other:?}"))),
        }
    }
    let _ = ch.close().await;
    Ok(())
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (c[0] as u32) << 16 | (*c.get(1).unwrap_or(&0) as u32) << 8 | *c.get(2).unwrap_or(&0) as u32;
        for i in 0..4 {
            if i <= c.len() {
                out.push(B64[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    let (mut acc, mut bits) = (0u32, 0u32);
    for ch in s.bytes().filter(|b| !b.is_ascii_whitespace()) {
        if ch == b'=' {
            break;
        }
        let v = B64.iter().position(|&b| b == ch)? as u32;
        acc = acc << 6 | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip_and_rfc4616_example() {
        for s in [&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar", &[0u8, 255, 7]] {
            assert_eq!(base64_decode(&base64_encode(s)).unwrap(), s);
        }
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // RFC 4616 §4: "\0tim\0tanstaaftanstaaf"
        let c = PlainCredentials::new("tim", "tanstaaftanstaaf");
        assert_eq!(base64_encode(&c.message()), "AHRpbQB0YW5zdGFhZnRhbnN0YWFm");
        let (status, data) = parse_blob("<blob status='continue'>AHRpbQB0YW5zdGFhZnRhbnN0YWFm</blob>").unwrap();
        assert_eq!((status.as_deref(), data), (Some("continue"), c.message()));
        assert!(parse_blob("<nope/>").is_err());
    }
}
