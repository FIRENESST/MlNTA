//! Built-in RPC test server (`beepgw echo`). Every language binding is tested against it.
//!
//! | target            | behaviour                                                        |
//! |-------------------|------------------------------------------------------------------|
//! | `/echo`           | 200, body and Content-Type echoed, `X-Echo-Method`/`X-Echo-Target`, `X-Test` echoed |
//! | `/inspect`        | 200, JSON describing the request                                 |
//! | `/status/{code}`  | that status, body `status {code}`                                |
//! | `/stream/{n}`     | ANS stream of `n` lines `line {i}\n`                             |
//! | `/err/{code}`     | BEEP ERR with that code                                          |
//! | `/sleep/{ms}`     | sleep, then 200 `slept`                                          |
//! | `/bytes/{n}`      | `n` bytes of `abc…z` repeated                                    |
//! | `/duplex`         | rpc:2: each request chunk echoed as it arrives, then trailers `X-Echo-Trailer: done` (+ request trailers as `X-Req-*`) |
//! | `/count`          | 200 JSON `{"bytes": n, "crc32": "…"}` over a body read without buffering |

use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use minta_beep::rpc::{BodyItem, RequestPart, RpcFailure, RpcHandler, RpcServerRequest, RpcServerResponse};
use serde_json::json;

pub struct Echo;

pub fn handler() -> Echo {
    Echo
}

fn failure(e: minta_beep::rpc::RpcError) -> RpcFailure {
    RpcFailure::new(e.beep_code(), e.to_string())
}

#[async_trait]
impl RpcHandler for Echo {
    fn streaming(&self) -> bool {
        true
    }

    async fn handle(&self, mut req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let path = req.target.split('?').next().unwrap_or("/").to_string();
        let stream = req.stream.take();
        match (path.as_str(), stream) {
            ("/duplex", Some(body)) => Ok(duplex(req, body)),
            ("/count", Some(mut body)) => {
                let (mut n, mut crc) = (0u64, 0u32);
                while let Some(p) = body.next().await.map_err(failure)? {
                    if let RequestPart::Data(b) = p {
                        n += b.len() as u64;
                        crc = crc32(crc, &b);
                    }
                }
                let v = json!({"bytes": n, "crc32": format!("{crc:08x}")});
                Ok(RpcServerResponse::full(200, vec![("Content-Type".into(), "application/json".into())], v.to_string()))
            }
            (_, Some(body)) => {
                let (b, _) = body.collect(64 << 20).await.map_err(failure)?;
                req.body = b;
                respond(req).await
            }
            (_, None) => respond(req).await,
        }
    }
}

/// CRC-32 (IEEE, as zlib.crc32), continuing from `crc`.
fn crc32(crc: u32, data: &[u8]) -> u32 {
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xedb8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *e = c;
        }
        t
    });
    let mut c = !crc;
    for &b in data {
        c = table[((c ^ b as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

fn duplex(req: RpcServerRequest, mut body: minta_beep::rpc::RequestBody) -> RpcServerResponse {
    let ctype = req.header("Content-Type").unwrap_or("application/octet-stream").to_string();
    let (resp, tx) = RpcServerResponse::frames(200, vec![("Content-Type".into(), ctype)]);
    tokio::spawn(async move {
        let mut trailers = vec![("X-Echo-Trailer".to_string(), "done".to_string())];
        loop {
            let item = match body.next().await {
                Ok(Some(RequestPart::Data(b))) => BodyItem::Data(b),
                Ok(Some(RequestPart::Trailers(t))) => {
                    trailers.extend(t.into_iter().map(|(k, v)| (format!("X-Req-{k}"), v)));
                    continue;
                }
                Ok(None) => BodyItem::Trailers(std::mem::take(&mut trailers)),
                Err(e) => BodyItem::Abort(e.to_string()),
            };
            let last = !matches!(item, BodyItem::Data(_));
            if tx.send(item).await.is_err() || last {
                return;
            }
        }
    });
    resp
}

async fn respond(req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
    let path = req.target.split('?').next().unwrap_or("/").to_string();
    let seg: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
    let arg = seg.get(1).copied().unwrap_or("");
    let text = |status: u16, body: String| Ok(RpcServerResponse::full(status, vec![("Content-Type".into(), "text/plain; charset=utf-8".into())], body));
    match seg[0] {
        "echo" => {
            let mut h = vec![
                ("Content-Type".to_string(), req.header("Content-Type").unwrap_or("application/octet-stream").to_string()),
                ("X-Echo-Method".to_string(), req.method.clone()),
                ("X-Echo-Target".to_string(), req.target.clone()),
            ];
            if let Some(v) = req.header("X-Test") {
                h.push(("X-Test".into(), v.to_string()));
            }
            Ok(RpcServerResponse::full(200, h, req.body))
        }
        "inspect" => {
            let headers: serde_json::Map<String, serde_json::Value> =
                req.headers.iter().map(|(k, v)| (k.to_ascii_lowercase(), json!(v))).collect();
            let body = match std::str::from_utf8(&req.body) {
                Ok(s) => json!(s),
                Err(_) => json!(base64::engine::general_purpose::STANDARD.encode(&req.body)),
            };
            let v = json!({"method": req.method, "target": req.target, "headers": headers, "body_len": req.body.len(), "body": body,
                           "channel": req.channel, "profile": req.profile, "identity": req.identity});
            Ok(RpcServerResponse::full(200, vec![("Content-Type".into(), "application/json".into())], v.to_string()))
        }
        "status" => {
            let code: u16 = arg.parse().ok().filter(|c| (100..=999).contains(c)).ok_or_else(|| RpcFailure::new(553, "bad status"))?;
            text(code, format!("status {code}"))
        }
        "stream" => {
            let n: usize = arg.parse().map_err(|_| RpcFailure::new(553, "bad count"))?;
            let (resp, tx) = RpcServerResponse::stream(200, vec![("Content-Type".into(), "text/plain; charset=utf-8".into())]);
            tokio::spawn(async move {
                for i in 0..n {
                    if tx.send(Ok(Bytes::from(format!("line {i}\n")))).await.is_err() {
                        return;
                    }
                }
            });
            Ok(resp)
        }
        "err" => Err(RpcFailure::new(arg.parse().unwrap_or(554), format!("requested error {arg}"))),
        "sleep" => {
            let ms: u64 = arg.parse().map_err(|_| RpcFailure::new(553, "bad duration"))?;
            tokio::time::sleep(Duration::from_millis(ms)).await;
            text(200, "slept".into())
        }
        "bytes" => {
            let n: usize = arg.parse().map_err(|_| RpcFailure::new(553, "bad size"))?;
            if n > 256 << 20 {
                return Err(RpcFailure::new(553, "too large"));
            }
            let body: Vec<u8> = (0..n).map(|i| b'a' + (i % 26) as u8).collect();
            Ok(RpcServerResponse::full(200, vec![("Content-Type".into(), "application/octet-stream".into())], body))
        }
        _ => text(404, format!("no route for {}", req.target)),
    }
}
