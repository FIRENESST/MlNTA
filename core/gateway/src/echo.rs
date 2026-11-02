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

use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use minta_beep::rpc::{handler_fn, RpcFailure, RpcHandler, RpcServerRequest, RpcServerResponse};
use serde_json::json;

pub fn handler() -> impl RpcHandler {
    handler_fn(|req: RpcServerRequest| async move { respond(req).await })
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
            let v = json!({"method": req.method, "target": req.target, "headers": headers, "body_len": req.body.len(), "body": body, "channel": req.channel});
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
