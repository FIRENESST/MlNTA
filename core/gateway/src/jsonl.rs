//! Egress: newline-delimited JSON over TCP (docs/PROTOCOL.md §3.2), for languages
//! without a usable HTTP client.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use minta_beep::rpc::{RpcClient, RpcRequest};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::config::JsonlListener;
use crate::metrics::{Counters, Metrics};

pub struct JsonlState {
    pub cfg: JsonlListener,
    pub upstreams: Arc<HashMap<String, RpcClient>>,
    pub counters: &'static Counters,
}

pub async fn serve(state: Arc<JsonlState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&state.cfg.listen).await?;
    tracing::info!("jsonl egress listening on {} → upstream {:?}", listener.local_addr()?, state.cfg.upstream);
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("accept: {e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = connection(tcp, st).await {
                tracing::debug!(%peer, "jsonl connection ended: {e}");
            }
        });
    }
}

async fn connection(tcp: TcpStream, st: Arc<JsonlState>) -> std::io::Result<()> {
    let (rd, mut wr) = tcp.into_split();
    let (tx, mut rx) = mpsc::channel::<String>(256);
    let writer = tokio::spawn(async move {
        while let Some(mut line) = rx.recv().await {
            line.push('\n');
            if wr.write_all(line.as_bytes()).await.is_err() {
                return;
            }
        }
        let _ = wr.shutdown().await;
    });
    // base64 inflates by 4/3; leave headroom for headers.
    let max_line = st.cfg.max_body_bytes / 3 * 4 + 1024 * 1024;
    let mut reader = BufReader::new(rd);
    let mut line = Vec::new();
    loop {
        line.clear();
        let n = (&mut reader).take(max_line as u64 + 1).read_until(b'\n', &mut line).await?;
        if n == 0 {
            break;
        }
        if line.len() > max_line {
            let _ = tx.send(json!({"id": null, "error": {"code": 554, "message": "request line too long"}}).to_string()).await;
            break;
        }
        let text = String::from_utf8_lossy(&line).trim().to_string();
        if text.is_empty() {
            continue;
        }
        let st = st.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let _inflight = Metrics::start(st.counters);
            let resp = handle(&text, &st).await;
            if resp.get("error").is_some() {
                st.counters.errors.fetch_add(1, Ordering::Relaxed);
            }
            let _ = tx.send(resp.to_string()).await;
        });
    }
    drop(tx);
    let _ = writer.await;
    Ok(())
}

fn err(id: &Value, code: u32, message: impl Into<String>) -> Value {
    json!({"id": id, "error": {"code": code, "message": message.into()}})
}

async fn handle(line: &str, st: &JsonlState) -> Value {
    let v: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => return err(&Value::Null, 500, format!("invalid JSON: {e}")),
    };
    let id = v.get("id").cloned().unwrap_or(Value::Null);
    let method = v.get("method").and_then(Value::as_str).unwrap_or("GET").to_string();
    let Some(target) = v.get("target").and_then(Value::as_str).map(str::to_string) else {
        return err(&id, 501, "missing \"target\"");
    };
    let mut headers: Vec<(String, String)> = vec![];
    match v.get("headers") {
        Some(Value::Object(m)) => {
            for (k, val) in m {
                headers.push((k.clone(), val.as_str().map(str::to_string).unwrap_or_else(|| val.to_string())));
            }
        }
        Some(Value::Array(a)) => {
            for pair in a {
                if let (Some(k), Some(val)) = (pair.get(0).and_then(Value::as_str), pair.get(1).and_then(Value::as_str)) {
                    headers.push((k.to_string(), val.to_string()));
                }
            }
        }
        _ => {}
    }
    let body = if let Some(b64) = v.get("body_b64").and_then(Value::as_str) {
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(b) => Bytes::from(b),
            Err(e) => return err(&id, 501, format!("bad body_b64: {e}")),
        }
    } else {
        match v.get("body") {
            Some(Value::String(s)) => Bytes::from(s.clone()),
            Some(Value::Null) | None => Bytes::new(),
            Some(other) => Bytes::from(other.to_string()),
        }
    };
    if body.len() > st.cfg.max_body_bytes {
        return err(&id, 554, "request body too large");
    }
    let name = v.get("upstream").and_then(Value::as_str).unwrap_or(&st.cfg.upstream).to_string();
    let Some(client) = st.upstreams.get(&name) else {
        return err(&id, 553, format!("unknown upstream {name:?}"));
    };
    let timeout = v.get("timeout_ms").and_then(Value::as_u64).map(Duration::from_millis);
    if let Some(t) = timeout {
        headers.push(("Rpc-Timeout".into(), t.as_millis().to_string()));
    }
    st.counters.bytes_in.fetch_add(body.len() as u64, Ordering::Relaxed);
    let req = RpcRequest { method, target, headers, body };
    let result = match timeout {
        Some(t) => match client.call_streaming_timeout(req, Some(t)).await {
            Ok((head, body)) => body.collect(64 << 20).await.map(|b| (head, b)),
            Err(e) => Err(e),
        },
        None => match client.call_streaming(req).await {
            Ok((head, body)) => body.collect(64 << 20).await.map(|b| (head, b)),
            Err(e) => Err(e),
        },
    };
    match result {
        Ok((head, body)) => {
            st.counters.bytes_out.fetch_add(body.len() as u64, Ordering::Relaxed);
            let headers: Vec<Value> = head.headers.iter().map(|(k, v)| json!([k, v])).collect();
            let mut out = json!({
                "id": id,
                "status": head.status,
                "headers": headers,
                "body_b64": base64::engine::general_purpose::STANDARD.encode(&body),
            });
            if let Ok(text) = std::str::from_utf8(&body) {
                out["body"] = Value::String(text.to_string());
            }
            out
        }
        Err(e) => {
            let message = match &e {
                minta_beep::rpc::RpcError::Remote { message, .. } => message.clone(),
                other => other.to_string(),
            };
            err(&id, e.beep_code(), message)
        }
    }
}
