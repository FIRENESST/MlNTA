use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use minta_beep::rpc::*;
use minta_beep::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const ECHO: &str = "urn:test:echo";

struct Echo;

#[async_trait]
impl ProfileHandler for Echo {
    async fn on_message(&self, session: &Session, msg: IncomingMessage, reply: Responder) {
        let body = msg.collect(session.config().max_message_size).await.unwrap();
        if body.starts_with(b"\r\nANS") {
            for i in 0..3u8 {
                reply.ans(Bytes::from(vec![b'0' + i; 10])).await.unwrap();
            }
            reply.nul().await.unwrap();
        } else if body.starts_with(b"\r\nERR") {
            reply.err_code(code::ACTION_NOT_TAKEN, "nope").await.unwrap();
        } else {
            reply.rpy(body).await.unwrap();
        }
    }
}

async fn echo_listener(cfg: Config) -> std::net::SocketAddr {
    let l = Listener::bind("127.0.0.1:0", cfg, Profiles::new().with(ECHO, Echo)).await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(l.serve());
    addr
}

fn rpc_handler() -> impl RpcHandler {
    handler_fn(|req: RpcServerRequest| async move {
        let path = req.target.split('?').next().unwrap_or("").to_string();
        if let Some(n) = path.strip_prefix("/stream/") {
            let n: usize = n.parse().unwrap();
            let (resp, tx) = RpcServerResponse::stream(200, vec![("Content-Type".into(), "text/plain".into())]);
            tokio::spawn(async move {
                for i in 0..n {
                    if tx.send(Ok(Bytes::from(format!("chunk{i};")))).await.is_err() {
                        return;
                    }
                }
            });
            return Ok(resp);
        }
        if path == "/abort" {
            let (resp, tx) = RpcServerResponse::stream(200, vec![]);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Bytes::from_static(b"partial"))).await;
                let _ = tx.send(Err("backend died".into())).await;
            });
            return Ok(resp);
        }
        if let Some(c) = path.strip_prefix("/err/") {
            return Err(RpcFailure::new(c.parse().unwrap(), "requested failure"));
        }
        if let Some(ms) = path.strip_prefix("/sleep/") {
            tokio::time::sleep(Duration::from_millis(ms.parse().unwrap())).await;
        }
        let mut headers = vec![("X-Method".to_string(), req.method.clone()), ("X-Target".to_string(), req.target.clone())];
        if let Some(v) = req.header("X-Custom") {
            headers.push(("X-Custom".into(), v.to_string()));
        }
        Ok(RpcServerResponse::full(201, headers, req.body))
    })
}

async fn rpc_server(cfg: Config) -> std::net::SocketAddr {
    let s = RpcServer::bind("127.0.0.1:0", rpc_handler(), cfg).await.unwrap();
    let addr = s.local_addr().unwrap();
    tokio::spawn(s.serve());
    addr
}

fn payload(body: &[u8]) -> Bytes {
    let mut b = BytesMut::from(&b"\r\n"[..]);
    b.extend_from_slice(body);
    b.freeze()
}

#[tokio::test]
async fn echo_roundtrip_and_reply_kinds() {
    let addr = echo_listener(Config::default()).await;
    let s = Session::connect(addr, Config::default(), Profiles::new()).await.unwrap();
    assert_eq!(s.peer_profiles(), vec![ECHO.to_string()]);
    let ch = s.start_channel(ECHO).await.unwrap();
    assert_eq!(ch.number() % 2, 1);
    assert_eq!(ch.request(payload(b"hi")).await.unwrap(), Reply::Rpy(payload(b"hi")));
    match ch.request(payload(b"ANS")).await.unwrap() {
        Reply::Ans(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[2], (2, Bytes::from(vec![b'2'; 10])));
        }
        other => panic!("{other:?}"),
    }
    match ch.request(payload(b"ERR")).await.unwrap() {
        Reply::Err(b) => assert_eq!(parse_err_payload(b), (550, "nope".to_string())),
        other => panic!("{other:?}"),
    }
    match s.start_channel("urn:nobody").await {
        Err(Error::Refused { code: 550, .. }) => {}
        other => panic!("{other:?}"),
    }
    ch.close().await.unwrap();
    assert_eq!(s.channel_count(), 0);
    s.close().await.unwrap();
    assert!(s.is_closed());
}

#[tokio::test]
async fn large_messages_with_tiny_windows_and_pipelining() {
    let cfg = Config { recv_window: 5000, max_frame_size: 1777, ..Config::default() };
    let addr = echo_listener(cfg.clone()).await;
    let s = Session::connect(addr, cfg, Profiles::new()).await.unwrap();
    let ch = s.start_channel(ECHO).await.unwrap();
    let big: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    // Pipeline several MSGs on one channel; replies must come back in order.
    let mut streams = vec![];
    for i in 0..4u8 {
        let mut body = vec![i];
        body.extend_from_slice(&big[..big.len() / (i as usize + 1)]);
        streams.push((body.clone(), ch.send(payload(&body)).await.unwrap()));
    }
    for (body, st) in streams {
        assert_eq!(st.collect(64 << 20).await.unwrap(), Reply::Rpy(payload(&body)));
    }
}

#[tokio::test]
async fn rpc_full_stream_err_and_concurrency() {
    let addr = rpc_server(Config::default()).await;
    let client = RpcClient::new(addr.to_string(), ClientOptions { max_channels: 8, ..Default::default() });

    let r = client.call(RpcRequest::new("POST", "/echo?x=1").header("X-Custom", "v").body("payload")).await.unwrap();
    assert_eq!(r.status, 201);
    assert_eq!(r.header("x-method"), Some("POST"));
    assert_eq!(r.header("X-Target"), Some("/echo?x=1"));
    assert_eq!(r.header("X-Custom"), Some("v"));
    assert_eq!(&r.body[..], b"payload");

    let r = client.call(RpcRequest::new("GET", "/stream/100")).await.unwrap();
    assert_eq!(r.status, 200);
    let expected: String = (0..100).map(|i| format!("chunk{i};")).collect();
    assert_eq!(String::from_utf8(r.body.to_vec()).unwrap(), expected);

    match client.call(RpcRequest::new("GET", "/err/537")).await {
        Err(e @ RpcError::Remote { code: 537, .. }) => assert_eq!(e.http_status(), 403),
        other => panic!("{other:?}"),
    }
    match client.call(RpcRequest::new("GET", "/abort")).await {
        Err(RpcError::Aborted(reason)) => assert_eq!(reason, "backend died"),
        other => panic!("{other:?}"),
    }

    let done = Arc::new(AtomicUsize::new(0));
    let mut tasks = vec![];
    for i in 0..200 {
        let c = client.clone();
        let done = done.clone();
        tasks.push(tokio::spawn(async move {
            let body = format!("req-{i}").repeat(i % 50 + 1);
            let r = c.call(RpcRequest::new("PUT", format!("/sleep/{}", i % 7)).body(body.clone())).await.unwrap();
            assert_eq!(r.body, Bytes::from(body));
            done.fetch_add(1, Ordering::SeqCst);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(done.load(Ordering::SeqCst), 200);
    let st = client.status();
    assert_eq!(st.len(), 1);
    assert!(st[0] <= 8, "channel pool exceeded limit: {st:?}");
    client.close().await;
}

#[tokio::test]
async fn timeouts_and_abandoned_streams_recycle_channels() {
    let addr = rpc_server(Config::default()).await;
    let client = RpcClient::new(
        addr.to_string(),
        ClientOptions { max_channels: 2, request_timeout: Some(Duration::from_millis(100)), ..Default::default() },
    );
    // Server-side deadline (Rpc-Timeout = 100ms) fires before the 500ms sleep → ERR 451,
    // or the client deadline fires first; both are timeouts.
    match client.call(RpcRequest::new("GET", "/sleep/500")).await {
        Err(RpcError::Timeout) | Err(RpcError::Remote { code: 451, .. }) => {}
        other => panic!("{other:?}"),
    }
    // Abandon streaming bodies early; the pool must still be usable with only 2 channels.
    for _ in 0..10 {
        let (head, mut body) = client.call_streaming(RpcRequest::new("GET", "/stream/1000")).await.unwrap();
        assert_eq!(head.status, 200);
        let first = body.chunk().await.unwrap().unwrap();
        assert!(first.starts_with(b"chunk0;"));
        drop(body);
    }
    let r = client.call(RpcRequest::new("GET", "/ok").body("x")).await.unwrap();
    assert_eq!(&r.body[..], b"x");
    assert!(client.status()[0] <= 2);
}

/// Speak raw RFC 3080 frames at our listener to pin down the exact wire behaviour.
#[tokio::test]
async fn wire_level_conformance() {
    let addr = rpc_server(Config::default()).await;
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let mut got = Vec::new();
    async fn read_until(tcp: &mut TcpStream, got: &mut Vec<u8>, buf: &mut [u8], pat: &[u8]) {
        while !got.windows(pat.len()).any(|w| w == pat) {
            let n = tokio::time::timeout(Duration::from_secs(5), tcp.read(buf)).await.unwrap().unwrap();
            assert!(n > 0, "eof; got so far: {:?}", String::from_utf8_lossy(got));
            got.extend_from_slice(&buf[..n]);
        }
    }
    read_until(&mut tcp, &mut got, &mut buf, b"END\r\n").await;
    let text = String::from_utf8_lossy(&got).to_string();
    assert!(text.starts_with("RPY 0 0 . 0 "), "{text}");
    assert!(text.contains("<greeting><profile uri='urn:minta:beep:rpc:1' /><profile uri='urn:minta:beep:rpc:2' /></greeting>"), "{text}");

    let greeting = b"Content-Type: application/beep+xml\r\n\r\n<greeting />\r\n";
    tcp.write_all(format!("RPY 0 0 . 0 {}\r\n", greeting.len()).as_bytes()).await.unwrap();
    tcp.write_all(greeting).await.unwrap();
    tcp.write_all(b"END\r\n").await.unwrap();
    let start = b"Content-Type: application/beep+xml\r\n\r\n<start number='1'><profile uri='urn:minta:beep:rpc:1'/></start>";
    tcp.write_all(format!("MSG 0 1 . {} {}\r\n", greeting.len(), start.len()).as_bytes()).await.unwrap();
    tcp.write_all(start).await.unwrap();
    tcp.write_all(b"END\r\n").await.unwrap();
    got.clear();
    read_until(&mut tcp, &mut got, &mut buf, b"SEQ 1 0 262144\r\n").await;
    let text = String::from_utf8_lossy(&got).to_string();
    assert!(text.contains("RPY 0 1 . "), "{text}");
    assert!(text.contains("<profile uri='urn:minta:beep:rpc:1' />"), "{text}");

    let msg = b"Rpc-Method: GET\r\nRpc-Target: /wire\r\nRpc-Id: abc\r\n\r\n";
    tcp.write_all(format!("MSG 1 0 . 0 {}\r\n", msg.len()).as_bytes()).await.unwrap();
    tcp.write_all(msg).await.unwrap();
    tcp.write_all(b"END\r\n").await.unwrap();
    got.clear();
    read_until(&mut tcp, &mut got, &mut buf, b"END\r\n").await;
    let text = String::from_utf8_lossy(&got).to_string();
    assert!(text.starts_with("RPY 1 0 . 0 "), "{text}");
    assert!(text.contains("Rpc-Status: 201\r\n"), "{text}");
    assert!(text.contains("Rpc-Id: abc\r\n"), "{text}");

    // A frame with a wrong seqno is a protocol violation: the listener must drop us.
    tcp.write_all(b"MSG 1 1 . 999 2\r\n\r\nEND\r\n").await.unwrap();
    let mut closed = false;
    for _ in 0..50 {
        match tokio::time::timeout(Duration::from_secs(5), tcp.read(&mut buf)).await.unwrap() {
            Ok(0) | Err(_) => {
                closed = true;
                break;
            }
            Ok(_) => {}
        }
    }
    assert!(closed);
}

/// Streaming handler for rpc:2: `/duplex` echoes every request chunk as soon as it arrives
/// and ends with trailers; `/count` consumes the body without buffering it.
struct Duplex;

#[async_trait]
impl RpcHandler for Duplex {
    fn streaming(&self) -> bool {
        true
    }
    async fn handle(&self, mut req: RpcServerRequest) -> Result<RpcServerResponse, RpcFailure> {
        let mut body = req.stream.take().expect("streaming handler gets a stream");
        if req.target == "/count" {
            let mut n = 0usize;
            while let Some(p) = body.next().await.map_err(|e| RpcFailure::new(e.beep_code(), e.to_string()))? {
                if let RequestPart::Data(b) = p {
                    n += b.len();
                }
            }
            return Ok(RpcServerResponse::full(200, vec![], n.to_string()));
        }
        let (resp, tx) = RpcServerResponse::frames(200, vec![("X-Profile".into(), req.profile.into())]);
        tokio::spawn(async move {
            let mut req_trailer = String::new();
            loop {
                let item = match body.next().await {
                    Ok(Some(RequestPart::Data(b))) => BodyItem::Data(b),
                    Ok(Some(RequestPart::Trailers(t))) => {
                        req_trailer = t.iter().find(|(k, _)| k == "X-T").map(|(_, v)| v.clone()).unwrap_or_default();
                        continue;
                    }
                    Ok(None) => BodyItem::Trailers(vec![("X-Done".into(), "yes".into()), ("X-Req-T".into(), req_trailer.clone())]),
                    Err(e) => BodyItem::Abort(format!("request aborted: {e}")),
                };
                let last = !matches!(item, BodyItem::Data(_));
                if tx.send(item).await.is_err() || last {
                    return;
                }
            }
        });
        Ok(resp)
    }
}

#[tokio::test]
async fn rpc2_duplex_trailers_abort_and_unbounded_upload() {
    let s = RpcServer::bind("127.0.0.1:0", Duplex, Config::default()).await.unwrap();
    let addr = s.local_addr().unwrap();
    tokio::spawn(s.serve());
    let client = RpcClient::new(addr.to_string(), ClientOptions::default());

    // Full duplex: each echo must arrive before the next chunk is even written.
    let (mut w, pending) = client.call_duplex(RpcRequest::new("POST", "/duplex"), None).await.unwrap();
    assert!(w.is_streaming());
    w.send(Bytes::from_static(b"one")).await.unwrap();
    let (head, mut body) = pending.response().await.unwrap();
    assert_eq!((head.status, head.header("X-Profile")), (200, Some(RPC_PROFILE_V2)));
    assert_eq!(&body.chunk().await.unwrap().unwrap()[..], b"one");
    w.send(Bytes::from_static(b"two")).await.unwrap();
    assert_eq!(&body.chunk().await.unwrap().unwrap()[..], b"two");
    w.finish(vec![("X-T".into(), "42".into())]).await.unwrap();
    assert!(body.chunk().await.is_none());
    let t = body.trailers().expect("trailers");
    assert!(t.contains(&("X-Done".to_string(), "yes".to_string())) && t.contains(&("X-Req-T".to_string(), "42".to_string())), "{t:?}");

    // Dropping the writer mid-body is an abort the server can tell from a normal end.
    let (mut w, pending) = client.call_duplex(RpcRequest::new("POST", "/duplex"), None).await.unwrap();
    w.send(Bytes::from_static(b"x")).await.unwrap();
    let (_, mut body) = pending.response().await.unwrap();
    assert_eq!(&body.chunk().await.unwrap().unwrap()[..], b"x");
    drop(w);
    match body.chunk().await {
        Some(Err(RpcError::Aborted(reason))) => assert!(reason.contains("request aborted"), "{reason}"),
        other => panic!("{other:?}"),
    }

    // Streamed bodies are not bound by max_message_size (64 MiB): send 80 MiB.
    let (mut w, pending) = client.call_duplex(RpcRequest::new("PUT", "/count"), None).await.unwrap();
    let block = Bytes::from(vec![7u8; 1 << 20]);
    let sender = tokio::spawn(async move {
        for _ in 0..80 {
            w.send(block.clone()).await.unwrap();
        }
        w.finish(vec![]).await.unwrap();
    });
    let (head, body) = pending.response().await.unwrap();
    assert_eq!(head.status, 200);
    assert_eq!(&body.collect(64).await.unwrap()[..], (80usize << 20).to_string().as_bytes());
    sender.await.unwrap();

    // Channels are recycled after duplex calls.
    let r = client.call(RpcRequest::new("PUT", "/count").body("abc")).await.unwrap();
    assert_eq!(&r.body[..], b"3");
    assert!(client.status()[0] <= 3, "{:?}", client.status());
}

#[tokio::test]
async fn rpc2_client_falls_back_to_rpc1_peer() {
    let l = Listener::bind("127.0.0.1:0", Config::default(), Profiles::new().with(RPC_PROFILE, RpcProfile::new(rpc_handler()))).await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(l.serve());
    let client = RpcClient::new(addr.to_string(), ClientOptions::default());
    let (mut w, pending) = client.call_duplex(RpcRequest::new("POST", "/echo"), None).await.unwrap();
    assert!(!w.is_streaming(), "an rpc:1 peer gets a buffered body");
    w.send(Bytes::from_static(b"hello ")).await.unwrap();
    w.send(Bytes::from_static(b"rpc1")).await.unwrap();
    w.finish(vec![("X-Dropped".into(), "1".into())]).await.unwrap();
    let (head, body) = pending.response().await.unwrap();
    assert_eq!(head.status, 201);
    assert_eq!(&body.collect(1024).await.unwrap()[..], b"hello rpc1");
    match client.call_duplex(RpcRequest::new("GET", "/ws").header(H_UPGRADE, "websocket"), None).await {
        Err(RpcError::Remote { code: 504, .. }) => {}
        other => panic!("upgrade over rpc:1 must be refused, got {:?}", other.map(|_| ())),
    }
}

#[tokio::test]
async fn sasl_plain_required_before_rpc() {
    let auth = |_authzid: &str, user: &str, pass: &str| (user == "alice" && pass == "s3cret").then(|| user.to_string());
    let who = handler_fn(|req: RpcServerRequest| async move { Ok(RpcServerResponse::full(200, vec![], req.identity.unwrap_or_default())) });
    let profiles = rpc_profiles(who).with(sasl::SASL_PLAIN, sasl::PlainServer::new(auth));
    let l = Listener::bind("127.0.0.1:0", Config { require_auth: true, ..Config::default() }, profiles).await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(l.serve());

    let anonymous = RpcClient::new(addr.clone(), ClientOptions::default());
    match anonymous.call(RpcRequest::new("GET", "/whoami")).await {
        Err(e) => assert_eq!((e.beep_code(), e.http_status()), (530, 401), "{e}"),
        Ok(r) => panic!("unauthenticated call succeeded: {r:?}"),
    }
    let wrong = RpcClient::new(addr.clone(), ClientOptions { sasl: Some(sasl::PlainCredentials::new("alice", "nope")), ..Default::default() });
    match wrong.call(RpcRequest::new("GET", "/whoami")).await {
        Err(e) => assert_eq!((e.beep_code(), e.http_status()), (535, 401), "{e}"),
        Ok(r) => panic!("bad password accepted: {r:?}"),
    }
    let alice = RpcClient::new(addr, ClientOptions { sasl: Some(sasl::PlainCredentials::new("alice", "s3cret")), ..Default::default() });
    for _ in 0..3 {
        let r = alice.call(RpcRequest::new("GET", "/whoami")).await.unwrap();
        assert_eq!(&r.body[..], b"alice");
    }
}

#[tokio::test]
async fn tls_tuning_end_to_end() {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("minta-beep-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.key_pair.serialize_pem()).unwrap();

    let tls = tls::TlsServerOptions { cert_file: cert.clone(), key_file: key };
    let server = RpcServer::bind_tls("127.0.0.1:0", rpc_handler(), Config::default(), &tls).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let opts = ClientOptions {
        tls: Some(tls::TlsClientOptions { server_name: "localhost".into(), ca_file: Some(cert) }),
        ..Default::default()
    };
    let client = RpcClient::new(addr.to_string(), opts);
    let r = client.call(RpcRequest::new("POST", "/secure").body("over tls")).await.unwrap();
    assert_eq!(&r.body[..], b"over tls");

    // A plaintext client must be refused (the listener only offers the TLS profile).
    let plain = RpcClient::new(addr.to_string(), ClientOptions::default());
    assert!(plain.call(RpcRequest::new("GET", "/x")).await.is_err());
}
