use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{server::conn::http1, service::service_fn, Response};
use hyper_util::rt::TokioIo;
use minta_gateway::{
    config::GrpcListener,
    transport,
    wire::{bridge_client::BridgeClient, packet::Payload, Empty, Headers, Packet, RequestHead},
};
use std::{convert::Infallible, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;

fn packet(payload: Payload) -> Packet {
    Packet {
        payload: Some(payload),
    }
}

#[tokio::test]
async fn protobuf_peer_rejects_incomplete_and_out_of_order_bodies() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = backend.local_addr().unwrap();
    let backend_task = tokio::spawn(async move {
        loop {
            let (socket, _) = backend.accept().await.unwrap();
            tokio::spawn(async move {
                let service = service_fn(|req: hyper::Request<hyper::body::Incoming>| async move {
                    let response = match req.into_body().collect().await {
                        Ok(body) => Response::new(Full::new(body.to_bytes())),
                        Err(_) => Response::new(Full::new(Bytes::from_static(b"aborted"))),
                    };
                    Ok::<_, Infallible>(response)
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let cfg = GrpcListener {
        listen: address.to_string(),
        backend: format!("http://{backend_addr}"),
        backend_ca_file: None,
        cert_file: None,
        key_file: None,
        token_env: None,
        max_body_bytes: 1024,
        timeout_ms: 2000,
        idle_timeout_ms: 2000,
        max_duration_ms: 0,
        max_in_flight: 2,
    };
    let server = tokio::spawn(transport::serve(cfg, CancellationToken::new()));
    let endpoint = format!("http://{address}");
    let mut client = None;
    for _ in 0..100 {
        if let Ok(Ok(c)) = tokio::time::timeout(
            Duration::from_millis(100),
            BridgeClient::connect(endpoint.clone()),
        )
        .await
        {
            client = Some(c);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut client = client.expect("gRPC listener starts");
    let head = || {
        packet(Payload::Request(RequestHead {
            method: "POST".into(),
            target: "/".into(),
            authority: String::new(),
            headers: vec![],
            upgrade: String::new(),
            timeout_ms: 1000,
        }))
    };
    for (packets, code) in [
        (vec![packet(Payload::End(Empty {}))], Code::InvalidArgument),
        (
            vec![head(), packet(Payload::Data(Bytes::from_static(b"short")))],
            Code::Cancelled,
        ),
        (
            vec![
                head(),
                packet(Payload::Trailers(Headers { fields: vec![] })),
                packet(Payload::Data(Bytes::from_static(b"late"))),
                packet(Payload::End(Empty {})),
            ],
            Code::InvalidArgument,
        ),
        (
            vec![
                head(),
                packet(Payload::Data(Bytes::from(vec![0; 1025]))),
                packet(Payload::End(Empty {})),
            ],
            Code::ResourceExhausted,
        ),
    ] {
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            client.exchange(tokio_stream::iter(packets)),
        )
        .await
        .unwrap();
        let error = match result {
            Err(e) => e,
            Ok(r) => {
                let mut stream = r.into_inner();
                loop {
                    match stream.message().await {
                        Err(e) => break e,
                        Ok(Some(_)) => {}
                        Ok(None) => panic!("invalid body was accepted as complete"),
                    }
                }
            }
        };
        assert_eq!(error.code(), code, "{error}");
    }
    // A valid call must still work after malformed/aborted calls.
    let mut stream = client
        .exchange(tokio_stream::iter(vec![
            head(),
            packet(Payload::Data(Bytes::from_static(b"valid"))),
            packet(Payload::End(Empty {})),
        ]))
        .await
        .unwrap()
        .into_inner();
    let mut data = Vec::new();
    while let Some(p) = stream.message().await.unwrap() {
        if let Some(Payload::Data(bytes)) = p.payload {
            data.extend_from_slice(&bytes);
        }
    }
    assert_eq!(data, b"valid");
    server.abort();
    backend_task.abort();
}
