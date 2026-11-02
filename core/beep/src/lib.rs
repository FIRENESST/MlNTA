//! # minta-beep
//!
//! RFC 3080/3081 BEEP for Tokio, plus the MINTA RPC profile (`urn:minta:beep:rpc:1`)
//! that carries HTTP-shaped backend requests over BEEP channels.
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use minta_beep::rpc::{RpcClient, RpcRequest, ClientOptions};
//! let client = RpcClient::new("127.0.0.1:10288", ClientOptions::default());
//! let resp = client.call(RpcRequest::new("GET", "/hello")).await?;
//! println!("{} {:?}", resp.status, resp.body);
//! # Ok(()) }
//! ```

pub mod error;
pub mod frame;
pub mod mgmt;
pub mod mime;
pub mod rpc;
pub mod session;
#[cfg(feature = "tls")]
pub mod tls;

pub use error::{code, Error, Result};
pub use mime::{Entity, Headers};
pub use rpc::{RpcClient, RpcRequest, RpcResponse, RPC_PROFILE};
pub use session::{Channel, Config, IncomingMessage, Listener, ProfileHandler, Profiles, Reply, ReplyEvent, ReplyStream, Responder, Role, Session};
