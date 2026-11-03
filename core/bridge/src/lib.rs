pub mod body;
pub mod call;
pub mod config;
pub mod frontend;
pub mod transport;

pub mod wire {
    tonic::include_proto!("minta.bridge.v1");
}
