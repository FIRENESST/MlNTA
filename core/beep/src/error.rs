use std::sync::Arc;

/// Errors raised by the BEEP layer.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(Arc<std::io::Error>),
    /// The peer violated RFC 3080/3081; the session is terminated.
    #[error("protocol violation: {0}")]
    Protocol(String),
    /// The session is gone (peer closed, local close, or an earlier fatal error).
    #[error("session closed: {0}")]
    Closed(String),
    /// The peer answered a channel-management request with an `<error>`.
    #[error("refused by peer ({code}): {message}")]
    Refused { code: u32, message: String },
    #[error("timed out")]
    Timeout,
    #[error("message exceeds {0} bytes")]
    TooLarge(usize),
    #[error("tls: {0}")]
    Tls(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// RFC 3080 §8 reply codes.
pub mod code {
    pub const SUCCESS: u32 = 200;
    pub const SERVICE_NOT_AVAILABLE: u32 = 421;
    pub const ACTION_NOT_TAKEN_TEMP: u32 = 450;
    pub const ABORTED: u32 = 451;
    pub const TEMP_AUTH_FAILURE: u32 = 454;
    pub const SYNTAX_ERROR: u32 = 500;
    pub const PARAM_SYNTAX_ERROR: u32 = 501;
    pub const PARAM_NOT_IMPLEMENTED: u32 = 504;
    pub const AUTH_REQUIRED: u32 = 530;
    pub const AUTH_TOO_WEAK: u32 = 534;
    pub const AUTH_FAILURE: u32 = 535;
    pub const NOT_AUTHORIZED: u32 = 537;
    pub const ENCRYPTION_REQUIRED: u32 = 538;
    pub const ACTION_NOT_TAKEN: u32 = 550;
    pub const PARAM_INVALID: u32 = 553;
    pub const TRANSACTION_FAILED: u32 = 554;
}
