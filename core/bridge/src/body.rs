use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio_util::sync::CancellationToken;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type FrameTx = mpsc::Sender<Result<Frame<Bytes>, BoxError>>;

/// Dropping the HTTP body cancels both halves of the corresponding gRPC call.
pub struct Body {
    inner: UnsyncBoxBody<Bytes, BoxError>,
    cancel: Option<CancellationToken>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Body {
    pub fn new<B>(body: B) -> Self
    where
        B: HttpBody<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        Self {
            inner: body.map_err(Into::into).boxed_unsync(),
            cancel: None,
            _permit: None,
        }
    }

    pub fn full(data: impl Into<Bytes>) -> Self {
        Self::new(Full::new(data.into()))
    }

    pub fn without_cancellation(mut self) -> Self {
        self.cancel = None;
        self
    }

    pub fn channel(
        cancel: CancellationToken,
        permit: Option<OwnedSemaphorePermit>,
    ) -> (FrameTx, Self) {
        let (tx, rx) = mpsc::channel(4);
        let mut body = Self::new(Frames(rx));
        body.cancel = Some(cancel);
        body._permit = permit;
        (tx, body)
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
    }
}

impl HttpBody for Body {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

struct Frames(mpsc::Receiver<Result<Frame<Bytes>, BoxError>>);
impl HttpBody for Frames {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.0.poll_recv(cx)
    }
}
