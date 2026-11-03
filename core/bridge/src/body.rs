use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

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
        let mut body = Self::new(Frames {
            rx,
            call: Box::pin(cancel.clone().cancelled_owned()),
            failed: false,
        });
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

/// How long an abandoned request body is still read and discarded.
const LINGER: Duration = Duration::from_secs(5);

/// A request body that keeps being read when it is dropped before its end, i.e. the
/// reply finished or failed while the client was still uploading. Hyper then closes the
/// HTTP/1.1 connection, and closing with unread bytes resets it: the client's upload
/// fails and it never reads the reply. Up to `budget` further bytes are discarded for
/// at most `LINGER`, so a client that finishes its upload can read the reply.
pub struct Lingering<B: HttpBody<Data = Bytes> + Unpin + Send + 'static> {
    inner: Option<B>,
    budget: usize,
}

impl<B: HttpBody<Data = Bytes> + Unpin + Send + 'static> Lingering<B> {
    pub fn new(body: B, budget: usize) -> Self {
        Self {
            inner: Some(body),
            budget,
        }
    }
}

impl<B: HttpBody<Data = Bytes> + Unpin + Send + 'static> HttpBody for Lingering<B> {
    type Data = Bytes;
    type Error = B::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let polled = Pin::new(inner).poll_frame(cx);
        if matches!(polled, Poll::Ready(None | Some(Err(_)))) {
            self.inner = None;
        }
        polled
    }
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().is_none_or(|b| b.is_end_stream())
    }
    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map_or_else(|| SizeHint::with_exact(0), |b| b.size_hint())
    }
}

impl<B: HttpBody<Data = Bytes> + Unpin + Send + 'static> Drop for Lingering<B> {
    fn drop(&mut self) {
        let Some(mut body) = self.inner.take().filter(|b| !b.is_end_stream()) else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut budget = self.budget;
        runtime.spawn(async move {
            let until = tokio::time::Instant::now() + LINGER;
            while let Ok(Some(Ok(frame))) = tokio::time::timeout_at(until, body.frame()).await {
                let n = frame.data_ref().map_or(0, Bytes::len);
                match budget.checked_sub(n) {
                    Some(rest) => budget = rest,
                    None => break,
                }
            }
        });
    }
}

/// A channel body fails, instead of ending, when its call is cancelled first. Otherwise
/// a cancelled upload or response would look complete to whoever reads it (e.g. Hyper
/// would finish a chunked upload to the backend with its last chunk).
struct Frames {
    rx: mpsc::Receiver<Result<Frame<Bytes>, BoxError>>,
    call: Pin<Box<WaitForCancellationFutureOwned>>,
    failed: bool,
}

impl Frames {
    fn cancelled(&mut self) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.failed = true;
        Poll::Ready(Some(Err("call cancelled before the body ended".into())))
    }
}

impl HttpBody for Frames {
    type Data = Bytes;
    type Error = BoxError;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        if self.failed {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(None) if self.call.as_mut().poll(cx).is_ready() => self.cancelled(),
            Poll::Pending if self.call.as_mut().poll(cx).is_ready() => self.cancelled(),
            polled => polled,
        }
    }
}
