//! Per-call lifetime: cancellation plus progress timers.
//!
//! Until the response head arrives, a call may go `timeout` without progress (request
//! body bytes), so an upload never times out while it moves and the backend has
//! `timeout` after the last byte to answer. Once the head has arrived, the call ends
//! after `idle` without a byte in either direction, so streams and WebSocket tunnels
//! live as long as they are used. `max`, when set, caps the whole call.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tonic::Status;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timers {
    pub timeout: Duration,
    pub idle: Duration,
    pub max: Option<Duration>,
}

impl Timers {
    pub fn from_ms(timeout_ms: u64, idle_timeout_ms: u64, max_duration_ms: u64) -> Self {
        Self {
            timeout: Duration::from_millis(timeout_ms),
            idle: Duration::from_millis(idle_timeout_ms),
            max: (max_duration_ms > 0).then(|| Duration::from_millis(max_duration_ms)),
        }
    }
}

/// A cheap handle to one call; clones share the same timers and token.
#[derive(Clone)]
pub struct Call(Arc<Inner>);

struct Inner {
    cancel: CancellationToken,
    start: Instant,
    /// Milliseconds after `start` of the latest progress.
    last: AtomicU64,
    /// Inactivity allowed, in milliseconds: `timeout`, then `idle` once streaming.
    limit: AtomicU64,
    streaming: AtomicBool,
    idle: Duration,
    end: Option<Instant>,
    expired: OnceLock<Status>,
}

fn millis(d: Duration) -> u64 {
    d.as_millis().min(u128::from(u64::MAX)) as u64
}

impl Inner {
    fn idle_due(&self) -> Instant {
        let ms = self
            .last
            .load(Relaxed)
            .saturating_add(self.limit.load(Relaxed));
        self.start + Duration::from_millis(ms)
    }

    fn expiry(&self, now: Instant) -> Option<Status> {
        if self.end.is_some_and(|end| now >= end) {
            return Some(Status::deadline_exceeded(
                "call exceeded its maximum duration",
            ));
        }
        (now >= self.idle_due()).then(|| {
            if self.streaming.load(Relaxed) {
                Status::deadline_exceeded("idle timeout: no data moved")
            } else {
                Status::deadline_exceeded("request deadline exceeded")
            }
        })
    }
}

impl Call {
    /// Starts the timers; cancelling `cancel` ends the call (and its watchdog).
    pub fn start(cancel: CancellationToken, timers: Timers) -> Self {
        let start = Instant::now();
        let inner = Arc::new(Inner {
            cancel: cancel.clone(),
            start,
            last: AtomicU64::new(0),
            limit: AtomicU64::new(millis(timers.timeout)),
            streaming: AtomicBool::new(false),
            idle: timers.idle,
            end: timers.max.map(|max| start + max),
            expired: OnceLock::new(),
        });
        tokio::spawn(watchdog(Arc::downgrade(&inner), cancel));
        Self(inner)
    }

    pub fn token(&self) -> &CancellationToken {
        &self.0.cancel
    }

    pub fn cancel(&self) {
        self.0.cancel.cancel();
    }

    /// Records progress: a body or tunnel chunk moved.
    pub fn touch(&self) {
        let now = millis(self.0.start.elapsed());
        self.0.last.fetch_max(now, Relaxed);
    }

    /// The caller asked for a shorter response deadline.
    pub fn shorten(&self, timeout: Duration) {
        if !self.0.streaming.load(Relaxed) {
            self.0.limit.fetch_min(millis(timeout), Relaxed);
        }
    }

    /// The response head arrived: from now on only inactivity ends the call.
    pub fn streaming(&self) {
        self.0.limit.store(millis(self.0.idle), Relaxed);
        self.0.streaming.store(true, Relaxed);
        self.touch();
    }

    /// Why the call ended, once its token is cancelled.
    pub fn status(&self) -> Status {
        self.0
            .expired
            .get()
            .cloned()
            .unwrap_or_else(|| Status::cancelled("call cancelled"))
    }

    /// Runs `future` until it completes or the call ends.
    pub async fn bounded<T>(&self, future: impl Future<Output = T>) -> Result<T, Status> {
        tokio::select! {
            biased;
            _ = self.0.cancel.cancelled() => Err(self.status()),
            r = future => Ok(r),
        }
    }
}

async fn watchdog(call: Weak<Inner>, cancel: CancellationToken) {
    loop {
        let Some(due) = call.upgrade().map(|c| {
            let idle = c.idle_due();
            c.end.map_or(idle, |end| idle.min(end))
        }) else {
            return;
        };
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep_until(due) => {}
        }
        let Some(c) = call.upgrade() else { return };
        // Progress since the sleep began moves the due time; loop to wait again.
        if let Some(reason) = c.expiry(Instant::now()) {
            let _ = c.expired.set(reason);
            c.cancel.cancel();
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timers(timeout: u64, idle: u64, max: u64) -> Timers {
        Timers::from_ms(timeout, idle, max)
    }

    #[tokio::test(start_paused = true)]
    async fn progress_extends_the_response_deadline() {
        let call = Call::start(CancellationToken::new(), timers(100, 1000, 0));
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(80)).await;
            call.touch();
        }
        assert!(
            !call.token().is_cancelled(),
            "a moving upload must not time out"
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(call.token().is_cancelled());
        assert_eq!(call.status().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(call.status().message(), "request deadline exceeded");
    }

    #[tokio::test(start_paused = true)]
    async fn streaming_switches_to_the_idle_timer() {
        let call = Call::start(CancellationToken::new(), timers(100, 500, 0));
        call.streaming();
        tokio::time::sleep(Duration::from_millis(400)).await;
        call.touch();
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            !call.token().is_cancelled(),
            "long but active stream survives"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(call.token().is_cancelled());
        assert_eq!(call.status().message(), "idle timeout: no data moved");
    }

    #[tokio::test(start_paused = true)]
    async fn maximum_duration_and_shorter_caller_deadline() {
        let call = Call::start(CancellationToken::new(), timers(1000, 1000, 300));
        call.streaming();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(90)).await;
            call.touch();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            call.status().message(),
            "call exceeded its maximum duration"
        );

        let call = Call::start(CancellationToken::new(), timers(1000, 1000, 0));
        call.shorten(Duration::from_millis(50));
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(call.token().is_cancelled());

        let cancel = CancellationToken::new();
        let call = Call::start(cancel.clone(), timers(1000, 1000, 0));
        cancel.cancel();
        assert_eq!(
            call.bounded(std::future::pending::<()>())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Cancelled
        );
    }
}
