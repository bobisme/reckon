use std::fmt;
use std::sync::{Arc, Mutex};

use asupersync::channel::mpsc;
use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use reckon_core::{Source, UsageEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStrategy {
    JsonlTail,
    SqlCursor,
    NeverCache,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderError {
    message: String,
}

impl ReaderError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl fmt::Display for ReaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ReaderError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkError {
    Closed,
    Disconnected,
    Cancelled,
    Full,
}

impl fmt::Display for SinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("sink closed"),
            Self::Disconnected => f.write_str("sink disconnected"),
            Self::Cancelled => f.write_str("sink send cancelled"),
            Self::Full => f.write_str("sink full"),
        }
    }
}

impl std::error::Error for SinkError {}

#[async_trait]
pub trait Reader: Send + Sync {
    fn source(&self) -> Source;
    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError>;
    fn cache_strategy(&self) -> CacheStrategy;
}

#[derive(Debug, Clone)]
pub struct Sink {
    inner: Arc<Mutex<Option<mpsc::Sender<UsageEvent>>>>,
}

impl Sink {
    pub const CAPACITY: usize = 1024;

    #[must_use]
    pub fn new(tx: mpsc::Sender<UsageEvent>) -> Self {
        Self { inner: Arc::new(Mutex::new(Some(tx))) }
    }

    pub async fn send(&self, cx: &Cx, event: UsageEvent) -> Result<(), SinkError> {
        let tx = self.inner.lock().expect("sink mutex poisoned").clone();
        let Some(tx) = tx else {
            return Err(SinkError::Closed);
        };

        match tx.send(cx, event).await {
            Ok(()) => Ok(()),
            Err(mpsc::SendError::Disconnected(_)) => Err(SinkError::Disconnected),
            Err(mpsc::SendError::Cancelled(_)) => Err(SinkError::Cancelled),
            Err(mpsc::SendError::Full(_)) => Err(SinkError::Full),
        }
    }

    pub fn close(&self) {
        let _ = self.inner.lock().expect("sink mutex poisoned").take();
    }
}

pub async fn run_readers(cx: &Cx, readers: Vec<Box<dyn Reader>>) -> Vec<UsageEvent> {
    let (tx, mut rx) = mpsc::channel(Sink::CAPACITY);
    let sink = Sink::new(tx);

    let mut scans = FuturesUnordered::new();
    for reader in readers {
        let cx = cx.clone();
        let sink = sink.clone();
        scans.push(async move { reader.scan(&cx, &sink).await });
    }

    let readers_done = async {
        while scans.next().await.is_some() {}
        sink.close();
    };

    let drain = async {
        let mut events = Vec::new();
        loop {
            match rx.recv(cx).await {
                Ok(event) => events.push(event),
                Err(mpsc::RecvError::Disconnected | mpsc::RecvError::Cancelled) => break,
                Err(mpsc::RecvError::Empty) => continue,
            }
        }
        events
    };

    let (_, events) = future::join(readers_done, drain).await;
    events
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use asupersync::lab::{LabConfig, LabRuntime};
    use asupersync::{Budget, CancelReason, Cx};
    use reckon_core::{ModelSlug, TokenCounts, YearMonth};

    #[derive(Debug)]
    struct MockReader {
        source: Source,
        count: usize,
        cancel_after_first_send: bool,
    }

    impl MockReader {
        fn new(source: Source, count: usize) -> Self {
            Self { source, count, cancel_after_first_send: false }
        }

        fn cancelling(source: Source, count: usize) -> Self {
            Self { source, count, cancel_after_first_send: true }
        }
    }

    #[async_trait]
    impl Reader for MockReader {
        fn source(&self) -> Source {
            self.source
        }

        async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
            for index in 0..self.count {
                match sink.send(cx, event(self.source, index)).await {
                    Ok(()) => {}
                    Err(SinkError::Cancelled) => {
                        return Outcome::Cancelled(
                            cx.cancel_reason().unwrap_or_else(CancelReason::shutdown),
                        );
                    }
                    Err(err) => return Outcome::Err(ReaderError::new(err.to_string())),
                }

                if self.cancel_after_first_send && index == 0 {
                    cx.set_cancel_reason(CancelReason::shutdown());
                }
            }

            Outcome::ok(())
        }

        fn cache_strategy(&self) -> CacheStrategy {
            CacheStrategy::NeverCache
        }
    }

    fn event(source: Source, index: usize) -> UsageEvent {
        UsageEvent {
            source,
            month: YearMonth::new(2026, 5),
            model: ModelSlug::new(format!("model-{source}-{index}")),
            provider: source.to_string(),
            project: Some("test".into()),
            tokens: TokenCounts {
                input: index as u64,
                output: 1,
                cache_read: 0,
                cache_write: 0,
                reasoning: 0,
            },
            dedup_key: format!("{source}-{index}"),
        }
    }

    fn run_on_lab<T, F>(seed: u64, f: F) -> (LabRuntime, T)
    where
        T: Send + 'static,
        F: FnOnce(Cx) -> futures::future::BoxFuture<'static, T> + Send + 'static,
    {
        let mut runtime = LabRuntime::new(LabConfig::new(seed));
        let region = runtime.state.create_root_region(Budget::INFINITE);
        let slot = Arc::new(Mutex::new(None));
        let slot_clone = Arc::clone(&slot);
        let (task_id, _handle) = runtime
            .state
            .create_task(region, Budget::INFINITE, async move {
                let cx = Cx::current().expect("task cx");
                let value = f(cx).await;
                *slot_clone.lock().expect("slot mutex poisoned") = Some(value);
            })
            .expect("create task");
        runtime.scheduler.lock().schedule(task_id, Budget::INFINITE.priority);
        runtime.run_until_quiescent();
        let value = slot.lock().expect("slot mutex poisoned").take().expect("task result");
        (runtime, value)
    }

    #[test]
    fn mock_reader_runs_deterministically_under_lab_runtime() {
        let run = |seed| {
            let (_runtime, mut events) = run_on_lab(seed, |cx| {
                Box::pin(async move {
                    run_readers(&cx, vec![Box::new(MockReader::new(Source::Claude, 100))]).await
                })
            });
            events.sort_by(|a, b| a.dedup_key.cmp(&b.dedup_key));
            events
        };

        let first = run(7);
        let second = run(7);
        assert_eq!(first, second);
        assert_eq!(first.len(), 100);
    }

    #[test]
    fn cancelling_parent_mid_scan_returns_cancelled_and_quiesces() {
        let (runtime, outcome) = run_on_lab(11, |cx| {
            Box::pin(async move {
                MockReader::cancelling(Source::Pi, 100)
                    .scan(&cx, &Sink::new(mpsc::channel(Sink::CAPACITY).0))
                    .await
            })
        });

        assert!(matches!(outcome, Outcome::Cancelled(_)));
        assert!(runtime.is_quiescent());
    }

    #[test]
    fn two_readers_saturate_bounded_channel_without_drops() {
        let (_runtime, mut events) = run_on_lab(19, |cx| {
            Box::pin(async move {
                run_readers(
                    &cx,
                    vec![
                        Box::new(MockReader::new(Source::Claude, 1_500)),
                        Box::new(MockReader::new(Source::Codex, 1_500)),
                    ],
                )
                .await
            })
        });

        events.sort_by(|a, b| a.dedup_key.cmp(&b.dedup_key));
        assert_eq!(events.len(), 3_000);
        assert_eq!(events.iter().filter(|event| event.source == Source::Claude).count(), 1_500);
        assert_eq!(events.iter().filter(|event| event.source == Source::Codex).count(), 1_500);
    }
}
