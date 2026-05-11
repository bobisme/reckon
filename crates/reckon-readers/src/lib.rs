pub mod claude;
pub mod codex;
pub mod gemini;
pub mod opencode;
pub mod openrouter;
pub mod pi;

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use asupersync::channel::mpsc;
use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use reckon_core::{Source, UsageEvent, open_cache};
use rusqlite::params;

use rusqlite::Connection;

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
        Self {
            message: message.into(),
        }
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

const EVENT_BATCH_SIZE: usize = 5_000;

#[derive(Debug, Clone)]
pub(crate) struct SourceFileState {
    mtime_ns: i64,
    size_bytes: i64,
    last_offset: i64,
}

struct SinkCache {
    conn: Connection,
    events: Vec<UsageEvent>,
    source_files: HashMap<(Source, String), SourceFileState>,
}

#[derive(Clone)]
pub struct Sink {
    inner: Arc<Mutex<Option<mpsc::Sender<UsageEvent>>>>,
    cache: Option<Arc<Mutex<SinkCache>>>,
}

impl fmt::Debug for Sink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sink")
            .field("cached", &self.cache.is_some())
            .finish_non_exhaustive()
    }
}

impl Sink {
    pub const CAPACITY: usize = 1024;

    #[must_use]
    pub fn new(tx: mpsc::Sender<UsageEvent>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(tx))),
            cache: None,
        }
    }

    #[must_use]
    pub fn new_cached(tx: mpsc::Sender<UsageEvent>, cache_path: &Path) -> Self {
        let conn = open_cache(cache_path);
        let source_files = load_source_files(&conn);
        Self {
            inner: Arc::new(Mutex::new(Some(tx))),
            cache: Some(Arc::new(Mutex::new(SinkCache {
                conn,
                events: Vec::new(),
                source_files,
            }))),
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    ///
    /// # Errors
    ///
    /// Returns `SinkError` if the channel is closed, disconnected, cancelled, or full.
    pub async fn send(&self, cx: &Cx, event: UsageEvent) -> Result<(), SinkError> {
        let tx = self.inner.lock().expect("sink mutex poisoned").clone();
        let Some(tx) = tx else {
            return Err(SinkError::Closed);
        };

        if let Some(cache) = self.cache.as_ref() {
            let mut guard = cache.lock().expect("sink cache mutex poisoned");
            guard.events.push(event.clone());
            if guard.events.len() >= EVENT_BATCH_SIZE {
                let state = &mut *guard;
                persist_events(&mut state.conn, &mut state.events);
            }
            drop(guard);
        }

        match tx.send(cx, event).await {
            Ok(()) => Ok(()),
            Err(mpsc::SendError::Disconnected(_)) => Err(SinkError::Disconnected),
            Err(mpsc::SendError::Cancelled(_)) => Err(SinkError::Cancelled),
            Err(mpsc::SendError::Full(_)) => Err(SinkError::Full),
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn record_source_file(
        &self,
        source: Source,
        path: &Path,
        mtime_ns: i64,
        size_bytes: i64,
        last_offset: i64,
    ) {
        if let Some(cache) = self.cache.as_ref() {
            let mut state = cache.lock().expect("sink cache mutex poisoned");
            state.source_files.insert(
                (source, path.to_string_lossy().to_string()),
                SourceFileState {
                    mtime_ns,
                    size_bytes,
                    last_offset,
                },
            );
        }
    }

    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub(crate) fn source_file_state(&self, source: Source, path: &Path) -> Option<SourceFileState> {
        let cache = self.cache.as_ref()?;
        let state = cache.lock().expect("sink cache mutex poisoned");
        state
            .source_files
            .get(&(source, path.to_string_lossy().to_string()))
            .cloned()
    }

    /// # Panics
    ///
    /// Panics if the internal mutexes are poisoned.
    pub fn close(&self) {
        let _ = self.inner.lock().expect("sink mutex poisoned").take();

        if let Some(cache) = self.cache.as_ref() {
            let mut guard = cache.lock().expect("sink cache mutex poisoned");
            let state = &mut *guard;
            persist_events(&mut state.conn, &mut state.events);
            persist_source_files(&mut state.conn, &state.source_files);
            drop(guard);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonlScanPlan {
    Skip {
        mtime_ns: i64,
        size_bytes: i64,
    },
    ReadFrom {
        start_offset: i64,
        mtime_ns: i64,
        size_bytes: i64,
    },
}

pub(crate) fn plan_jsonl_scan(
    sink: &Sink,
    source: Source,
    path: &Path,
    mtime_ns: i64,
    size_bytes: i64,
) -> JsonlScanPlan {
    let Some(previous) = sink.source_file_state(source, path) else {
        return JsonlScanPlan::ReadFrom {
            start_offset: 0,
            mtime_ns,
            size_bytes,
        };
    };

    if previous.mtime_ns == mtime_ns && previous.size_bytes == size_bytes {
        return JsonlScanPlan::Skip {
            mtime_ns,
            size_bytes,
        };
    }

    if size_bytes > previous.size_bytes && mtime_ns >= previous.mtime_ns {
        return JsonlScanPlan::ReadFrom {
            start_offset: previous.last_offset.min(size_bytes),
            mtime_ns,
            size_bytes,
        };
    }

    JsonlScanPlan::ReadFrom {
        start_offset: 0,
        mtime_ns,
        size_bytes,
    }
}

pub(crate) fn read_jsonl_prefix(path: &Path, end_offset: i64) -> io::Result<String> {
    #[cfg(test)]
    JSONL_OPEN_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(
        u64::try_from(end_offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative JSONL offset"))?,
    )
    .read_to_end(&mut bytes)?;
    String::from_utf8(bytes).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

pub(crate) fn read_jsonl_from_offset(path: &Path, start_offset: i64) -> io::Result<String> {
    #[cfg(test)]
    JSONL_OPEN_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    if start_offset == 0 {
        return std::fs::read_to_string(path);
    }

    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(u64::try_from(start_offset).map_err(
        |_| io::Error::new(io::ErrorKind::InvalidData, "negative JSONL offset"),
    )?))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}

fn persist_events(conn: &mut Connection, events: &mut Vec<UsageEvent>) {
    let stmt = "INSERT OR IGNORE INTO events \
        (source, dedup_key, month, model, provider, project, input, output, cache_read, cache_write, reasoning, known_cost_usd, byok_usage_inference) \
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)";

    for chunk in events.chunks(EVENT_BATCH_SIZE) {
        let tx = conn
            .transaction()
            .expect("failed to begin cache events transaction");
        for event in chunk {
            tx.execute(
                stmt,
                params![
                    event.source.to_string(),
                    event.dedup_key.as_str(),
                    event.month.to_string(),
                    event.model.as_str(),
                    event.provider.as_str(),
                    event.project.as_deref(),
                    event.tokens.input,
                    event.tokens.output,
                    event.tokens.cache_read,
                    event.tokens.cache_write,
                    event.tokens.reasoning,
                    event.known_cost_usd,
                    event.byok_usage_inference,
                ],
            )
            .expect("failed to persist usage event");
        }
        tx.commit()
            .expect("failed to commit cache events transaction");
    }

    events.clear();
}

fn load_source_files(conn: &Connection) -> HashMap<(Source, String), SourceFileState> {
    let mut stmt = conn
        .prepare("SELECT source, path, mtime_ns, size_bytes, last_offset FROM source_files")
        .expect("prepare source-files query");
    let rows = stmt
        .query_map([], |row| {
            let source: String = row.get(0)?;
            let source = match source.as_str() {
                "claude" => Source::Claude,
                "codex" => Source::Codex,
                "gemini" => Source::Gemini,
                "pi" => Source::Pi,
                "opencode" => Source::OpenCode,
                "openrouter" => Source::OpenRouter,
                _ => return Ok(None),
            };
            let path: String = row.get(1)?;
            let mtime_ns: i64 = row.get(2)?;
            let size_bytes: i64 = row.get(3)?;
            let last_offset: i64 = row.get(4)?;
            Ok(Some((
                (source, path),
                SourceFileState {
                    mtime_ns,
                    size_bytes,
                    last_offset,
                },
            )))
        })
        .expect("query source-files rows");

    let mut source_files = HashMap::new();
    for row in rows {
        if let Some((key, state)) = row.expect("read source-files row") {
            source_files.insert(key, state);
        }
    }
    source_files
}

fn persist_source_files(
    conn: &mut Connection,
    source_files: &HashMap<(Source, String), SourceFileState>,
) {
    if source_files.is_empty() {
        return;
    }

    let tx = conn
        .transaction()
        .expect("failed to begin cache source-files transaction");
    for ((source, path), state) in source_files {
        tx.execute(
            "INSERT INTO source_files (source, path, mtime_ns, size_bytes, last_offset)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(source, path) DO UPDATE
             SET mtime_ns = excluded.mtime_ns,
                 size_bytes = excluded.size_bytes,
                 last_offset = excluded.last_offset",
            params![
                source.to_string(),
                path,
                state.mtime_ns,
                state.size_bytes,
                state.last_offset,
            ],
        )
        .expect("failed to persist source file state");
    }
    tx.commit()
        .expect("failed to commit cache source-files transaction");
}

pub async fn run_readers(cx: &Cx, readers: Vec<Box<dyn Reader>>) -> Vec<UsageEvent> {
    let (tx, mut rx) = mpsc::channel(Sink::CAPACITY);
    let sink = Sink::new(tx);

    run_readers_inner(cx, readers, sink, &mut rx).await
}

pub async fn run_readers_with_cache(
    cx: &Cx,
    readers: Vec<Box<dyn Reader>>,
    cache_path: &Path,
) -> Vec<UsageEvent> {
    let (tx, mut rx) = mpsc::channel(Sink::CAPACITY);
    let sink = Sink::new_cached(tx, cache_path);

    run_readers_inner(cx, readers, sink, &mut rx).await
}

async fn run_readers_inner(
    cx: &Cx,
    readers: Vec<Box<dyn Reader>>,
    sink: Sink,
    rx: &mut mpsc::Receiver<UsageEvent>,
) -> Vec<UsageEvent> {
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
                Err(mpsc::RecvError::Empty) => {}
            }
        }
        events
    };

    let ((), events) = future::join(readers_done, drain).await;
    events
}

#[cfg(test)]
static JSONL_OPEN_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_jsonl_open_count() {
    JSONL_OPEN_COUNT.store(0, std::sync::atomic::Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn jsonl_open_count() -> usize {
    JSONL_OPEN_COUNT.load(std::sync::atomic::Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use asupersync::lab::{LabConfig, LabRuntime};
    use asupersync::{Budget, CancelReason, Cx};
    use reckon_core::{ModelSlug, TokenCounts, YearMonth, open_cache};
    use tempfile::TempDir;

    #[derive(Debug)]
    struct MockReader {
        source: Source,
        count: usize,
        cancel_after_first_send: bool,
    }

    impl MockReader {
        fn new(source: Source, count: usize) -> Self {
            Self {
                source,
                count,
                cancel_after_first_send: false,
            }
        }

        fn cancelling(source: Source, count: usize) -> Self {
            Self {
                source,
                count,
                cancel_after_first_send: true,
            }
        }
    }

    #[derive(Debug)]
    struct DuplicateKeyReader {
        source: Source,
        count: usize,
        dedup_key: String,
    }

    impl DuplicateKeyReader {
        fn new(source: Source, count: usize, dedup_key: &str) -> Self {
            Self {
                source,
                count,
                dedup_key: dedup_key.to_string(),
            }
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

    #[async_trait]
    impl Reader for DuplicateKeyReader {
        fn source(&self) -> Source {
            self.source
        }

        async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
            for _ in 0..self.count {
                let mut event = event(self.source, 0);
                event.dedup_key = self.dedup_key.clone();
                match sink.send(cx, event).await {
                    Ok(()) => {}
                    Err(err) => return Outcome::Err(ReaderError::new(err.to_string())),
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
            known_cost_usd: None,
            byok_usage_inference: None,
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
        runtime
            .scheduler
            .lock()
            .schedule(task_id, Budget::INFINITE.priority);
        runtime.run_until_quiescent();
        let value = slot
            .lock()
            .expect("slot mutex poisoned")
            .take()
            .expect("task result");
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
        assert_eq!(
            events
                .iter()
                .filter(|event| event.source == Source::Claude)
                .count(),
            1_500
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.source == Source::Codex)
                .count(),
            1_500
        );
    }

    #[test]
    fn cached_run_deduplicates_events_in_db() {
        let cache_dir = TempDir::new().expect("temp cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");
        let (runtime, event_count) = run_on_lab(29, {
            let cache_path = cache_path.clone();
            move |cx| {
                Box::pin(async move {
                    let events = run_readers_with_cache(
                        &cx,
                        vec![Box::new(DuplicateKeyReader::new(
                            Source::Claude,
                            12,
                            "dup-key",
                        ))],
                        &cache_path,
                    )
                    .await;

                    let conn = open_cache(&cache_path);
                    let persisted_events: i64 = conn
                        .query_row("SELECT COUNT(*) FROM events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .expect("query events count");
                    let persisted_files: i64 = conn
                        .query_row("SELECT COUNT(*) FROM source_files", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .expect("query source files count");

                    (events.len(), persisted_events, persisted_files)
                })
            }
        });

        assert_eq!(runtime.is_quiescent(), true);
        let (event_count, persisted_events, persisted_source_files) = event_count;
        assert_eq!(event_count, 12);
        assert_eq!(persisted_events, 1);
        assert_eq!(persisted_source_files, 0);
    }

    #[test]
    fn cold_scan_1000_events_all_persisted() {
        let cache_dir = TempDir::new().expect("temp cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");
        let start = std::time::Instant::now();
        let (_runtime, (event_count, db_rows)) = run_on_lab(37, {
            let cache_path = cache_path.clone();
            move |cx| {
                Box::pin(async move {
                    let events = run_readers_with_cache(
                        &cx,
                        vec![Box::new(MockReader::new(Source::Claude, 1000))],
                        &cache_path,
                    )
                    .await;

                    let conn = open_cache(&cache_path);
                    let rows: i64 = conn
                        .query_row("SELECT COUNT(*) FROM events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .expect("query events count");

                    (events.len(), rows)
                })
            }
        });
        let elapsed = start.elapsed();

        assert_eq!(event_count, 1000);
        assert_eq!(db_rows, 1000);
        assert!(
            elapsed.as_millis() < 200,
            "cold scan took {elapsed:?}, expected < 200ms"
        );
    }

    #[test]
    fn cold_scan_flushes_intermediate_batches() {
        let cache_dir = TempDir::new().expect("temp cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");
        let (_runtime, db_rows) = run_on_lab(41, {
            let cache_path = cache_path.clone();
            move |cx| {
                Box::pin(async move {
                    let _events = run_readers_with_cache(
                        &cx,
                        vec![Box::new(MockReader::new(Source::Claude, 7_500))],
                        &cache_path,
                    )
                    .await;

                    let conn = open_cache(&cache_path);
                    let rows: i64 = conn
                        .query_row("SELECT COUNT(*) FROM events", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .expect("query events count");
                    rows
                })
            }
        });

        assert_eq!(db_rows, 7_500);
    }
}
