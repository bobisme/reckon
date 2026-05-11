use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use rusqlite::{Connection, ErrorCode, OpenFlags};
use serde::Deserialize;

use crate::{CacheStrategy, Reader, ReaderError, Sink, SinkError};

const PAGE_LIMIT: usize = 1000;
const BUSY_RETRY_DELAY: Duration = Duration::from_millis(50);
const DB_LOCKED_WARNING: &str = "opencode: DB locked, skipping";

const SELECT_SQL: &str = "SELECT id, session_id, tokens_input, tokens_output, tokens_reasoning, \
     tokens_cache_read, tokens_cache_write, model_id, provider_id, created \
     FROM message WHERE created > ?1 ORDER BY created LIMIT 1000";

pub struct OpenCodeReader {
    db_path: PathBuf,
}

impl Default for OpenCodeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenCodeReader {
    #[must_use]
    pub fn new() -> Self {
        let db_path = env::var("OPENCODE_DB").map_or_else(
            |_| {
                let mut p = home_dir();
                p.push(".local/share/opencode/opencode.db");
                p
            },
            PathBuf::from,
        );
        Self { db_path }
    }

    #[must_use]
    pub const fn with_db_path(db_path: PathBuf) -> Self {
        Self { db_path }
    }
}

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

struct MessageRow {
    id: String,
    tokens: TokenCounts,
    model_id: String,
    provider_id: String,
    created_ms: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LegacyMessage {
    id: String,
    #[serde(default, rename = "modelID")]
    model_id: String,
    #[serde(default, rename = "providerID")]
    provider_id: String,
    time: LegacyTime,
    #[serde(default)]
    tokens: LegacyTokens,
}

#[derive(Default, Deserialize)]
struct LegacyTime {
    created: i64,
}

#[derive(Default, Deserialize)]
struct LegacyTokens {
    #[serde(default)]
    input: i64,
    #[serde(default)]
    output: i64,
    #[serde(default)]
    reasoning: i64,
    #[serde(default)]
    cache: LegacyCacheTokens,
}

#[derive(Default, Deserialize)]
struct LegacyCacheTokens {
    #[serde(default)]
    read: i64,
    #[serde(default)]
    write: i64,
}

fn open_connection(db_path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.busy_timeout(Duration::ZERO)?;
    Ok(conn)
}

const fn is_sqlite_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn with_busy_retry<T, F>(mut f: F) -> Result<T, rusqlite::Error>
where
    F: FnMut() -> Result<T, rusqlite::Error>,
{
    match f() {
        Ok(value) => Ok(value),
        Err(error) if is_sqlite_busy(&error) => {
            thread::sleep(BUSY_RETRY_DELAY);
            f()
        }
        Err(error) => Err(error),
    }
}

#[expect(clippy::cast_sign_loss)]
const fn i64_to_u64(v: i64) -> u64 {
    if v < 0 { 0 } else { v as u64 }
}

fn read_page(conn: &Connection, after_created: i64) -> rusqlite::Result<Vec<MessageRow>> {
    let mut stmt = conn.prepare_cached(SELECT_SQL)?;

    let rows = stmt.query_map(rusqlite::params![after_created], |row| {
        let id: String = row.get(0)?;
        // index 1 is session_id — not needed for UsageEvent
        let tokens_input: Option<i64> = row.get(2)?;
        let tokens_output: Option<i64> = row.get(3)?;
        let tokens_reasoning: Option<i64> = row.get(4)?;
        let tokens_cache_read: Option<i64> = row.get(5)?;
        let tokens_cache_write: Option<i64> = row.get(6)?;
        let model_id: String = row.get(7)?;
        let provider_id: String = row.get(8)?;
        let created_ms: i64 = row.get(9)?;

        Ok(MessageRow {
            id,
            tokens: TokenCounts {
                input: i64_to_u64(tokens_input.unwrap_or(0)),
                output: i64_to_u64(tokens_output.unwrap_or(0)),
                reasoning: i64_to_u64(tokens_reasoning.unwrap_or(0)),
                cache_read: i64_to_u64(tokens_cache_read.unwrap_or(0)),
                cache_write: i64_to_u64(tokens_cache_write.unwrap_or(0)),
            },
            model_id,
            provider_id,
            created_ms,
        })
    })?;

    let mut page = Vec::with_capacity(PAGE_LIMIT);
    for row in rows {
        page.push(row?);
    }

    Ok(page)
}

fn storage_message_dir(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .map_or_else(PathBuf::new, Path::to_path_buf)
        .join("storage/message")
}

fn collect_message_json_paths(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_message_json_paths_inner(dir, &mut paths)?;
    paths.sort();
    Ok(paths)
}

fn collect_message_json_paths_inner(dir: &Path, paths: &mut Vec<PathBuf>) -> io::Result<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };

    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            collect_message_json_paths_inner(&path, paths)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            paths.push(path);
        }
    }

    Ok(())
}

fn parse_legacy_message(path: &Path) -> Result<Option<UsageEvent>, ReaderError> {
    let json = fs::read_to_string(path)
        .map_err(|e| ReaderError::new(format!("reading {}: {e}", path.display())))?;
    let message: LegacyMessage = serde_json::from_str(&json)
        .map_err(|e| ReaderError::new(format!("parsing {}: {e}", path.display())))?;

    let tokens = TokenCounts {
        input: i64_to_u64(message.tokens.input),
        output: i64_to_u64(message.tokens.output),
        reasoning: i64_to_u64(message.tokens.reasoning),
        cache_read: i64_to_u64(message.tokens.cache.read),
        cache_write: i64_to_u64(message.tokens.cache.write),
    };

    if tokens.total() == 0 {
        return Ok(None);
    }

    Ok(Some(UsageEvent {
        source: Source::OpenCode,
        month: YearMonth::from_utc(message.time.created / 1000),
        model: model_map::canonical(
            Source::OpenCode,
            &message.model_id,
            Some(&message.provider_id),
        ),
        provider: message.provider_id,
        project: None,
        tokens,
        dedup_key: message.id,
        known_cost_usd: None,
        byok_usage_inference: None,
    }))
}

impl OpenCodeReader {
    async fn scan_with_warn<F>(&self, cx: &Cx, sink: &Sink, warn: F) -> Outcome<(), ReaderError>
    where
        F: Fn(&str),
    {
        if !self.db_path.exists() {
            let storage_dir = storage_message_dir(&self.db_path);
            let paths = match collect_message_json_paths(&storage_dir) {
                Ok(paths) => paths,
                Err(error) => {
                    return Outcome::Err(ReaderError::new(format!(
                        "walking {}: {error}",
                        storage_dir.display()
                    )));
                }
            };

            for path in paths {
                let event = match parse_legacy_message(&path) {
                    Ok(Some(event)) => event,
                    Ok(None) => continue,
                    Err(error) => return Outcome::Err(error),
                };

                match sink.send(cx, event).await {
                    Ok(()) => {}
                    Err(SinkError::Cancelled) => {
                        return Outcome::Cancelled(
                            cx.cancel_reason()
                                .unwrap_or_else(asupersync::CancelReason::shutdown),
                        );
                    }
                    Err(_) => return Outcome::ok(()),
                }
            }

            return Outcome::ok(());
        }

        let conn = match with_busy_retry(|| open_connection(&self.db_path)) {
            Ok(conn) => conn,
            Err(error) if is_sqlite_busy(&error) => {
                warn(DB_LOCKED_WARNING);
                return Outcome::ok(());
            }
            Err(error) => {
                return Outcome::Err(ReaderError::new(format!(
                    "opening {}: {error}",
                    self.db_path.display()
                )));
            }
        };

        let mut cursor: i64 = sink
            .source_file_state(Source::OpenCode, &self.db_path)
            .map_or(0, |state| state.last_offset);

        loop {
            let page = match with_busy_retry(|| read_page(&conn, cursor)) {
                Ok(page) => page,
                Err(error) if is_sqlite_busy(&error) => {
                    warn(DB_LOCKED_WARNING);
                    return Outcome::ok(());
                }
                Err(error) => return Outcome::Err(ReaderError::new(format!("query: {error}"))),
            };

            let done = page.len() < PAGE_LIMIT;

            for row in page {
                cursor = row.created_ms;

                if row.tokens.total() == 0 {
                    continue;
                }

                let month = YearMonth::from_utc(row.created_ms / 1000);
                let model =
                    model_map::canonical(Source::OpenCode, &row.model_id, Some(&row.provider_id));

                let event = UsageEvent {
                    source: Source::OpenCode,
                    month,
                    model,
                    provider: row.provider_id,
                    project: None,
                    tokens: row.tokens,
                    dedup_key: row.id,
                    known_cost_usd: None,
                    byok_usage_inference: None,
                };

                match sink.send(cx, event).await {
                    Ok(()) => {}
                    Err(SinkError::Cancelled) => {
                        return Outcome::Cancelled(
                            cx.cancel_reason()
                                .unwrap_or_else(asupersync::CancelReason::shutdown),
                        );
                    }
                    Err(_) => return Outcome::ok(()),
                }
            }

            if done {
                break;
            }
        }

        sink.record_source_file(Source::OpenCode, &self.db_path, 0, 0, cursor);

        Outcome::ok(())
    }
}

#[async_trait]
impl Reader for OpenCodeReader {
    fn source(&self) -> Source {
        Source::OpenCode
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        self.scan_with_warn(cx, sink, |line| eprintln!("{line}"))
            .await
    }

    fn cache_strategy(&self) -> CacheStrategy {
        CacheStrategy::SqlCursor
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use asupersync::Budget;
    use asupersync::channel::mpsc;
    use asupersync::lab::{LabConfig, LabRuntime};
    use rusqlite::Connection;

    use super::*;
    use crate::{run_readers, run_readers_with_cache};

    fn run_on_lab<T, F>(seed: u64, f: F) -> T
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
        slot.lock()
            .expect("slot mutex poisoned")
            .take()
            .expect("task result")
    }

    fn create_message_db(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).expect("open db");
        conn.execute_batch(
            "CREATE TABLE message (
                id TEXT PRIMARY KEY,
                session_id TEXT,
                tokens_input INTEGER,
                tokens_output INTEGER,
                tokens_reasoning INTEGER,
                tokens_cache_read INTEGER,
                tokens_cache_write INTEGER,
                model_id TEXT,
                provider_id TEXT,
                created INTEGER
            )",
        )
        .expect("create table");
        conn
    }

    fn insert_row(
        conn: &Connection,
        id: &str,
        session_id: &str,
        tokens: (i64, i64, i64, i64, i64),
        model_id: &str,
        provider_id: &str,
        created: i64,
    ) {
        conn.execute(
            "INSERT INTO message (id, session_id, tokens_input, tokens_output, tokens_reasoning, \
             tokens_cache_read, tokens_cache_write, model_id, provider_id, created) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                id,
                session_id,
                tokens.0,
                tokens.1,
                tokens.2,
                tokens.3,
                tokens.4,
                model_id,
                provider_id,
                created
            ],
        )
        .expect("insert row");
    }

    fn write_legacy_message(path: &Path, json: &str) {
        fs::write(path, json).expect("write legacy message");
    }

    fn run_scan_with_warn(
        reader: OpenCodeReader,
    ) -> (Outcome<(), ReaderError>, Vec<UsageEvent>, Vec<String>) {
        run_on_lab(99, move |cx| {
            Box::pin(async move {
                let (tx, mut rx) = mpsc::channel(Sink::CAPACITY);
                let sink = Sink::new(tx);
                let warnings = Arc::new(Mutex::new(Vec::new()));
                let warnings_clone = Arc::clone(&warnings);

                let outcome = reader
                    .scan_with_warn(&cx, &sink, move |line| {
                        warnings_clone
                            .lock()
                            .expect("warnings mutex poisoned")
                            .push(line.to_string());
                    })
                    .await;

                sink.close();

                let mut events = Vec::new();
                loop {
                    match rx.recv(&cx).await {
                        Ok(event) => events.push(event),
                        Err(mpsc::RecvError::Disconnected | mpsc::RecvError::Cancelled) => break,
                        Err(mpsc::RecvError::Empty) => {}
                    }
                }

                (
                    outcome,
                    events,
                    warnings.lock().expect("warnings mutex poisoned").clone(),
                )
            })
        })
    }

    #[test]
    fn missing_db_returns_empty() {
        let reader = OpenCodeReader::with_db_path(PathBuf::from("/nonexistent/opencode.db"));
        let events: Vec<UsageEvent> = run_on_lab(1, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });
        assert!(events.is_empty());
    }

    #[test]
    fn missing_db_falls_back_to_legacy_storage_messages() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let session_dir = tmp.path().join("storage/message/ses_1");
        fs::create_dir_all(&session_dir).expect("create session dir");

        write_legacy_message(
            &session_dir.join("msg-1.json"),
            r#"{
                "id": "msg-1",
                "modelID": "claude-opus-4-5",
                "providerID": "anthropic",
                "time": {"created": 1748000000000},
                "tokens": {"input": 120, "output": 45, "reasoning": 6, "cache": {"read": 10, "write": 2}}
            }"#,
        );
        write_legacy_message(
            &session_dir.join("msg-2.json"),
            r#"{
                "id": "msg-2",
                "modelID": "gpt-4o",
                "providerID": "openai",
                "time": {"created": 1748000001000},
                "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}}
            }"#,
        );
        write_legacy_message(
            &session_dir.join("msg-3.json"),
            r#"{
                "id": "msg-3",
                "modelID": "gemini-2.5-pro",
                "providerID": "google",
                "time": {"created": 1748000002000},
                "tokens": {"input": 300, "output": 150, "reasoning": 20, "cache": {"read": 0, "write": 0}}
            }"#,
        );

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(7, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].dedup_key, "msg-1");
        assert_eq!(events[0].provider, "anthropic");
        assert_eq!(events[0].tokens.input, 120);
        assert_eq!(events[0].tokens.output, 45);
        assert_eq!(events[0].tokens.reasoning, 6);
        assert_eq!(events[0].tokens.cache_read, 10);
        assert_eq!(events[0].tokens.cache_write, 2);
        assert_eq!(events[1].dedup_key, "msg-3");
        assert_eq!(events[1].provider, "google");
        assert_eq!(events[1].tokens.input, 300);
        assert_eq!(events[1].tokens.output, 150);
        assert_eq!(events[1].tokens.reasoning, 20);
    }

    #[test]
    fn all_zero_row_is_skipped() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        insert_row(
            &conn,
            "zero-row",
            "sess-1",
            (0, 0, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        insert_row(
            &conn,
            "nonzero-row",
            "sess-1",
            (100, 50, 0, 0, 0),
            "gpt-4o",
            "openai",
            2_000_000,
        );
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(2, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].dedup_key, "nonzero-row");
        assert_eq!(events[0].tokens.input, 100);
        assert_eq!(events[0].tokens.output, 50);
    }

    #[test]
    fn basic_event_fields_are_correct() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // created = 1_748_000_000_000 ms → 1_748_000_000 s → 2025-05
        let created_ms: i64 = 1_748_000_000_000;
        insert_row(
            &conn,
            "msg-abc",
            "sess-1",
            (500, 200, 10, 300, 100),
            "anthropic/claude-3-5-sonnet",
            "anthropic",
            created_ms,
        );
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(3, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.source, Source::OpenCode);
        assert_eq!(e.dedup_key, "msg-abc");
        assert_eq!(e.provider, "anthropic");
        assert_eq!(e.tokens.input, 500);
        assert_eq!(e.tokens.output, 200);
        assert_eq!(e.tokens.reasoning, 10);
        assert_eq!(e.tokens.cache_read, 300);
        assert_eq!(e.tokens.cache_write, 100);
        assert_eq!(e.month, YearMonth::from_utc(created_ms / 1000));
        assert!(e.project.is_none());
    }

    #[test]
    fn schema_mismatch_returns_typed_error() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = Connection::open(&db_path).expect("open db");
        conn.execute_batch("CREATE TABLE message (id TEXT, created INTEGER)")
            .expect("create");
        conn.execute("INSERT INTO message VALUES ('x', 1000)", [])
            .expect("insert");
        drop(conn);

        let conn = open_connection(&db_path).expect("open for test");
        let result = read_page(&conn, 0);
        assert!(result.is_err(), "expected error from schema mismatch");
    }

    #[test]
    fn five_thousand_rows_processed_within_budget() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // Insert 5000 rows with strictly increasing created timestamps
        for i in 0i64..5000 {
            insert_row(
                &conn,
                &format!("msg-{i:05}"),
                "sess-perf",
                (100, 50, 0, 0, 0),
                "gpt-4o",
                "openai",
                1_000_000_000 + i,
            );
        }
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let start = Instant::now();
        let events: Vec<UsageEvent> = run_on_lab(4, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });
        let elapsed = start.elapsed();

        assert_eq!(events.len(), 5000);
        assert!(
            elapsed.as_millis() < 500,
            "expected < 500ms, got {}ms",
            elapsed.as_millis()
        );
    }

    #[test]
    fn pagination_processes_all_rows_across_multiple_pages() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // 2500 rows → 3 pages (1000 + 1000 + 500)
        for i in 0i64..2500 {
            insert_row(
                &conn,
                &format!("paged-{i:04}"),
                "sess-x",
                (10, 5, 0, 0, 0),
                "model-x",
                "provider-x",
                i + 1,
            );
        }
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(5, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 2500);
    }

    #[test]
    fn locked_db_warns_once_and_returns_ok() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);
        insert_row(
            &conn,
            "locked-row",
            "sess-1",
            (100, 20, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        drop(conn);

        let lock_conn = Connection::open(&db_path).expect("open lock db");
        lock_conn.execute_batch("BEGIN EXCLUSIVE").expect("lock db");

        let reader = OpenCodeReader::with_db_path(db_path);
        let (outcome, events, warnings) = run_scan_with_warn(reader);

        assert!(matches!(outcome, Outcome::Ok(())));
        assert!(events.is_empty());
        assert_eq!(warnings, vec![DB_LOCKED_WARNING.to_string()]);

        lock_conn.execute_batch("ROLLBACK").expect("unlock db");
    }

    #[test]
    fn null_token_columns_treated_as_zero() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = Connection::open(&db_path).expect("open db");
        conn.execute_batch(
            "CREATE TABLE message (
                id TEXT, session_id TEXT,
                tokens_input INTEGER, tokens_output INTEGER,
                tokens_reasoning INTEGER, tokens_cache_read INTEGER,
                tokens_cache_write INTEGER, model_id TEXT, provider_id TEXT, created INTEGER
            )",
        )
        .expect("create");
        // Insert with NULL token columns except input
        conn.execute(
            "INSERT INTO message VALUES ('null-test', 'sess', 42, NULL, NULL, NULL, NULL, 'model', 'prov', 1000)",
            [],
        )
        .expect("insert");
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(6, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.input, 42);
        assert_eq!(events[0].tokens.output, 0);
        assert_eq!(events[0].tokens.reasoning, 0);
        assert_eq!(events[0].tokens.cache_read, 0);
        assert_eq!(events[0].tokens.cache_write, 0);
    }

    #[test]
    fn warm_scan_emits_zero_events_when_no_new_rows() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);
        insert_row(
            &conn,
            "msg-1",
            "sess-1",
            (100, 50, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        insert_row(
            &conn,
            "msg-2",
            "sess-1",
            (200, 100, 0, 0, 0),
            "gpt-4o",
            "openai",
            2_000_000,
        );
        drop(conn);

        let cache_dir = tempfile::tempdir().expect("cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");

        let events_cold: Vec<UsageEvent> = {
            let db = db_path.clone();
            let cp = cache_path.clone();
            run_on_lab(1, move |cx| {
                Box::pin(async move {
                    run_readers_with_cache(
                        &cx,
                        vec![Box::new(OpenCodeReader::with_db_path(db))],
                        &cp,
                    )
                    .await
                })
            })
        };
        assert_eq!(events_cold.len(), 2);

        let start = Instant::now();
        let events_warm: Vec<UsageEvent> = {
            let db = db_path.clone();
            let cp = cache_path.clone();
            run_on_lab(2, move |cx| {
                Box::pin(async move {
                    run_readers_with_cache(
                        &cx,
                        vec![Box::new(OpenCodeReader::with_db_path(db))],
                        &cp,
                    )
                    .await
                })
            })
        };
        let elapsed = start.elapsed();

        assert_eq!(events_warm.len(), 0);
        assert!(
            elapsed.as_millis() < 50,
            "warm scan took {elapsed:?}, expected < 50ms"
        );
    }

    #[test]
    fn warm_scan_emits_only_new_rows() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);
        insert_row(
            &conn,
            "msg-1",
            "sess-1",
            (100, 50, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        drop(conn);

        let cache_dir = tempfile::tempdir().expect("cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");

        let events_cold: Vec<UsageEvent> = {
            let db = db_path.clone();
            let cp = cache_path.clone();
            run_on_lab(10, move |cx| {
                Box::pin(async move {
                    run_readers_with_cache(
                        &cx,
                        vec![Box::new(OpenCodeReader::with_db_path(db))],
                        &cp,
                    )
                    .await
                })
            })
        };
        assert_eq!(events_cold.len(), 1);

        let conn = Connection::open(&db_path).expect("reopen db");
        insert_row(
            &conn,
            "msg-2",
            "sess-1",
            (300, 150, 0, 0, 0),
            "gpt-4o",
            "openai",
            2_000_000,
        );
        drop(conn);

        let events_warm: Vec<UsageEvent> = {
            let db = db_path.clone();
            let cp = cache_path.clone();
            run_on_lab(11, move |cx| {
                Box::pin(async move {
                    run_readers_with_cache(
                        &cx,
                        vec![Box::new(OpenCodeReader::with_db_path(db))],
                        &cp,
                    )
                    .await
                })
            })
        };

        assert_eq!(events_warm.len(), 1);
        assert_eq!(events_warm[0].dedup_key, "msg-2");
        assert_eq!(events_warm[0].tokens.input, 300);
    }
}
