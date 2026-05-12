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

const SELECT_SQL: &str = "SELECT id, session_id, time_created, data \
     FROM message WHERE time_created > ?1 ORDER BY time_created LIMIT 1000";

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
    project: Option<String>,
    known_cost_usd: Option<f64>,
}

/// JSON blob shape used by both the SQLite `data` column and the legacy
/// file-based storage. Only fields we care about are listed.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageData {
    #[serde(default)]
    role: String,
    #[serde(default, rename = "modelID")]
    model_id: String,
    #[serde(default, rename = "providerID")]
    provider_id: String,
    #[serde(default)]
    tokens: MessageTokens,
    #[serde(default)]
    cost: Option<f64>,
    #[serde(default)]
    path: Option<MessagePath>,
    // Present in legacy file blobs and in SQLite `data` blobs; we prefer
    // the `time_created` column when available, but legacy parsing reads
    // this field.
    #[serde(default)]
    time: MessageTime,
    // Legacy file blobs put the id inside the JSON. SQLite messages have
    // an `id` column, so this is optional.
    #[serde(default)]
    id: Option<String>,
}

#[derive(Default, Deserialize)]
struct MessageTime {
    #[serde(default)]
    created: i64,
}

#[derive(Default, Deserialize)]
struct MessageTokens {
    #[serde(default)]
    input: i64,
    #[serde(default)]
    output: i64,
    #[serde(default)]
    reasoning: i64,
    #[serde(default)]
    cache: MessageCacheTokens,
}

#[derive(Default, Deserialize)]
struct MessageCacheTokens {
    #[serde(default)]
    read: i64,
    #[serde(default)]
    write: i64,
}

#[derive(Default, Deserialize)]
struct MessagePath {
    #[serde(default)]
    cwd: Option<String>,
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

fn message_data_to_tokens(data: &MessageData) -> TokenCounts {
    TokenCounts {
        input: i64_to_u64(data.tokens.input),
        output: i64_to_u64(data.tokens.output),
        reasoning: i64_to_u64(data.tokens.reasoning),
        cache_read: i64_to_u64(data.tokens.cache.read),
        cache_write: i64_to_u64(data.tokens.cache.write),
    }
}

fn read_page(conn: &Connection, after_created: i64) -> rusqlite::Result<Vec<MessageRow>> {
    let mut stmt = conn.prepare_cached(SELECT_SQL)?;

    let rows = stmt.query_map(rusqlite::params![after_created], |row| {
        let id: String = row.get(0)?;
        // index 1 is session_id — not needed for UsageEvent
        let created_ms: i64 = row.get(2)?;
        let data_json: String = row.get(3)?;
        Ok((id, created_ms, data_json))
    })?;

    let mut page = Vec::with_capacity(PAGE_LIMIT);
    for row in rows {
        let (id, created_ms, data_json) = row?;

        // Parse the JSON blob. If the JSON is malformed we skip this row
        // rather than abort the scan — keep the cursor moving so callers
        // still see good rows. We surface this through a None push so the
        // caller can advance `cursor` past the malformed timestamp.
        let data: MessageData = match serde_json::from_str(&data_json) {
            Ok(d) => d,
            Err(_) => {
                // Yield a placeholder row with zero tokens so the cursor
                // advances and the row is then filtered out downstream.
                page.push(MessageRow {
                    id,
                    tokens: TokenCounts::default(),
                    model_id: String::new(),
                    provider_id: String::new(),
                    created_ms,
                    project: None,
                    known_cost_usd: None,
                });
                continue;
            }
        };

        // Only assistant messages carry token usage in OpenCode.
        if data.role != "assistant" {
            page.push(MessageRow {
                id,
                tokens: TokenCounts::default(),
                model_id: String::new(),
                provider_id: String::new(),
                created_ms,
                project: None,
                known_cost_usd: None,
            });
            continue;
        }

        let tokens = message_data_to_tokens(&data);
        let project = data.path.as_ref().and_then(|p| p.cwd.clone());
        let known_cost_usd = match data.cost {
            Some(c) if c > 0.0 => Some(c),
            _ => None,
        };

        page.push(MessageRow {
            id,
            tokens,
            model_id: data.model_id,
            provider_id: data.provider_id,
            created_ms,
            project,
            known_cost_usd,
        });
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
    let message: MessageData = serde_json::from_str(&json)
        .map_err(|e| ReaderError::new(format!("parsing {}: {e}", path.display())))?;

    let tokens = message_data_to_tokens(&message);

    if tokens.total() == 0 {
        return Ok(None);
    }

    let Some(id) = message.id.clone() else {
        return Ok(None);
    };

    let project = message.path.as_ref().and_then(|p| p.cwd.clone());
    let known_cost_usd = match message.cost {
        Some(c) if c > 0.0 => Some(c),
        _ => None,
    };

    Ok(Some(UsageEvent {
        source: Source::OpenCode,
        month: YearMonth::from_utc(message.time.created / 1000),
        model: model_map::canonical(
            Source::OpenCode,
            &message.model_id,
            Some(&message.provider_id),
        ),
        provider: message.provider_id,
        project,
        tokens,
        dedup_key: id,
        known_cost_usd,
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
                    project: row.project,
                    tokens: row.tokens,
                    dedup_key: row.id,
                    known_cost_usd: row.known_cost_usd,
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
    use serde_json::json;

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
                session_id TEXT NOT NULL,
                time_created INTEGER NOT NULL,
                time_updated INTEGER NOT NULL,
                data TEXT NOT NULL
            )",
        )
        .expect("create table");
        conn
    }

    /// Build an assistant message JSON blob with the given tokens / model / cost.
    fn assistant_blob(
        model_id: &str,
        provider_id: &str,
        tokens: (i64, i64, i64, i64, i64),
        created_ms: i64,
        cost: f64,
        cwd: Option<&str>,
    ) -> String {
        let mut blob = json!({
            "role": "assistant",
            "time": {"created": created_ms, "completed": created_ms + 100},
            "modelID": model_id,
            "providerID": provider_id,
            "tokens": {
                "input": tokens.0,
                "output": tokens.1,
                "reasoning": tokens.2,
                "cache": {"read": tokens.3, "write": tokens.4},
            },
            "cost": cost,
        });
        if let Some(cwd) = cwd {
            blob["path"] = json!({"cwd": cwd, "root": cwd});
        }
        blob.to_string()
    }

    fn insert_assistant_row(
        conn: &Connection,
        id: &str,
        session_id: &str,
        tokens: (i64, i64, i64, i64, i64),
        model_id: &str,
        provider_id: &str,
        created_ms: i64,
    ) {
        let data = assistant_blob(model_id, provider_id, tokens, created_ms, 0.0, None);
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) \
             VALUES (?1, ?2, ?3, ?3, ?4)",
            rusqlite::params![id, session_id, created_ms, data],
        )
        .expect("insert row");
    }

    fn insert_raw_row(
        conn: &Connection,
        id: &str,
        session_id: &str,
        time_created: i64,
        data: &str,
    ) {
        conn.execute(
            "INSERT INTO message (id, session_id, time_created, time_updated, data) \
             VALUES (?1, ?2, ?3, ?3, ?4)",
            rusqlite::params![id, session_id, time_created, data],
        )
        .expect("insert raw row");
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
                "role": "assistant",
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
                "role": "assistant",
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
                "role": "assistant",
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

        insert_assistant_row(
            &conn,
            "zero-row",
            "sess-1",
            (0, 0, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        insert_assistant_row(
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
    fn non_assistant_rows_are_skipped() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // A user-role row with tokens populated — should still be skipped.
        let user_blob = json!({
            "role": "user",
            "modelID": "gpt-4o",
            "providerID": "openai",
            "tokens": {"input": 999, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}},
        })
        .to_string();
        insert_raw_row(&conn, "user-row", "sess-1", 1_000_000, &user_blob);
        insert_assistant_row(
            &conn,
            "asst-row",
            "sess-1",
            (50, 25, 0, 0, 0),
            "gpt-4o",
            "openai",
            2_000_000,
        );
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(20, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].dedup_key, "asst-row");
    }

    #[test]
    fn basic_event_fields_are_correct() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // time_created = 1_748_000_000_000 ms → 1_748_000_000 s → 2025-05
        let created_ms: i64 = 1_748_000_000_000;
        let blob = assistant_blob(
            "anthropic/claude-3-5-sonnet",
            "anthropic",
            (500, 200, 10, 300, 100),
            created_ms,
            0.0123,
            Some("/home/bob/src/proj"),
        );
        insert_raw_row(&conn, "msg-abc", "sess-1", created_ms, &blob);
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
        assert_eq!(e.project.as_deref(), Some("/home/bob/src/proj"));
        assert_eq!(e.known_cost_usd, Some(0.0123));
    }

    #[test]
    fn zero_cost_field_is_none() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        insert_assistant_row(
            &conn,
            "no-cost",
            "sess-1",
            (10, 5, 0, 0, 0),
            "model-x",
            "provider-x",
            1_000_000,
        );
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(21, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert!(events[0].known_cost_usd.is_none());
    }

    #[test]
    fn malformed_json_row_is_skipped() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        insert_raw_row(&conn, "bad-row", "sess-1", 1_000_000, "not json");
        insert_assistant_row(
            &conn,
            "good-row",
            "sess-1",
            (10, 5, 0, 0, 0),
            "gpt-4o",
            "openai",
            2_000_000,
        );
        drop(conn);

        let reader = OpenCodeReader::with_db_path(db_path);
        let events: Vec<UsageEvent> = run_on_lab(22, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].dedup_key, "good-row");
    }

    #[test]
    fn five_thousand_rows_processed_within_budget() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // Insert 5000 rows with strictly increasing time_created
        for i in 0i64..5000 {
            insert_assistant_row(
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
            elapsed.as_millis() < 1500,
            "expected < 1500ms, got {}ms",
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
            insert_assistant_row(
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
        insert_assistant_row(
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
    fn missing_token_fields_treated_as_zero() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        // Assistant row but with only `input` token set in the JSON.
        let blob = json!({
            "role": "assistant",
            "modelID": "model",
            "providerID": "prov",
            "tokens": {"input": 42},
        })
        .to_string();
        insert_raw_row(&conn, "null-test", "sess", 1000, &blob);
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
        insert_assistant_row(
            &conn,
            "msg-1",
            "sess-1",
            (100, 50, 0, 0, 0),
            "gpt-4o",
            "openai",
            1_000_000,
        );
        insert_assistant_row(
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

        // Warm scan emits zero NEW rows but replays the 2 persisted events.
        assert_eq!(events_warm.len(), 2);
        assert!(
            elapsed.as_millis() < 100,
            "warm scan took {elapsed:?}, expected < 100ms"
        );
    }

    #[test]
    fn warm_scan_emits_only_new_rows() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);
        insert_assistant_row(
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
        insert_assistant_row(
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

        // Warm scan emits 1 new row but replays the 1 persisted event = 2 total.
        assert_eq!(events_warm.len(), 2);
        let msg2 = events_warm
            .iter()
            .find(|e| e.dedup_key == "msg-2")
            .expect("new row present");
        assert_eq!(msg2.tokens.input, 300);
        let msg1 = events_warm
            .iter()
            .find(|e| e.dedup_key == "msg-1")
            .expect("persisted row replayed");
        assert_eq!(msg1.tokens.input, 100);
    }
}
