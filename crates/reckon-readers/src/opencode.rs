use std::path::PathBuf;
use std::env;

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use rusqlite::{Connection, OpenFlags};

use crate::{CacheStrategy, Reader, ReaderError, Sink, SinkError};

const PAGE_LIMIT: usize = 1000;

const SELECT_SQL: &str =
    "SELECT id, session_id, tokens_input, tokens_output, tokens_reasoning, \
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

fn open_connection(db_path: &std::path::Path) -> Result<Connection, ReaderError> {
    Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| ReaderError::new(format!("opening {}: {e}", db_path.display())))
}

#[expect(clippy::cast_sign_loss)]
const fn i64_to_u64(v: i64) -> u64 {
    if v < 0 { 0 } else { v as u64 }
}

fn read_page(conn: &Connection, after_created: i64) -> Result<Vec<MessageRow>, ReaderError> {
    let mut stmt = conn
        .prepare_cached(SELECT_SQL)
        .map_err(|e| ReaderError::new(format!("prepare: {e}")))?;

    let rows = stmt
        .query_map(rusqlite::params![after_created], |row| {
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
        })
        .map_err(|e| ReaderError::new(format!("query: {e}")))?;

    let mut page = Vec::with_capacity(PAGE_LIMIT);
    for row in rows {
        page.push(row.map_err(|e| ReaderError::new(format!("row: {e}")))?);
    }

    Ok(page)
}

#[async_trait]
impl Reader for OpenCodeReader {
    fn source(&self) -> Source {
        Source::OpenCode
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        if !self.db_path.exists() {
            return Outcome::ok(());
        }

        let conn = match open_connection(&self.db_path) {
            Ok(c) => c,
            Err(e) => return Outcome::Err(e),
        };

        let mut cursor: i64 = 0;

        loop {
            let page = match read_page(&conn, cursor) {
                Ok(p) => p,
                Err(e) => return Outcome::Err(e),
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

        Outcome::ok(())
    }

    fn cache_strategy(&self) -> CacheStrategy {
        CacheStrategy::SqlCursor
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use asupersync::lab::{LabConfig, LabRuntime};
    use asupersync::Budget;
    use rusqlite::Connection;

    use super::*;
    use crate::run_readers;

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
        runtime.scheduler.lock().schedule(task_id, Budget::INFINITE.priority);
        runtime.run_until_quiescent();
        slot.lock().expect("slot mutex poisoned").take().expect("task result")
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

    #[test]
    fn missing_db_returns_empty() {
        let reader = OpenCodeReader::with_db_path(PathBuf::from("/nonexistent/opencode.db"));
        let events: Vec<UsageEvent> = run_on_lab(1, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });
        assert!(events.is_empty());
    }

    #[test]
    fn all_zero_row_is_skipped() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let db_path = tmp.path().join("opencode.db");
        let conn = create_message_db(&db_path);

        insert_row(&conn, "zero-row", "sess-1", (0, 0, 0, 0, 0), "gpt-4o", "openai", 1_000_000);
        insert_row(&conn, "nonzero-row", "sess-1", (100, 50, 0, 0, 0), "gpt-4o", "openai", 2_000_000);
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
        conn.execute_batch("CREATE TABLE message (id TEXT, created INTEGER)").expect("create");
        conn.execute("INSERT INTO message VALUES ('x', 1000)", []).expect("insert");
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
}
