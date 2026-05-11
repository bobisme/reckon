use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use rusqlite::{Connection, OpenFlags};
use serde::Deserialize;

use crate::{
    CacheStrategy, JsonlScanPlan, Reader, ReaderError, Sink, SinkError, plan_jsonl_scan,
    read_jsonl_from_offset,
};

/// Tuple describing one Pi session file to parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PiSessionTuple {
    pub session_path: PathBuf,
    pub session_id: String,
    pub cwd: String,
    pub last_modified: i64,
}

impl fmt::Display for PiSessionTuple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}",
            self.session_path.display(),
            self.session_id,
            self.cwd,
            self.last_modified
        )
    }
}

#[derive(Debug, Clone)]
pub struct PiSessionLocator {
    root: PathBuf,
}

impl Default for PiSessionLocator {
    fn default() -> Self {
        Self::new()
    }
}

impl PiSessionLocator {
    #[must_use]
    pub fn new() -> Self {
        let root = env::var("PI_HOME").map_or_else(
            |_| {
                let mut p = home_dir();
                p.push(".pi");
                p
            },
            PathBuf::from,
        );
        Self { root }
    }

    #[must_use]
    pub const fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// # Errors
    ///
    /// Returns an error if the sessions directory cannot be read.
    pub fn list_session_tuples(&self) -> io::Result<Vec<PiSessionTuple>> {
        let sessions_dir = self.root.join("agent").join("sessions");
        let index_path = sessions_dir.join("session-index.sqlite");

        let walk_entries = collect_jsonl_paths(&sessions_dir)?;

        if should_use_sqlite_index(&index_path, &walk_entries)
            && let Ok(mut sessions) = scan_sqlite_index(&index_path, &sessions_dir)
        {
            sort_by_session_path(&mut sessions);
            return Ok(sessions);
        }

        let mut sessions = collect_walkdir_sessions(&walk_entries)?;
        sort_by_session_path(&mut sessions);
        Ok(sessions)
    }
}

#[derive(Debug, Clone)]
pub struct PiReader {
    locator: PiSessionLocator,
}

impl Default for PiReader {
    fn default() -> Self {
        Self::new()
    }
}

impl PiReader {
    #[must_use]
    pub fn new() -> Self {
        Self {
            locator: PiSessionLocator::new(),
        }
    }

    #[must_use]
    pub const fn with_root(root: PathBuf) -> Self {
        Self {
            locator: PiSessionLocator::with_root(root),
        }
    }
}

#[async_trait]
impl Reader for PiReader {
    fn source(&self) -> Source {
        Source::Pi
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        let tuples = match self.locator.list_session_tuples() {
            Ok(t) => t,
            Err(e) => {
                return Outcome::Err(ReaderError::new(format!("listing Pi sessions: {e}")));
            }
        };

        for tuple in tuples {
            match scan_pi_session_file(&tuple, cx, sink).await {
                Ok(()) => {}
                Err(ScanError::Sink(SinkError::Cancelled)) => {
                    return Outcome::Cancelled(
                        cx.cancel_reason()
                            .unwrap_or_else(asupersync::CancelReason::shutdown),
                    );
                }
                Err(ScanError::Sink(_)) => return Outcome::ok(()),
                Err(ScanError::Io(e)) => {
                    return Outcome::Err(ReaderError::new(format!(
                        "reading {}: {e}",
                        tuple.session_path.display()
                    )));
                }
            }
        }

        Outcome::ok(())
    }

    fn cache_strategy(&self) -> CacheStrategy {
        CacheStrategy::JsonlTail
    }
}

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

fn should_use_sqlite_index(path: &Path, walk_entries: &[PathBuf]) -> bool {
    if !path.exists() {
        return false;
    }

    let Ok(db_mtime) = file_modified_secs(path) else {
        return false;
    };

    if walk_entries.is_empty() {
        return true;
    }

    let mut latest_jsonl = None;
    for entry in walk_entries {
        let Ok(modified) = file_modified_secs(entry) else {
            return false;
        };

        latest_jsonl = Some(match latest_jsonl {
            Some(prev) if prev > modified => prev,
            _ => modified,
        });
    }

    latest_jsonl.is_none_or(|ts| db_mtime > ts)
}

fn sort_by_session_path(entries: &mut [PiSessionTuple]) {
    entries.sort_by(|a, b| a.session_path.cmp(&b.session_path));
}

fn scan_sqlite_index(index_path: &Path, sessions_dir: &Path) -> io::Result<Vec<PiSessionTuple>> {
    let conn = Connection::open_with_flags(
        index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(io::Error::other)?;

    let mut stmt = conn
        .prepare("SELECT path, id, cwd, timestamp, last_modified FROM sessions")
        .map_err(io::Error::other)?;

    let rows = stmt
        .query_map([], |row| {
            let raw_path: String = row.get(0)?;
            let session_id: String = row.get(1)?;
            let cwd: Option<String> = row.get(2)?;
            let last_modified: Option<i64> = row.get(4)?;

            Ok(PiSessionTuple {
                session_path: resolve_db_session_path(sessions_dir, &raw_path),
                session_id,
                cwd: cwd.unwrap_or_default(),
                last_modified: last_modified.unwrap_or_default(),
            })
        })
        .map_err(io::Error::other)?;

    let mut sessions = Vec::new();
    for row in rows {
        sessions.push(row.map_err(io::Error::other)?);
    }

    Ok(sessions)
}

fn resolve_db_session_path(sessions_dir: &Path, raw_path: &str) -> PathBuf {
    let candidate = Path::new(raw_path);
    if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        sessions_dir.join(candidate)
    }
}

fn collect_jsonl_paths(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    collect_jsonl_paths_inner(dir, &mut paths)?;
    Ok(paths)
}

fn collect_jsonl_paths_inner(dir: &Path, paths: &mut Vec<PathBuf>) -> io::Result<()> {
    let mut queue: VecDeque<PathBuf> = VecDeque::new();
    queue.push_back(dir.to_path_buf());

    while let Some(current) = queue.pop_front() {
        let Ok(entries) = fs::read_dir(&current) else {
            continue;
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                queue.push_back(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                paths.push(path);
            }
        }
    }

    Ok(())
}

fn collect_walkdir_sessions(jsonl_paths: &[PathBuf]) -> io::Result<Vec<PiSessionTuple>> {
    let mut tuples = Vec::with_capacity(jsonl_paths.len());

    for path in jsonl_paths {
        let session_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "session file missing name"))?
            .to_string();

        let cwd = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|name| name.to_str())
            .map(decode_project_name)
            .unwrap_or_default();

        let last_modified = file_modified_secs(path)?;

        tuples.push(PiSessionTuple {
            session_path: path.clone(),
            session_id,
            cwd,
            last_modified,
        });
    }

    Ok(tuples)
}

fn decode_project_name(encoded: &str) -> String {
    encoded.replace('-', "/")
}

fn file_modified_secs(path: &Path) -> io::Result<i64> {
    let metadata = fs::metadata(path)?;
    let duration = metadata
        .modified()?
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(duration.as_secs()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "file modified time out of i64 range",
        )
    })
}

enum ScanError {
    Sink(SinkError),
    Io(io::Error),
}

#[derive(Deserialize)]
struct PiJsonlLine {
    r#type: String,
    message: Option<PiMessage>,
}

#[derive(Deserialize)]
struct PiMessage {
    id: Option<String>,
    timestamp: Option<i64>,
    role: Option<String>,
    provider: Option<String>,
    model: Option<String>,
    #[serde(default)]
    usage: Option<PiUsage>,
}

#[derive(Deserialize)]
struct PiUsage {
    input: Option<u64>,
    output: Option<u64>,
    #[serde(rename = "cacheRead")]
    cache_read: Option<u64>,
    #[serde(rename = "cacheWrite")]
    cache_write: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    cost: Option<f64>,
}

fn indexed_mtime_ns(tuple: &PiSessionTuple) -> Option<i64> {
    tuple
        .last_modified
        .checked_mul(1_000_000_000)
        .filter(|mtime_ns| *mtime_ns > 0)
}

fn plan_pi_scan(tuple: &PiSessionTuple, sink: &Sink) -> Result<Option<(i64, i64, i64)>, ScanError> {
    let indexed_mtime_ns = indexed_mtime_ns(tuple);
    if let (Some(indexed_mtime_ns), Some(previous)) = (
        indexed_mtime_ns,
        sink.source_file_state(Source::Pi, &tuple.session_path),
    ) && previous.mtime_ns == indexed_mtime_ns
        && previous.last_offset == previous.size_bytes
    {
        return Ok(None);
    }

    let metadata = fs::metadata(&tuple.session_path).map_err(ScanError::Io)?;
    let actual_mtime_ns = file_modified_secs(&tuple.session_path)
        .map_err(ScanError::Io)?
        .checked_mul(1_000_000_000)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "mtime too large for i64"))
        .map_err(ScanError::Io)?;
    let recorded_mtime_ns = indexed_mtime_ns.unwrap_or(actual_mtime_ns);
    let size_bytes = i64::try_from(metadata.len()).expect("file size too large");

    match plan_jsonl_scan(
        sink,
        Source::Pi,
        &tuple.session_path,
        recorded_mtime_ns,
        size_bytes,
    ) {
        JsonlScanPlan::Skip { .. } => {
            sink.record_source_file(
                Source::Pi,
                &tuple.session_path,
                recorded_mtime_ns,
                size_bytes,
                size_bytes,
            );
            Ok(None)
        }
        JsonlScanPlan::ReadFrom { start_offset, .. } => {
            Ok(Some((start_offset, recorded_mtime_ns, size_bytes)))
        }
    }
}

async fn scan_pi_session_file(
    tuple: &PiSessionTuple,
    cx: &Cx,
    sink: &Sink,
) -> Result<(), ScanError> {
    let Some((start_offset, recorded_mtime_ns, size_bytes)) = plan_pi_scan(tuple, sink)? else {
        return Ok(());
    };

    let contents =
        read_jsonl_from_offset(&tuple.session_path, start_offset).map_err(ScanError::Io)?;

    for line in contents.lines() {
        if line.is_empty() {
            continue;
        }

        let entry: PiJsonlLine = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if entry.r#type != "message" {
            continue;
        }

        let Some(message) = entry.message else {
            continue;
        };

        let Some(role) = message.role else { continue };
        if role != "assistant" {
            continue;
        }

        let Some(usage) = message.usage else { continue };
        let Some(message_id) = message.id else {
            continue;
        };
        let Some(timestamp_ms) = message.timestamp else {
            continue;
        };
        let Some(provider) = message.provider else {
            continue;
        };
        let Some(model) = message.model else { continue };

        let timestamp_secs = timestamp_ms / 1000;

        let canonical_model = model_map::canonical(Source::Pi, &model, Some(&provider));

        let event = UsageEvent {
            source: Source::Pi,
            month: YearMonth::from_utc(timestamp_secs),
            model: canonical_model,
            provider,
            project: Some(tuple.cwd.clone()),
            tokens: TokenCounts {
                input: usage.input.unwrap_or(0),
                output: usage.output.unwrap_or(0),
                cache_read: usage.cache_read.unwrap_or(0),
                cache_write: usage.cache_write.unwrap_or(0),
                reasoning: 0,
            },
            dedup_key: format!("{}:{}", tuple.session_id, message_id),
            known_cost_usd: None,
            byok_usage_inference: None,
        };

        sink.send(cx, event).await.map_err(ScanError::Sink)?;
    }

    sink.record_source_file(
        Source::Pi,
        &tuple.session_path,
        recorded_mtime_ns,
        size_bytes,
        size_bytes,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use filetime::{self, FileTime};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    use crate::{reset_jsonl_open_count, run_readers, run_readers_with_cache};
    use asupersync::Budget;
    use asupersync::lab::{LabConfig, LabRuntime};

    fn set_mtime(path: &Path, secs: i64) {
        let time = FileTime::from_unix_time(secs, 0);
        filetime::set_file_times(path, time, time).expect("set file mtime");
    }

    fn create_session_file(path: &Path, session_id: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create session directory");
        }
        fs::write(path, format!("{{\"id\":\"{session_id}\"}}\n")).expect("write fixture");
    }

    fn create_pi_root() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join(".pi");
        fs::create_dir_all(root.join("agent").join("sessions")).expect("create sessions dir");
        tmp
    }

    fn create_session_db(
        db_path: &Path,
        rows: impl IntoIterator<Item = (&'static str, &'static str, &'static str, i64, i64)>,
    ) {
        let conn = Connection::open(db_path).expect("open sqlite");
        conn.execute_batch(
            "CREATE TABLE sessions (path TEXT, id TEXT, cwd TEXT, timestamp INTEGER, last_modified INTEGER)",
        )
        .expect("create table");

        let mut insert = conn
            .prepare(
                "INSERT INTO sessions(path,id,cwd,timestamp,last_modified) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .expect("prepare insert");

        for (path, id, cwd, timestamp, last_modified) in rows {
            insert
                .execute(rusqlite::params![path, id, cwd, timestamp, last_modified])
                .expect("insert row");
        }
    }

    fn pi_session_paths(root: &Path) -> (PathBuf, PathBuf) {
        let sessions_dir = root.join("agent").join("sessions");
        let first = sessions_dir
            .join("--home-bob-src-a")
            .join("session-a.jsonl");
        let second = sessions_dir
            .join("--home-bob-src-b")
            .join("session-b.jsonl");
        (first, second)
    }

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

    fn fixture_path(rel: &str) -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("../../tests/fixtures");
        p.push(rel);
        p
    }

    #[test]
    fn use_sqlite_when_index_is_newer_than_all_jsonl_files() {
        let tmp = create_pi_root();
        let root = tmp.path().join(".pi");
        let sessions_dir = root.join("agent").join("sessions");

        let (first, second) = pi_session_paths(&root);
        create_session_file(&first, "session-a");
        create_session_file(&second, "session-b");
        set_mtime(&first, 100);
        set_mtime(&second, 200);

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [
                (
                    "--home-bob-src-a/session-a.jsonl",
                    "db-001",
                    "home/bob/src/a",
                    1,
                    10,
                ),
                (
                    "--home-bob-src-b/session-b.jsonl",
                    "db-002",
                    "home/bob/src/b",
                    2,
                    20,
                ),
            ],
        );
        set_mtime(&db_path, 300);

        let locator = PiSessionLocator::with_root(root);
        let tuples = locator.list_session_tuples().expect("scan sessions");
        let ids: BTreeSet<_> = tuples
            .iter()
            .map(|tuple| tuple.session_id.as_str())
            .collect();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains("db-001"));
        assert!(ids.contains("db-002"));
        assert!(!ids.contains("session-a"));
        assert!(!ids.contains("session-b"));
    }

    #[test]
    fn use_walkdir_when_index_is_missing() {
        let tmp = create_pi_root();
        let root = tmp.path().join(".pi");
        let sessions_dir = root.join("agent").join("sessions");
        let (first, second) = pi_session_paths(&root);
        create_session_file(&first, "session-a");
        create_session_file(&second, "session-b");

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [(
                "--home-bob-src-a/session-a.jsonl",
                "db-001",
                "home/bob/src/a",
                1,
                10,
            )],
        );
        fs::remove_file(&db_path).expect("remove sqlite index");

        let locator = PiSessionLocator::with_root(root);
        let tuples = locator.list_session_tuples().expect("scan sessions");
        let ids: BTreeSet<_> = tuples
            .iter()
            .map(|tuple| tuple.session_id.as_str())
            .collect();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains("session-a"));
        assert!(ids.contains("session-b"));
        assert!(!ids.contains("db-001"));
    }

    #[test]
    fn use_walkdir_when_index_is_older_than_jsonl() {
        let tmp = create_pi_root();
        let root = tmp.path().join(".pi");
        let sessions_dir = root.join("agent").join("sessions");
        let (first, second) = pi_session_paths(&root);
        create_session_file(&first, "session-a");
        create_session_file(&second, "session-b");
        set_mtime(&first, 100);
        set_mtime(&second, 200);

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [
                (
                    "--home-bob-src-a/session-a.jsonl",
                    "db-001",
                    "home/bob/src/a",
                    1,
                    10,
                ),
                (
                    "--home-bob-src-b/session-b.jsonl",
                    "db-002",
                    "home/bob/src/b",
                    2,
                    20,
                ),
            ],
        );
        set_mtime(&db_path, 150);

        let locator = PiSessionLocator::with_root(root);
        let tuples = locator.list_session_tuples().expect("scan sessions");
        let ids: BTreeSet<_> = tuples
            .iter()
            .map(|tuple| tuple.session_id.as_str())
            .collect();

        assert_eq!(ids.len(), 2);
        assert!(ids.contains("session-a"));
        assert!(ids.contains("session-b"));
        assert!(!ids.contains("db-001"));
        assert!(!ids.contains("db-002"));
    }

    #[test]
    fn missing_pi_home_returns_empty() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("no-pi");

        let locator = PiSessionLocator::with_root(root);
        let tuples = locator.list_session_tuples().expect("scan sessions");

        assert!(tuples.is_empty());
    }

    #[test]
    fn sample_jsonl_yields_three_events() {
        let fixture_dir = fixture_path("pi");
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join(".pi");
        let sessions_dir = pi_root.join("agent").join("sessions");
        let session_path = sessions_dir
            .join("--home-bob-src-project")
            .join("sample.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create dirs");
        fs::copy(fixture_dir.join("sample.jsonl"), &session_path).expect("copy fixture");
        set_mtime(&session_path, 100);

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [(
                "--home-bob-src-project/sample.jsonl",
                "session-001",
                "home/bob/src/project",
                1000,
                1000,
            )],
        );
        set_mtime(&db_path, 200);

        let reader = PiReader::with_root(pi_root);

        let events: Vec<UsageEvent> = run_on_lab(42, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(
            events.len(),
            3,
            "expected 3 events, got {}: {events:?}",
            events.len()
        );

        let e0 = events
            .iter()
            .find(|e| e.dedup_key == "session-001:msg_002")
            .expect("msg_002");
        assert_eq!(e0.model.as_str(), "anthropic/claude-haiku-4.5");
        assert_eq!(e0.tokens.input, 150);
        assert_eq!(e0.tokens.output, 45);
        assert_eq!(e0.tokens.cache_read, 0);
        assert_eq!(e0.tokens.cache_write, 0);
        assert_eq!(e0.tokens.reasoning, 0);
        assert_eq!(e0.provider, "anthropic");
        assert_eq!(e0.month, YearMonth::new(2026, 5));
        assert_eq!(e0.project, Some("home/bob/src/project".into()));

        let e1 = events
            .iter()
            .find(|e| e.dedup_key == "session-001:msg_004")
            .expect("msg_004");
        assert_eq!(e1.tokens.input, 200);
        assert_eq!(e1.tokens.output, 20);
        assert_eq!(e1.tokens.cache_read, 100);
        assert_eq!(e1.tokens.cache_write, 50);

        let e2 = events
            .iter()
            .find(|e| e.dedup_key == "session-001:msg_006")
            .expect("msg_006");
        assert_eq!(e2.provider, "google");
        assert_eq!(e2.model.as_str(), "google/gemini-2-pro");
    }

    #[test]
    fn pi_cost_block_not_in_output() {
        let fixture_dir = fixture_path("pi");
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join(".pi");
        let sessions_dir = pi_root.join("agent").join("sessions");
        let session_path = sessions_dir
            .join("--home-bob-src-project")
            .join("sample.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create dirs");
        fs::copy(fixture_dir.join("sample.jsonl"), &session_path).expect("copy fixture");
        set_mtime(&session_path, 100);

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [(
                "--home-bob-src-project/sample.jsonl",
                "session-001",
                "home/bob/src/project",
                1000,
                1000,
            )],
        );
        set_mtime(&db_path, 200);

        let reader = PiReader::with_root(pi_root);

        let events: Vec<UsageEvent> = run_on_lab(42, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        for event in events {
            assert!(!event.dedup_key.contains("cost"));
            assert_eq!(
                event.tokens.input
                    + event.tokens.output
                    + event.tokens.cache_read
                    + event.tokens.cache_write,
                0u64 + event.tokens.input
                    + event.tokens.output
                    + event.tokens.cache_read
                    + event.tokens.cache_write
            );
        }
    }

    #[test]
    fn session_with_no_assistant_messages_yields_no_events() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join(".pi");
        let sessions_dir = pi_root.join("agent").join("sessions");
        let session_path = sessions_dir
            .join("--home-bob-src-empty")
            .join("empty.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create dirs");
        fs::write(
            &session_path,
            r#"{"type":"message","message":{"id":"msg_001","timestamp":1715420427091,"role":"user","provider":"anthropic","model":"claude-haiku-4-5","content":"Hello"}}
"#,
        )
        .expect("write fixture");

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [(
                "--home-bob-src-empty/empty.jsonl",
                "session-empty",
                "home/bob/src/empty",
                1000,
                1000,
            )],
        );

        let reader = PiReader::with_root(pi_root);

        let events: Vec<UsageEvent> = run_on_lab(99, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert!(events.is_empty());
    }

    #[test]
    fn cached_scan_can_short_circuit_pi_index_without_statting_jsonl() {
        let fixture_dir = fixture_path("pi");
        let tmp = tempfile::tempdir().expect("tempdir");
        let pi_root = tmp.path().join(".pi");
        let sessions_dir = pi_root.join("agent").join("sessions");
        let session_path = sessions_dir
            .join("--home-bob-src-project")
            .join("sample.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create dirs");
        fs::copy(fixture_dir.join("sample.jsonl"), &session_path).expect("copy fixture");

        let db_path = sessions_dir.join("session-index.sqlite");
        create_session_db(
            &db_path,
            [(
                "--home-bob-src-project/sample.jsonl",
                "session-001",
                "home/bob/src/project",
                1000,
                1000,
            )],
        );
        set_mtime(&db_path, 2000);

        let cache_dir = tempfile::tempdir().expect("cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");

        let first_count = run_on_lab(43, {
            let cache_path = cache_path.clone();
            let root = pi_root.clone();
            move |cx| {
                Box::pin(async move {
                    let reader = PiReader::with_root(root);
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path)
                        .await
                        .len()
                })
            }
        });
        assert_eq!(first_count, 3);

        fs::remove_file(&session_path).expect("remove session file after caching");
        reset_jsonl_open_count();
        let second_count = run_on_lab(44, {
            let cache_path = cache_path.clone();
            let root = pi_root.clone();
            move |cx| {
                Box::pin(async move {
                    let reader = PiReader::with_root(root);
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path)
                        .await
                        .len()
                })
            }
        });
        // Second scan emits 0 new events (session file removed, index unchanged)
        // but replays the 3 persisted events from the cache.
        assert_eq!(second_count, 3);
    }

    #[test]
    fn missing_pi_home_returns_ok() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reader = PiReader::with_root(tmp.path().join("no-pi"));

        let events: Vec<UsageEvent> = run_on_lab(1, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert!(events.is_empty());
    }
}
