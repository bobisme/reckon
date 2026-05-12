use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    CacheStrategy, JsonlScanPlan, Reader, ReaderError, Sink, SinkError, plan_jsonl_scan,
    read_jsonl_from_offset, read_jsonl_prefix,
};

/// Represents a Codex session file with its path, session UUID, and date.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionFile {
    /// Full path to the session file.
    pub path: PathBuf,
    /// Session UUID extracted from filename.
    pub session_uuid: String,
    /// Date extracted from directory structure (YYYY, MM, DD).
    pub date: (u32, u32, u32),
}

/// Enumerates Codex session files from the date-partitioned directory tree.
///
/// # Errors
///
/// Returns an error if the root directory cannot be read.
pub fn enumerate_sessions(root: &Path) -> io::Result<Vec<SessionFile>> {
    let sessions_dir = root.join("sessions");

    // Return empty list if sessions dir doesn't exist (graceful).
    if !sessions_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();

    // Walk YYYY directories
    for year_entry in fs::read_dir(&sessions_dir)? {
        let year_entry = year_entry?;
        let year_path = year_entry.path();
        if !year_path.is_dir() {
            continue;
        }

        let year_str = year_entry.file_name().to_string_lossy().to_string();
        let Ok(year) = year_str.parse::<u32>() else {
            continue;
        };

        // Walk MM directories
        for month_entry in fs::read_dir(&year_path)? {
            let month_entry = month_entry?;
            let month_path = month_entry.path();
            if !month_path.is_dir() {
                continue;
            }

            let month_str = month_entry.file_name().to_string_lossy().to_string();
            let Ok(month) = month_str.parse::<u32>() else {
                continue;
            };

            // Walk DD directories
            for day_entry in fs::read_dir(&month_path)? {
                let day_entry = day_entry?;
                let day_path = day_entry.path();
                if !day_path.is_dir() {
                    continue;
                }

                let day_str = day_entry.file_name().to_string_lossy().to_string();
                let Ok(day) = day_str.parse::<u32>() else {
                    continue;
                };

                // Scan for rollout-*.jsonl files
                for file_entry in fs::read_dir(&day_path)? {
                    let file_entry = file_entry?;
                    let file_path = file_entry.path();
                    if !file_path.is_file() {
                        continue;
                    }

                    let file_name = file_entry.file_name().to_string_lossy().to_string();

                    // Only match rollout-*.jsonl files
                    if !file_name.starts_with("rollout-") {
                        continue;
                    }
                    let has_jsonl_ext = Path::new(&file_name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case(OsStr::new("jsonl")));
                    if !has_jsonl_ext {
                        continue;
                    }

                    // Extract UUID from filename: rollout-YYYY-MM-DDTHH-MM-SS-<UUID>.jsonl
                    if let Some(uuid) = extract_uuid(&file_name) {
                        files.push(SessionFile {
                            path: file_path,
                            session_uuid: uuid,
                            date: (year, month, day),
                        });
                    }
                }
            }
        }
    }

    // Sort chronologically by (year, month, day)
    files.sort_by_key(|f| f.date);

    Ok(files)
}

/// Extracts the session UUID from a Codex filename.
///
/// Expected format: `rollout-YYYY-MM-DDTHH-MM-SS-<UUID>.jsonl`
/// The UUID is everything after the timestamp and the final dash, before `.jsonl`.
fn extract_uuid(filename: &str) -> Option<String> {
    // Match: rollout-YYYY-MM-DDTHH-MM-SS-<UUID>.jsonl
    // The UUID portion is after the final dash, before .jsonl
    let name_without_ext = filename.strip_suffix(".jsonl")?;

    // Look for the pattern: rollout-YYYY-MM-DDTHH-MM-SS-<UUID>
    // We need to find where the UUID starts (after the ISO timestamp and final dash)
    let re = Regex::new(r"^rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-(.+)$").ok()?;
    let caps = re.captures(name_without_ext)?;
    caps.get(1).map(|m| m.as_str().to_string())
}

pub struct CodexReader {
    root: PathBuf,
}

impl Default for CodexReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexReader {
    #[must_use]
    pub fn new() -> Self {
        let root = env::var("CODEX_HOME").map_or_else(
            |_| {
                let mut p = home_dir();
                p.push(".codex");
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

    /// Enumerates all Codex session files in chronological order.
    ///
    /// # Errors
    ///
    /// Returns an error if the sessions directory cannot be read.
    pub fn enumerate(&self) -> io::Result<Vec<SessionFile>> {
        enumerate_sessions(&self.root)
    }
}

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CodexSessionState {
    previous_totals: TokenCounts,
    active_model: Option<String>,
    session_id: Option<String>,
    provider: Option<String>,
    project: Option<String>,
    turn_index: u32,
}

impl CodexSessionState {
    fn on_session_meta(&mut self, payload: SessionMetaPayload) {
        self.previous_totals = TokenCounts::default();
        self.active_model = None;
        self.session_id = payload.id;
        self.provider = payload.model_provider;
        self.project = payload.cwd;
        self.turn_index = 0;
    }

    fn on_turn_context(&mut self, payload: TurnContextPayload) {
        if let Some(model) = payload.model {
            self.active_model = Some(model);
        }
        if let Some(cwd) = payload.cwd {
            self.project = Some(cwd);
        }
    }

    fn on_token_count(&mut self, timestamp_secs: i64, current: TokenCounts) -> Option<UsageEvent> {
        let delta = delta_counts(self.previous_totals, current);
        self.previous_totals = current;

        let turn_index = self.turn_index;
        self.turn_index = self.turn_index.saturating_add(1);

        if delta.total() == 0 {
            return None;
        }

        let raw_model = self.active_model.as_deref()?;
        let session_id = self.session_id.as_deref()?;

        Some(UsageEvent {
            source: Source::Codex,
            month: YearMonth::from_utc(timestamp_secs),
            timestamp_secs,
            model: model_map::canonical(Source::Codex, raw_model, None),
            provider: self.provider.clone().unwrap_or_else(|| "openai".into()),
            project: self.project.clone(),
            tokens: delta,
            dedup_key: format!("{session_id}:{turn_index}"),
            known_cost_usd: None,
            byok_usage_inference: None,
        })
    }
}

#[must_use]
const fn delta_counts(previous: TokenCounts, current: TokenCounts) -> TokenCounts {
    TokenCounts {
        input: current.input.saturating_sub(previous.input),
        output: current.output.saturating_sub(previous.output),
        cache_read: current.cache_read.saturating_sub(previous.cache_read),
        cache_write: current.cache_write.saturating_sub(previous.cache_write),
        reasoning: current.reasoning.saturating_sub(previous.reasoning),
    }
}

#[derive(Deserialize)]
struct JsonlEntry {
    timestamp: Option<String>,
    #[serde(rename = "type")]
    entry_type: String,
    payload: Value,
}

#[derive(Deserialize)]
struct SessionMetaPayload {
    id: Option<String>,
    cwd: Option<String>,
    model_provider: Option<String>,
}

#[derive(Deserialize)]
struct TurnContextPayload {
    model: Option<String>,
    cwd: Option<String>,
}

#[derive(Deserialize)]
struct EventPayload {
    #[serde(rename = "type")]
    payload_type: String,
    info: Option<TokenCountInfo>,
}

#[derive(Deserialize)]
struct TokenCountInfo {
    total_token_usage: Option<TotalTokenUsage>,
}

#[derive(Deserialize)]
#[expect(clippy::struct_field_names)]
struct TotalTokenUsage {
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    reasoning_output_tokens: Option<u64>,
}

fn token_counts_from_info(info: Option<TokenCountInfo>) -> Option<TokenCounts> {
    let total = info?.total_token_usage?;
    // OpenAI's `total_token_usage.input_tokens` is the **inclusive** count
    // (uncached + cached). `cached_input_tokens` is the subset that hit prompt
    // cache. Subtract so `tokens.input` is the non-cached prompt size only;
    // otherwise cache hits would be double-counted across `input` and
    // `cache_read`. `saturating_sub` guards against any unexpected ordering
    // where cached exceeds total (shouldn't happen, but we don't want to
    // underflow if it does).
    let input = total
        .input_tokens
        .unwrap_or(0)
        .saturating_sub(total.cached_input_tokens.unwrap_or(0));
    Some(TokenCounts {
        input,
        output: total.output_tokens.unwrap_or(0),
        cache_read: total.cached_input_tokens.unwrap_or(0),
        cache_write: 0,
        reasoning: total.reasoning_output_tokens.unwrap_or(0),
    })
}

enum ScanError {
    Sink(SinkError),
    Io(io::Error),
}

async fn scan_rollout_file(path: &Path, cx: &Cx, sink: &Sink) -> Result<(), ScanError> {
    let metadata = fs::metadata(path).map_err(ScanError::Io)?;
    let mtime_ns =
        file_modified_nanos(metadata.modified().map_err(ScanError::Io)?).map_err(ScanError::Io)?;
    let size_bytes = i64::try_from(metadata.len()).expect("file size too large");

    let start_offset = match plan_jsonl_scan(sink, Source::Codex, path, mtime_ns, size_bytes) {
        JsonlScanPlan::Skip { .. } => {
            sink.record_source_file(Source::Codex, path, mtime_ns, size_bytes, size_bytes);
            return Ok(());
        }
        JsonlScanPlan::ReadFrom { start_offset, .. } => start_offset,
    };

    let mut state = CodexSessionState::default();
    if start_offset > 0 {
        let prefix = read_jsonl_prefix(path, start_offset).map_err(ScanError::Io)?;
        apply_rollout_contents(&prefix, &mut state, None, cx).await?;
    }

    let contents = read_jsonl_from_offset(path, start_offset).map_err(ScanError::Io)?;
    apply_rollout_contents(&contents, &mut state, Some(sink), cx).await?;

    sink.record_source_file(Source::Codex, path, mtime_ns, size_bytes, size_bytes);

    Ok(())
}

async fn apply_rollout_contents(
    contents: &str,
    state: &mut CodexSessionState,
    emit_to: Option<&Sink>,
    cx: &Cx,
) -> Result<(), ScanError> {
    for line in contents.lines() {
        if line.is_empty() {
            continue;
        }

        let entry: JsonlEntry = match serde_json::from_str(line) {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        match entry.entry_type.as_str() {
            "session_meta" => {
                let Ok(payload) = serde_json::from_value::<SessionMetaPayload>(entry.payload)
                else {
                    continue;
                };
                state.on_session_meta(payload);
            }
            "turn_context" => {
                let Ok(payload) = serde_json::from_value::<TurnContextPayload>(entry.payload)
                else {
                    continue;
                };
                state.on_turn_context(payload);
            }
            "event_msg" => {
                let Ok(payload) = serde_json::from_value::<EventPayload>(entry.payload) else {
                    continue;
                };
                if payload.payload_type != "token_count" {
                    continue;
                }
                let Some(current) = token_counts_from_info(payload.info) else {
                    continue;
                };
                let Some(timestamp) = entry.timestamp.as_deref() else {
                    continue;
                };
                let Some(timestamp_secs) = parse_iso8601_to_epoch(timestamp) else {
                    continue;
                };
                if let Some(event) = state.on_token_count(timestamp_secs, current)
                    && let Some(emit_to) = emit_to
                {
                    emit_to.send(cx, event).await.map_err(ScanError::Sink)?;
                }
            }
            "token_count" => {
                let Ok(payload) = serde_json::from_value::<EventPayload>(entry.payload) else {
                    continue;
                };
                let Some(current) = token_counts_from_info(payload.info) else {
                    continue;
                };
                let Some(timestamp) = entry.timestamp.as_deref() else {
                    continue;
                };
                let Some(timestamp_secs) = parse_iso8601_to_epoch(timestamp) else {
                    continue;
                };
                if let Some(event) = state.on_token_count(timestamp_secs, current)
                    && let Some(emit_to) = emit_to
                {
                    emit_to.send(cx, event).await.map_err(ScanError::Sink)?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn file_modified_nanos(modified: SystemTime) -> Result<i64, io::Error> {
    modified
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "mtime too large for i64"))
}

fn parse_iso8601_to_epoch(s: &str) -> Option<i64> {
    if s.len() < 19 {
        return None;
    }
    let year: i32 = s.get(0..4)?.parse().ok()?;
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    let hour: u32 = s.get(11..13)?.parse().ok()?;
    let min: u32 = s.get(14..16)?.parse().ok()?;
    let sec: u32 = s.get(17..19)?.parse().ok()?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    Some(civil_to_epoch(year, month, day, hour, min, sec))
}

#[expect(clippy::cast_sign_loss, clippy::cast_possible_wrap)]
fn civil_to_epoch(y: i32, m: u32, d: u32, h: u32, min: u32, sec: u32) -> i64 {
    let (y, m) = if m <= 2 {
        (i64::from(y) - 1, m + 9)
    } else {
        (i64::from(y), m - 3)
    };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * u64::from(m) + 2) / 5 + u64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    days * 86_400 + i64::from(h) * 3_600 + i64::from(min) * 60 + i64::from(sec)
}

#[async_trait]
impl Reader for CodexReader {
    fn source(&self) -> Source {
        Source::Codex
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        let sessions = match self.enumerate() {
            Ok(sessions) => sessions,
            Err(e) => {
                return Outcome::Err(ReaderError::new(format!(
                    "reading {}: {e}",
                    self.root.join("sessions").display()
                )));
            }
        };

        for session in sessions {
            match scan_rollout_file(&session.path, cx, sink).await {
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
                        session.path.display()
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use asupersync::Budget;
    use asupersync::lab::{LabConfig, LabRuntime};
    use proptest::prelude::*;

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

    #[test]
    fn extract_uuid_basic() {
        let filename = "rollout-2026-05-10T14-30-45-abc123def456.jsonl";
        let uuid = extract_uuid(filename);
        assert_eq!(uuid, Some("abc123def456".to_string()));
    }

    #[test]
    fn extract_uuid_with_dashes_in_uuid() {
        let filename = "rollout-2026-05-10T14-30-45-abc-123-def.jsonl";
        let uuid = extract_uuid(filename);
        assert_eq!(uuid, Some("abc-123-def".to_string()));
    }

    #[test]
    fn extract_uuid_no_jsonl_extension() {
        let filename = "rollout-2026-05-10T14-30-45-abc123def456.txt";
        let uuid = extract_uuid(filename);
        assert_eq!(uuid, None);
    }

    #[test]
    fn extract_uuid_wrong_prefix() {
        let filename = "session-2026-05-10T14-30-45-abc123def456.jsonl";
        let uuid = extract_uuid(filename);
        assert_eq!(uuid, None);
    }

    #[test]
    fn enumerate_empty_root() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reader = CodexReader::with_root(tmp.path().to_path_buf());
        let sessions = reader.enumerate().expect("enumerate");
        assert!(sessions.is_empty());
    }

    #[test]
    fn enumerate_nonexistent_root() {
        let root = PathBuf::from("/tmp/nonexistent_codex_root_12345");
        let reader = CodexReader::with_root(root);
        let sessions = reader.enumerate().expect("enumerate");
        assert!(sessions.is_empty());
    }

    #[test]
    fn enumerate_partial_tree() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_dir = tmp.path().join("sessions");

        // Create only 2026/05 (no 06, no day subdirs initially)
        fs::create_dir_all(sessions_dir.join("2026/05/10")).expect("create 2026/05/10");
        fs::create_dir_all(sessions_dir.join("2026/05/11")).expect("create 2026/05/11");

        // Add rollout files to 2026/05/10
        fs::write(
            sessions_dir.join("2026/05/10/rollout-2026-05-10T10-00-00-session1.jsonl"),
            "dummy",
        )
        .expect("write");
        fs::write(
            sessions_dir.join("2026/05/10/rollout-2026-05-10T11-00-00-session2.jsonl"),
            "dummy",
        )
        .expect("write");

        // Add rollout file to 2026/05/11
        fs::write(
            sessions_dir.join("2026/05/11/rollout-2026-05-11T09-00-00-session3.jsonl"),
            "dummy",
        )
        .expect("write");

        let reader = CodexReader::with_root(tmp.path().to_path_buf());
        let sessions = reader.enumerate().expect("enumerate");

        assert_eq!(sessions.len(), 3);
        // Verify chronological order
        assert_eq!(sessions[0].date, (2026, 5, 10));
        assert_eq!(sessions[1].date, (2026, 5, 10));
        assert_eq!(sessions[2].date, (2026, 5, 11));
    }

    #[test]
    fn enumerate_skip_non_jsonl_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_dir = tmp.path().join("sessions");
        fs::create_dir_all(sessions_dir.join("2026/05/10")).expect("create");

        // Add rollout JSONL file
        fs::write(
            sessions_dir.join("2026/05/10/rollout-2026-05-10T10-00-00-session1.jsonl"),
            "dummy",
        )
        .expect("write");

        // Add non-JSONL files
        fs::write(sessions_dir.join("2026/05/10/logs_2.sqlite"), "dummy").expect("write");
        fs::write(sessions_dir.join("2026/05/10/other-file.txt"), "dummy").expect("write");
        fs::write(
            sessions_dir.join("2026/05/10/session-2026-05-10T10-00-00-session2.jsonl"),
            "dummy",
        )
        .expect("write"); // Wrong prefix, should skip

        let reader = CodexReader::with_root(tmp.path().to_path_buf());
        let sessions = reader.enumerate().expect("enumerate");

        // Should only find the rollout-*.jsonl file
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].session_uuid, "session1");
    }

    #[test]
    fn enumerate_chronological_order_across_days() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let sessions_dir = tmp.path().join("sessions");

        // Create files in reverse order to verify sorting
        let files = vec![
            (
                "2026/05/15/rollout-2026-05-15T10-00-00-c.jsonl",
                (2026, 5, 15),
            ),
            (
                "2026/05/10/rollout-2026-05-10T10-00-00-a.jsonl",
                (2026, 5, 10),
            ),
            (
                "2026/05/12/rollout-2026-05-12T10-00-00-b.jsonl",
                (2026, 5, 12),
            ),
        ];

        for (path, _) in &files {
            let full_path = sessions_dir.join(path);
            fs::create_dir_all(full_path.parent().expect("parent")).expect("create");
            fs::write(&full_path, "dummy").expect("write");
        }

        let reader = CodexReader::with_root(tmp.path().to_path_buf());
        let sessions = reader.enumerate().expect("enumerate");

        assert_eq!(sessions.len(), 3);
        // Verify chronological order
        assert_eq!(sessions[0].date, (2026, 5, 10));
        assert_eq!(sessions[0].session_uuid, "a");
        assert_eq!(sessions[1].date, (2026, 5, 12));
        assert_eq!(sessions[1].session_uuid, "b");
        assert_eq!(sessions[2].date, (2026, 5, 15));
        assert_eq!(sessions[2].session_uuid, "c");
    }

    #[test]
    fn rollout_sample_yields_known_events() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fixture_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("tests")
            .join("fixtures")
            .join("codex");
        let session_path = tmp
            .path()
            .join("sessions")
            .join("2026")
            .join("05")
            .join("10")
            .join("rollout-2026-05-10T12-00-00-session-123.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create sessions dir");
        fs::copy(fixture_dir.join("rollout-sample.jsonl"), &session_path).expect("copy fixture");

        let events: Vec<UsageEvent> = run_on_lab(42, move |cx| {
            let root = tmp.path().to_path_buf();
            Box::pin(async move {
                let _tmp = tmp;
                run_readers(&cx, vec![Box::new(CodexReader::with_root(root))]).await
            })
        });

        // Fixture cumulative readings (input_tokens is inclusive of cached):
        //   reading 1: input=100, cached=10  -> uncached=90,  cache_read=10
        //   reading 2: input=150, cached=15  -> uncached=135, cache_read=15
        //   reading 3: input=190, cached=20  -> uncached=170, cache_read=20
        // Emitted deltas:
        //   event 1: input=90,  cache_read=10, output=20, reasoning=5
        //   event 2: input=45,  cache_read=5,  output=10, reasoning=1
        //   event 3: input=35,  cache_read=5,  output=20, reasoning=2
        assert_eq!(events.len(), 3);
        assert_eq!(
            events,
            vec![
                UsageEvent {
                    source: Source::Codex,
                    month: YearMonth::new(2026, 5),
                    timestamp_secs: 1_778_414_402,
                    model: model_map::canonical(Source::Codex, "gpt-5.2", None),
                    provider: "openai".into(),
                    project: Some("/home/bob/src/reckon".into()),
                    tokens: TokenCounts {
                        input: 90,
                        output: 20,
                        cache_read: 10,
                        cache_write: 0,
                        reasoning: 5,
                    },
                    dedup_key: "session-123:0".into(),
                    known_cost_usd: None,
                    byok_usage_inference: None,
                },
                UsageEvent {
                    source: Source::Codex,
                    month: YearMonth::new(2026, 5),
                    timestamp_secs: 1_778_414_404,
                    model: model_map::canonical(Source::Codex, "gpt-5.2", None),
                    provider: "openai".into(),
                    project: Some("/home/bob/src/reckon".into()),
                    tokens: TokenCounts {
                        input: 45,
                        output: 10,
                        cache_read: 5,
                        cache_write: 0,
                        reasoning: 1,
                    },
                    dedup_key: "session-123:1".into(),
                    known_cost_usd: None,
                    byok_usage_inference: None,
                },
                UsageEvent {
                    source: Source::Codex,
                    month: YearMonth::new(2026, 5),
                    timestamp_secs: 1_778_414_406,
                    model: model_map::canonical(Source::Codex, "gpt-5.2-codex", None),
                    provider: "openai".into(),
                    project: Some("/home/bob/src/reckon".into()),
                    tokens: TokenCounts {
                        input: 35,
                        output: 20,
                        cache_read: 5,
                        cache_write: 0,
                        reasoning: 2,
                    },
                    dedup_key: "session-123:2".into(),
                    known_cost_usd: None,
                    byok_usage_inference: None,
                },
            ]
        );
    }

    #[test]
    fn rollout_missing_token_count_yields_zero_events() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let session_path = tmp
            .path()
            .join("sessions")
            .join("2026")
            .join("05")
            .join("10")
            .join("rollout-2026-05-10T12-00-00-session-123.jsonl");
        fs::create_dir_all(session_path.parent().expect("parent")).expect("create sessions dir");
        fs::write(
            &session_path,
            concat!(
                "{\"timestamp\":\"2026-05-10T12:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"session-123\",\"cwd\":\"/home/bob/src/reckon\",\"model_provider\":\"openai\"}}\n",
                "{\"timestamp\":\"2026-05-10T12:00:01.000Z\",\"type\":\"turn_context\",\"payload\":{\"model\":\"gpt-5.2\",\"cwd\":\"/home/bob/src/reckon\"}}\n"
            ),
        )
        .expect("write fixture");

        let events: Vec<UsageEvent> = run_on_lab(7, move |cx| {
            let root = tmp.path().to_path_buf();
            Box::pin(async move {
                let _tmp = tmp;
                run_readers(&cx, vec![Box::new(CodexReader::with_root(root))]).await
            })
        });

        assert!(events.is_empty());
    }

    /// Build a `token_count` event payload via `TokenCountInfo` so we exercise
    /// the same path as the rollout parser without going through JSON.
    fn make_info(input_tokens: u64, cached_input_tokens: u64) -> Option<TokenCountInfo> {
        Some(TokenCountInfo {
            total_token_usage: Some(TotalTokenUsage {
                input_tokens: Some(input_tokens),
                cached_input_tokens: Some(cached_input_tokens),
                output_tokens: Some(0),
                reasoning_output_tokens: Some(0),
            }),
        })
    }

    #[test]
    fn token_counts_subtracts_cached_from_input() {
        // OpenAI reports input_tokens inclusive of cached. We must subtract.
        let counts = token_counts_from_info(make_info(1000, 800)).expect("counts");
        assert_eq!(counts.input, 200, "input must be non-cached only");
        assert_eq!(counts.cache_read, 800, "cache_read must equal cached_input_tokens");
    }

    #[test]
    fn token_counts_saturates_when_cached_exceeds_input() {
        // Pathological ordering — shouldn't happen, but we don't underflow.
        let counts = token_counts_from_info(make_info(50, 100)).expect("counts");
        assert_eq!(counts.input, 0);
        assert_eq!(counts.cache_read, 100);
    }

    #[test]
    fn cumulative_readings_emit_non_cached_delta() {
        // Two cumulative token_count payloads:
        //   reading 1: input=1000, cached=800  -> uncached=200, cache=800
        //   reading 2: input=1500, cached=1100 -> uncached=400, cache=1100
        // Expected emitted delta on reading 2:
        //   tokens.input      = 400 - 200 = 200
        //   tokens.cache_read = 1100 - 800 = 300
        let mut state = CodexSessionState::default();
        state.on_session_meta(SessionMetaPayload {
            id: Some("session-cache".into()),
            cwd: Some("/tmp/project".into()),
            model_provider: Some("openai".into()),
        });
        state.on_turn_context(TurnContextPayload {
            model: Some("gpt-5.2".into()),
            cwd: None,
        });

        let reading1 = token_counts_from_info(make_info(1000, 800)).expect("reading 1");
        assert_eq!(reading1.input, 200);
        assert_eq!(reading1.cache_read, 800);
        let event1 = state
            .on_token_count(1_778_414_400, reading1)
            .expect("first event");
        assert_eq!(event1.tokens.input, 200, "first delta is the full reading");
        assert_eq!(event1.tokens.cache_read, 800);

        let reading2 = token_counts_from_info(make_info(1500, 1100)).expect("reading 2");
        assert_eq!(reading2.input, 400);
        assert_eq!(reading2.cache_read, 1100);
        let event2 = state
            .on_token_count(1_778_414_460, reading2)
            .expect("second event");
        assert_eq!(
            event2.tokens.input, 200,
            "delta of non-cached input across two readings (400 - 200)"
        );
        assert_eq!(
            event2.tokens.cache_read, 300,
            "delta of cached input across two readings (1100 - 800)"
        );
    }

    proptest! {
        #[test]
        fn cumulative_deltas_sum_to_final_totals(
            increments in prop::collection::vec(
                (0u16..5000, 0u16..5000, 0u16..1000, 0u16..1000),
                1..20,
            ),
            model_switches in prop::collection::vec(any::<bool>(), 1..20),
        ) {
            let mut state = CodexSessionState::default();
            state.on_session_meta(SessionMetaPayload {
                id: Some("session-prop".into()),
                cwd: Some("/tmp/project".into()),
                model_provider: Some("openai".into()),
            });

            let mut sum = TokenCounts::default();
            let mut final_totals = TokenCounts::default();

            for (index, (input, output, cache_read, reasoning)) in increments.iter().copied().enumerate() {
                let model = if model_switches[index % model_switches.len()] {
                    "gpt-5.2-codex"
                } else {
                    "gpt-5.2"
                };
                state.on_turn_context(TurnContextPayload {
                    model: Some(model.into()),
                    cwd: None,
                });

                final_totals.input += u64::from(input);
                final_totals.output += u64::from(output);
                final_totals.cache_read += u64::from(cache_read);
                final_totals.reasoning += u64::from(reasoning);

                let event = state
                    .on_token_count(1_778_414_400, final_totals)
                    .expect("strictly increasing totals should emit an event");
                sum += event.tokens;
            }

            prop_assert_eq!(sum, final_totals);
        }

        #[test]
        fn negative_deltas_clamp_to_zero(
            previous in (1u16..5000, 1u16..5000, 1u16..1000, 1u16..1000),
            smaller in (0u16..4999, 0u16..4999, 0u16..999, 0u16..999),
        ) {
            let mut state = CodexSessionState::default();
            state.on_session_meta(SessionMetaPayload {
                id: Some("session-prop".into()),
                cwd: Some("/tmp/project".into()),
                model_provider: Some("openai".into()),
            });
            state.on_turn_context(TurnContextPayload {
                model: Some("gpt-5.2".into()),
                cwd: None,
            });

            let previous_totals = TokenCounts {
                input: u64::from(previous.0),
                output: u64::from(previous.1),
                cache_read: u64::from(previous.2),
                cache_write: 0,
                reasoning: u64::from(previous.3),
            };
            let smaller_totals = TokenCounts {
                input: u64::from(smaller.0.min(previous.0.saturating_sub(1))),
                output: u64::from(smaller.1.min(previous.1.saturating_sub(1))),
                cache_read: u64::from(smaller.2.min(previous.2.saturating_sub(1))),
                cache_write: 0,
                reasoning: u64::from(smaller.3.min(previous.3.saturating_sub(1))),
            };

            let _ = state.on_token_count(1_778_414_400, previous_totals).expect("first event");
            let event = state.on_token_count(1_778_414_401, smaller_totals);

            prop_assert_eq!(event, None);
        }
    }
}
