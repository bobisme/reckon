use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{CacheStrategy, Reader, ReaderError, Sink, SinkError};

pub struct GeminiReader {
    root: PathBuf,
}

impl Default for GeminiReader {
    fn default() -> Self {
        Self::new()
    }
}

impl GeminiReader {
    #[must_use]
    pub fn new() -> Self {
        let root = env::var("GEMINI_HOME").map_or_else(
            |_| {
                let mut p = home_dir();
                p.push(".gemini");
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

    fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }
}

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

#[async_trait]
impl Reader for GeminiReader {
    fn source(&self) -> Source {
        Source::Gemini
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        let tmp_dir = self.tmp_dir();
        let project_dirs = match fs::read_dir(&tmp_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Outcome::ok(()),
            Err(e) => {
                return Outcome::Err(ReaderError::new(format!(
                    "reading {}: {e}",
                    tmp_dir.display()
                )));
            }
        };

        let project_map = load_project_map(&self.root).unwrap_or_default();

        for dir_entry in project_dirs {
            let Ok(dir_entry) = dir_entry else { continue };
            let dir_path = dir_entry.path();
            if !dir_path.is_dir() {
                continue;
            }

            let chats_dir = dir_path.join("chats");
            let chat_files = match find_session_files(&chats_dir) {
                Ok(files) => files,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Outcome::Err(ReaderError::new(format!(
                        "reading {}: {e}",
                        chats_dir.display()
                    )));
                }
            };

            let logs = load_logs(&dir_path.join("logs.json")).unwrap_or_default();

            for file_path in chat_files {
                match scan_session_file(&file_path, &project_map, &logs, cx, sink).await {
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
                            file_path.display()
                        )));
                    }
                }
            }
        }

        Outcome::ok(())
    }

    fn cache_strategy(&self) -> CacheStrategy {
        CacheStrategy::NeverCache
    }
}

fn find_session_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if path.extension().and_then(|ext| ext.to_str()) == Some("json")
            && name.starts_with("session-")
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[derive(Default)]
struct ProjectMap {
    hash_to_path: HashMap<String, String>,
}

fn load_project_map(root: &Path) -> io::Result<ProjectMap> {
    let path = root.join("projects.json");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(ProjectMap::default()),
        Err(e) => return Err(e),
    };

    let index: ProjectsIndex = serde_json::from_str(&contents).map_err(io::Error::other)?;
    let mut map = HashMap::new();
    for cwd in index.projects.into_keys() {
        map.insert(sha256_hex(&cwd), cwd);
    }
    Ok(ProjectMap { hash_to_path: map })
}

fn sha256_hex(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}")
}

fn display_project(project_hash: Option<&str>, project_map: &ProjectMap) -> Option<String> {
    let hash = project_hash?;
    if let Some(path) = project_map.hash_to_path.get(hash) {
        return Some(path.clone());
    }
    Some(hash.chars().take(8).collect())
}

fn load_logs(path: &Path) -> io::Result<HashMap<(String, usize), i64>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    };

    let entries: Vec<LogEntry> = serde_json::from_str(&contents).map_err(io::Error::other)?;
    let mut map = HashMap::new();
    for entry in entries {
        let Some(ts) = entry.timestamp.as_deref().and_then(parse_iso8601_to_epoch) else {
            continue;
        };
        map.insert((entry.session_id, entry.message_id), ts);
    }
    Ok(map)
}

enum ScanError {
    Sink(SinkError),
    Io(io::Error),
}

async fn scan_session_file(
    path: &Path,
    project_map: &ProjectMap,
    logs: &HashMap<(String, usize), i64>,
    cx: &Cx,
    sink: &Sink,
) -> Result<(), ScanError> {
    let contents = fs::read_to_string(path).map_err(ScanError::Io)?;
    let session: SessionFile = serde_json::from_str(&contents)
        .map_err(io::Error::other)
        .map_err(ScanError::Io)?;

    let fallback_ts = session
        .start_time
        .as_deref()
        .and_then(parse_iso8601_to_epoch)
        .or_else(|| session_timestamp_from_filename(path));
    let project = display_project(session.project_hash.as_deref(), project_map);
    let session_id = session.session_id.clone();
    let session_model = session.model.clone();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            ScanError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid session filename",
            ))
        })?;

    for (index, message) in session.messages.into_iter().enumerate() {
        let Some(tokens) = token_counts_from_message(&message) else {
            continue;
        };

        let raw_model = message.model.as_deref().or(session_model.as_deref());
        let Some(raw_model) = raw_model else { continue };

        let timestamp_secs = message
            .timestamp
            .as_deref()
            .and_then(parse_iso8601_to_epoch)
            .or_else(|| {
                session_id
                    .as_ref()
                    .and_then(|id| logs.get(&(id.clone(), index)).copied())
            })
            .or_else(|| {
                fallback_ts.map(|ts| ts + i64::try_from(index).expect("message index overflow"))
            })
            .unwrap_or(0);

        let event = UsageEvent {
            source: Source::Gemini,
            month: YearMonth::from_utc(timestamp_secs),
            model: model_map::canonical(Source::Gemini, raw_model, Some("google")),
            provider: "google".into(),
            project: project.clone(),
            tokens,
            dedup_key: format!("{file_name}:{index}"),
            known_cost_usd: None,
            byok_usage_inference: None,
        };

        sink.send(cx, event).await.map_err(ScanError::Sink)?;
    }

    let metadata = fs::metadata(path).map_err(ScanError::Io)?;
    let mtime_ns =
        file_modified_nanos(metadata.modified().map_err(ScanError::Io)?).map_err(ScanError::Io)?;
    let size_bytes = i64::try_from(metadata.len()).expect("file size too large");
    sink.record_source_file(Source::Gemini, path, mtime_ns, size_bytes, size_bytes);

    Ok(())
}

fn token_counts_from_message(message: &SessionMessage) -> Option<TokenCounts> {
    if let Some(usage) = message.usage_metadata.as_ref() {
        return Some(TokenCounts {
            input: usage.prompt_token_count.unwrap_or(0),
            output: usage.candidates_token_count.unwrap_or(0),
            cache_read: usage.cached_content_token_count.unwrap_or(0),
            cache_write: 0,
            reasoning: usage.thoughts_token_count.unwrap_or(0),
        });
    }

    message.tokens.as_ref().map(|tokens| TokenCounts {
        input: tokens.input.unwrap_or(0),
        output: tokens.output.unwrap_or(0),
        cache_read: tokens.cached.unwrap_or(0),
        cache_write: 0,
        reasoning: tokens.thoughts.unwrap_or(0),
    })
}

fn file_modified_nanos(modified: SystemTime) -> Result<i64, io::Error> {
    modified
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "mtime too large for i64"))
}

fn session_timestamp_from_filename(path: &Path) -> Option<i64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("session-")?.strip_suffix(".json")?;
    let year: i32 = rest.get(0..4)?.parse().ok()?;
    let month: u32 = rest.get(5..7)?.parse().ok()?;
    let day: u32 = rest.get(8..10)?.parse().ok()?;
    let hour: u32 = rest.get(11..13)?.parse().ok()?;
    let min: u32 = rest.get(14..16)?.parse().ok()?;
    let sec = rest.get(17..19).and_then(|s| s.parse().ok()).unwrap_or(0);
    Some(civil_to_epoch(year, month, day, hour, min, sec))
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

#[derive(Deserialize)]
struct ProjectsIndex {
    projects: HashMap<String, String>,
}

#[derive(Deserialize)]
struct LogEntry {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "messageId")]
    message_id: usize,
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct SessionFile {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    #[serde(rename = "projectHash")]
    project_hash: Option<String>,
    #[serde(rename = "startTime")]
    start_time: Option<String>,
    model: Option<String>,
    #[serde(default)]
    messages: Vec<SessionMessage>,
}

#[derive(Deserialize)]
struct SessionMessage {
    timestamp: Option<String>,
    model: Option<String>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<UsageMetadata>,
    tokens: Option<MessageTokens>,
}

#[derive(Deserialize)]
#[expect(clippy::struct_field_names)]
struct UsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u64>,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u64>,
    #[serde(rename = "cachedContentTokenCount")]
    cached_content_token_count: Option<u64>,
    #[serde(rename = "thoughtsTokenCount")]
    thoughts_token_count: Option<u64>,
}

#[derive(Deserialize)]
struct MessageTokens {
    input: Option<u64>,
    output: Option<u64>,
    cached: Option<u64>,
    thoughts: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use asupersync::lab::{LabConfig, LabRuntime};
    use asupersync::{Budget, Cx};
    use futures::future::BoxFuture;
    use reckon_core::{UsageEvent, YearMonth};

    use crate::run_readers;

    use super::*;

    fn fixture_path(rel: &str) -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("../../tests/fixtures");
        p.push(rel);
        p
    }

    fn run_on_lab<T, F>(seed: u64, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(Cx) -> BoxFuture<'static, T> + Send + 'static,
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
    fn fixture_session_yields_known_events() {
        let fixture_dir = fixture_path("gemini");
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join(".gemini");
        let hash = "17421c9b4a7a4ae5ba7b7ed96deb56b18dda07f3688e2991e5bdbd804aeaab62";
        let chat_dir = root.join("tmp").join(hash).join("chats");
        fs::create_dir_all(&chat_dir).expect("create chats dir");
        fs::copy(
            fixture_dir.join("session-sample.json"),
            chat_dir.join("session-2026-02-19T22-09-ae63ca40.json"),
        )
        .expect("copy fixture");

        fs::write(
            root.join("projects.json"),
            r#"{"projects":{"/home/bob/src/manifold":"manifold"}}"#,
        )
        .expect("write projects");
        fs::write(
            root.join("tmp").join(hash).join("logs.json"),
            r#"[
  {"sessionId":"ae63ca40-a628-4d39-bdf0-cd5e3dbf6151","messageId":2,"timestamp":"2026-02-19T22:10:05.000Z"}
]"#,
        )
        .expect("write logs");

        let reader = GeminiReader::with_root(root);
        let mut events: Vec<UsageEvent> = run_on_lab(7, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });
        events.sort_by(|a, b| a.dedup_key.cmp(&b.dedup_key));

        assert_eq!(events.len(), 2, "expected 2 events, got {events:?}");

        let first = &events[0];
        assert_eq!(first.dedup_key, "session-2026-02-19T22-09-ae63ca40.json:1");
        assert_eq!(first.model.as_str(), "google/gemini-2.5-pro");
        assert_eq!(first.provider, "google");
        assert_eq!(first.project, Some("/home/bob/src/manifold".into()));
        assert_eq!(first.tokens.input, 100);
        assert_eq!(first.tokens.output, 25);
        assert_eq!(first.tokens.cache_read, 10);
        assert_eq!(first.tokens.reasoning, 5);
        assert_eq!(first.month, YearMonth::new(2026, 2));

        let second = &events[1];
        assert_eq!(second.dedup_key, "session-2026-02-19T22-09-ae63ca40.json:2");
        assert_eq!(second.model.as_str(), "google/gemini-2.5-flash");
        assert_eq!(second.tokens.input, 8);
        assert_eq!(second.tokens.output, 3);
        assert_eq!(second.tokens.cache_read, 1);
        assert_eq!(second.tokens.reasoning, 2);
        assert_eq!(second.month, YearMonth::new(2026, 2));
    }

    #[test]
    fn unlisted_hash_uses_first_eight_chars() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join(".gemini");
        let hash = "986c575880ce3430000a33cf53e809c08df1360be9c51cfd808a948abbc2188a";
        let chat_dir = root.join("tmp").join(hash).join("chats");
        fs::create_dir_all(&chat_dir).expect("create chats dir");
        fs::write(
            chat_dir.join("session-2026-05-11T09-30-abcdef12.json"),
            format!(
                r#"{{
  "sessionId": "s-1",
  "projectHash": "{hash}",
  "messages": [
    {{
      "timestamp": "2026-05-11T09:30:00.000Z",
      "model": "gemini-2.5-pro",
      "usageMetadata": {{"promptTokenCount": 1, "candidatesTokenCount": 2}}
    }}
  ]
}}"#
            ),
        )
        .expect("write session");

        let reader = GeminiReader::with_root(root);
        let events: Vec<UsageEvent> = run_on_lab(8, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].project, Some("986c5758".into()));
    }

    #[test]
    fn missing_usage_metadata_is_silently_skipped() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join(".gemini");
        let hash = "ffb0a9ec888277c655a4c7c4d60a3e596540c51f45849040ae07e62aedeba518";
        let chat_dir = root.join("tmp").join(hash).join("chats");
        fs::create_dir_all(&chat_dir).expect("create chats dir");
        fs::write(
            chat_dir.join("session-2026-05-11T09-30-abcdef12.json"),
            format!(
                r#"{{
  "sessionId": "s-2",
  "projectHash": "{hash}",
  "messages": [
    {{"timestamp": "2026-05-11T09:30:00.000Z", "model": "gemini-2.5-pro"}},
    {{"timestamp": "2026-05-11T09:30:01.000Z", "model": "gemini-2.5-pro", "usageMetadata": {{"promptTokenCount": 3}}}}
  ]
}}"#
            ),
        )
        .expect("write session");

        let reader = GeminiReader::with_root(root);
        let events: Vec<UsageEvent> = run_on_lab(9, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.input, 3);
    }

    #[test]
    fn real_world_tokens_shape_is_accepted() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join(".gemini");
        let hash = "ffb0a9ec888277c655a4c7c4d60a3e596540c51f45849040ae07e62aedeba518";
        let chat_dir = root.join("tmp").join(hash).join("chats");
        fs::create_dir_all(&chat_dir).expect("create chats dir");
        fs::write(
            chat_dir.join("session-2026-03-24T23-14-3cadf9fd.json"),
            format!(
                r#"{{
  "sessionId": "s-3",
  "projectHash": "{hash}",
  "messages": [
    {{
      "timestamp": "2026-03-24T23:14:32.464Z",
      "model": "gemini-2.5-pro",
      "tokens": {{"input": 10550, "output": 170, "cached": 0, "thoughts": 368}}
    }}
  ]
}}"#
            ),
        )
        .expect("write session");

        let reader = GeminiReader::with_root(root);
        let events: Vec<UsageEvent> = run_on_lab(10, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tokens.input, 10_550);
        assert_eq!(events[0].tokens.reasoning, 368);
    }
}
