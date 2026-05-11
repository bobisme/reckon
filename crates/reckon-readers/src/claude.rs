use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, io};

use asupersync::{Cx, Outcome};
use async_trait::async_trait;
use reckon_core::model_map;
use reckon_core::{Source, TokenCounts, UsageEvent, YearMonth};
use serde::Deserialize;

use crate::{
    CacheStrategy, JsonlScanPlan, Reader, ReaderError, Sink, SinkError, plan_jsonl_scan,
    read_jsonl_from_offset,
};

pub struct ClaudeReader {
    root: PathBuf,
}

impl Default for ClaudeReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ClaudeReader {
    #[must_use]
    pub fn new() -> Self {
        let root = env::var("CLAUDE_HOME").map_or_else(
            |_| {
                let mut p = home_dir();
                p.push(".claude");
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

    fn projects_dir(&self) -> PathBuf {
        self.root.join("projects")
    }
}

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

fn decode_project_name(dir_name: &str) -> String {
    dir_name.replace('-', "/")
}

#[async_trait]
impl Reader for ClaudeReader {
    fn source(&self) -> Source {
        Source::Claude
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        let projects_dir = self.projects_dir();
        let project_dirs = match fs::read_dir(&projects_dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Outcome::ok(()),
            Err(e) => {
                return Outcome::Err(ReaderError::new(format!(
                    "reading {}: {e}",
                    projects_dir.display()
                )));
            }
        };

        for dir_entry in project_dirs {
            let Ok(dir_entry) = dir_entry else { continue };
            let dir_path = dir_entry.path();
            if !dir_path.is_dir() {
                continue;
            }
            let project_name = dir_entry.file_name().to_string_lossy().into_owned();
            let project = decode_project_name(&project_name);

            let Ok(jsonl_files) = find_jsonl_files(&dir_path) else {
                continue;
            };

            for file_path in jsonl_files {
                match scan_jsonl_file(&file_path, &project, cx, sink).await {
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
        CacheStrategy::JsonlTail
    }
}

fn find_jsonl_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }
    Ok(files)
}

enum ScanError {
    Sink(SinkError),
    Io(io::Error),
}

async fn scan_jsonl_file(
    path: &Path,
    project: &str,
    cx: &Cx,
    sink: &Sink,
) -> Result<(), ScanError> {
    let metadata = fs::metadata(path).map_err(ScanError::Io)?;
    let mtime_ns =
        file_modified_nanos(metadata.modified().map_err(ScanError::Io)?).map_err(ScanError::Io)?;
    let size_bytes = i64::try_from(metadata.len()).expect("file size too large");

    let start_offset = match plan_jsonl_scan(sink, Source::Claude, path, mtime_ns, size_bytes) {
        JsonlScanPlan::Skip { .. } => {
            sink.record_source_file(Source::Claude, path, mtime_ns, size_bytes, size_bytes);
            return Ok(());
        }
        JsonlScanPlan::ReadFrom { start_offset, .. } => start_offset,
    };

    let contents = read_jsonl_from_offset(path, start_offset).map_err(ScanError::Io)?;

    for line in contents.lines() {
        if line.is_empty() {
            continue;
        }

        let entry: JsonlEntry = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(_) => continue,
        };

        if entry.r#type != "assistant" {
            continue;
        }

        let (Some(message), Some(request_id), Some(timestamp)) =
            (entry.message, entry.request_id, entry.timestamp)
        else {
            continue;
        };

        let Some(usage) = message.usage else {
            continue;
        };

        let Some(raw_model) = message.model else {
            continue;
        };

        let Some(ts_secs) = parse_iso8601_to_epoch(&timestamp) else {
            continue;
        };

        let model = model_map::canonical(Source::Claude, &raw_model, None);

        let event = UsageEvent {
            source: Source::Claude,
            month: YearMonth::from_utc(ts_secs),
            model,
            provider: "anthropic".into(),
            project: Some(project.into()),
            tokens: TokenCounts {
                input: usage.input_tokens.unwrap_or(0),
                output: usage.output_tokens.unwrap_or(0),
                cache_read: usage.cache_read_input_tokens.unwrap_or(0),
                cache_write: usage.cache_creation_input_tokens.unwrap_or(0),
                reasoning: 0,
            },
            dedup_key: request_id,
            known_cost_usd: None,
            byok_usage_inference: None,
        };

        sink.send(cx, event).await.map_err(ScanError::Sink)?;
    }

    sink.record_source_file(Source::Claude, path, mtime_ns, size_bytes, size_bytes);

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

#[derive(Deserialize)]
struct JsonlEntry {
    r#type: String,
    message: Option<Message>,
    #[serde(rename = "requestId")]
    request_id: Option<String>,
    timestamp: Option<String>,
}

#[derive(Deserialize)]
struct Message {
    model: Option<String>,
    usage: Option<Usage>,
}

#[derive(Deserialize)]
#[expect(clippy::struct_field_names)]
struct Usage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    cache_creation_input_tokens: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use asupersync::Budget;
    use asupersync::lab::{LabConfig, LabRuntime};
    use filetime::FileTime;
    use reckon_core::open_cache;

    use super::*;
    use crate::{jsonl_open_count, reset_jsonl_open_count, run_readers, run_readers_with_cache};
    use tempfile::TempDir;

    #[test]
    fn parse_iso8601_basic() {
        let ts = parse_iso8601_to_epoch("2026-05-11T18:30:27.091Z");
        assert!(ts.is_some());
        let ym = YearMonth::from_utc(ts.expect("parsed"));
        assert_eq!(ym, YearMonth::new(2026, 5));
    }

    #[test]
    fn parse_iso8601_no_millis() {
        let ts = parse_iso8601_to_epoch("2026-01-01T00:00:00Z");
        assert_eq!(ts, Some(1_767_225_600));
    }

    #[test]
    fn decode_project_name_basic() {
        assert_eq!(
            decode_project_name("-home-bob-src-reckon"),
            "/home/bob/src/reckon"
        );
    }

    #[test]
    fn civil_to_epoch_unix_epoch() {
        assert_eq!(civil_to_epoch(1970, 1, 1, 0, 0, 0), 0);
    }

    #[test]
    fn civil_to_epoch_known_date() {
        assert_eq!(civil_to_epoch(2026, 1, 1, 0, 0, 0), 1_767_225_600);
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
    fn sample_jsonl_yields_three_events() {
        let fixture_dir = fixture_path("claude");
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects_dir = tmp.path().join("projects").join("-test-project");
        fs::create_dir_all(&projects_dir).expect("create dirs");
        fs::copy(
            fixture_dir.join("sample.jsonl"),
            projects_dir.join("sample.jsonl"),
        )
        .expect("copy fixture");

        let reader = ClaudeReader::with_root(tmp.path().to_path_buf());

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
            .find(|e| e.dedup_key == "req_001")
            .expect("req_001");
        assert_eq!(e0.model.as_str(), "anthropic/claude-sonnet-4.6");
        assert_eq!(e0.tokens.input, 500);
        assert_eq!(e0.tokens.output, 200);
        assert_eq!(e0.tokens.cache_write, 1000);
        assert_eq!(e0.tokens.cache_read, 0);
        assert_eq!(e0.provider, "anthropic");
        assert_eq!(e0.month, YearMonth::new(2026, 5));

        let e1 = events
            .iter()
            .find(|e| e.dedup_key == "req_002")
            .expect("req_002");
        assert_eq!(e1.tokens.cache_read, 2000);

        let e2 = events
            .iter()
            .find(|e| e.dedup_key == "req_003")
            .expect("req_003");
        assert_eq!(e2.model.as_str(), "anthropic/claude-opus-4.7");
    }

    #[test]
    fn dedup_across_project_dirs() {
        let fixture_dir = fixture_path("claude-dedup");
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects_dir = tmp.path().join("projects");

        let alpha_src = fixture_dir.join("-project-alpha");
        let beta_src = fixture_dir.join("-project-beta");
        let alpha_dst = projects_dir.join("-project-alpha");
        let beta_dst = projects_dir.join("-project-beta");
        fs::create_dir_all(&alpha_dst).expect("create alpha");
        fs::create_dir_all(&beta_dst).expect("create beta");
        for entry in fs::read_dir(&alpha_src).expect("read alpha") {
            let entry = entry.expect("entry");
            fs::copy(entry.path(), alpha_dst.join(entry.file_name())).expect("copy");
        }
        for entry in fs::read_dir(&beta_src).expect("read beta") {
            let entry = entry.expect("entry");
            fs::copy(entry.path(), beta_dst.join(entry.file_name())).expect("copy");
        }

        let reader = ClaudeReader::with_root(tmp.path().to_path_buf());

        let events: Vec<UsageEvent> = run_on_lab(99, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        let dedup_keys: Vec<&str> = events.iter().map(|e| e.dedup_key.as_str()).collect();
        assert!(
            dedup_keys.contains(&"req_dup_001"),
            "should contain dup key"
        );
        assert!(
            dedup_keys.contains(&"req_unique_001"),
            "should contain unique key"
        );

        let dup_count = dedup_keys.iter().filter(|k| **k == "req_dup_001").count();
        assert_eq!(dup_count, 2, "reader emits both; Sink dedup collapses them");

        let unique: HashSet<&str> = dedup_keys.into_iter().collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn cached_scan_records_source_file_offsets() {
        let fixture_dir = fixture_path("claude");
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects_dir = tmp.path().join("projects").join("-test-project");
        fs::create_dir_all(&projects_dir).expect("create dirs");
        fs::copy(
            fixture_dir.join("sample.jsonl"),
            projects_dir.join("sample.jsonl"),
        )
        .expect("copy fixture");

        let cache_dir = TempDir::new().expect("temp cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");
        let cache_path_for_run = cache_path.clone();

        let rows = run_on_lab(73, {
            let cache_path = cache_path_for_run;
            let reader = ClaudeReader::with_root(tmp.path().to_path_buf());
            move |cx| {
                Box::pin(async move {
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path).await;

                    let conn = open_cache(&cache_path);
                    let count: i64 = conn
                        .query_row("SELECT COUNT(*) FROM source_files", [], |row| {
                            row.get::<_, i64>(0)
                        })
                        .expect("query source_files count");
                    count
                })
            }
        });

        assert_eq!(rows, 1);
    }

    fn write_claude_assistant_events(path: &Path, start: usize, count: usize) {
        let mut body = String::new();
        for index in start..start + count {
            body.push_str(&format!(
                "{{\"message\":{{\"model\":\"claude-sonnet-4-6-20250514\",\"usage\":{{\"input_tokens\":{},\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"id\":\"msg_{index:03}\"}},\"requestId\":\"req_{index:03}\",\"type\":\"assistant\",\"timestamp\":\"2026-05-11T10:00:{:02}.000Z\"}}\n",
                100 + index,
                index % 60,
            ));
        }
        fs::write(path, body).expect("write claude jsonl");
    }

    fn set_mtime(path: &Path, secs: i64) {
        let time = FileTime::from_unix_time(secs, 0);
        filetime::set_file_times(path, time, time).expect("set file mtime");
    }

    #[test]
    fn warm_scan_appends_only_new_jsonl_events_and_skips_unchanged_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let projects_dir = tmp.path().join("projects").join("-test-project");
        fs::create_dir_all(&projects_dir).expect("create dirs");
        let session_path = projects_dir.join("sample.jsonl");
        write_claude_assistant_events(&session_path, 0, 100);
        set_mtime(&session_path, 100);

        let cache_dir = TempDir::new().expect("temp cache dir");
        let cache_path = cache_dir.path().join("index.sqlite");

        let first_count = run_on_lab(74, {
            let cache_path = cache_path.clone();
            let root = tmp.path().to_path_buf();
            move |cx| {
                Box::pin(async move {
                    let reader = ClaudeReader::with_root(root);
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path)
                        .await
                        .len()
                })
            }
        });
        assert_eq!(first_count, 100);

        let mut appended = fs::read_to_string(&session_path).expect("read first scan file");
        for index in 100..120 {
            appended.push_str(&format!(
                "{{\"message\":{{\"model\":\"claude-sonnet-4-6-20250514\",\"usage\":{{\"input_tokens\":{},\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"output_tokens\":1}},\"id\":\"msg_{index:03}\"}},\"requestId\":\"req_{index:03}\",\"type\":\"assistant\",\"timestamp\":\"2026-05-11T10:01:{:02}.000Z\"}}\n",
                100 + index,
                index % 60,
            ));
        }
        fs::write(&session_path, appended).expect("append new events");
        set_mtime(&session_path, 200);

        reset_jsonl_open_count();
        let second_count = run_on_lab(75, {
            let cache_path = cache_path.clone();
            let root = tmp.path().to_path_buf();
            move |cx| {
                Box::pin(async move {
                    let reader = ClaudeReader::with_root(root);
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path)
                        .await
                        .len()
                })
            }
        });
        assert_eq!(second_count, 20);
        assert!(jsonl_open_count() >= 1);

        reset_jsonl_open_count();
        let third_count = run_on_lab(76, {
            let cache_path = cache_path.clone();
            let root = tmp.path().to_path_buf();
            move |cx| {
                Box::pin(async move {
                    let reader = ClaudeReader::with_root(root);
                    run_readers_with_cache(&cx, vec![Box::new(reader)], &cache_path)
                        .await
                        .len()
                })
            }
        });
        assert_eq!(third_count, 0);
        assert_eq!(jsonl_open_count(), 0);
    }

    #[test]
    fn missing_claude_dir_returns_ok() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let reader = ClaudeReader::with_root(tmp.path().join("nonexistent"));

        let events: Vec<UsageEvent> = run_on_lab(1, move |cx| {
            Box::pin(async move { run_readers(&cx, vec![Box::new(reader)]).await })
        });

        assert!(events.is_empty());
    }

    #[test]
    fn slug_families_map_correctly() {
        let cases = [
            ("claude-opus-4-7-20251015", "anthropic/claude-opus-4.7"),
            ("claude-sonnet-4-6-20250514", "anthropic/claude-sonnet-4.6"),
            ("claude-haiku-4-5-20251001", "anthropic/claude-haiku-4.5"),
        ];
        for (raw, expected) in cases {
            let slug = model_map::canonical(Source::Claude, raw, None);
            assert_eq!(slug.as_str(), expected, "raw={raw}");
        }
    }
}
