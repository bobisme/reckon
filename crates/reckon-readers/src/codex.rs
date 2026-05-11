use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{env, fs, io};

use regex::Regex;

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

#[cfg(test)]
mod tests {
    use super::*;

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
        fs::write(
            sessions_dir.join("2026/05/10/logs_2.sqlite"),
            "dummy",
        )
        .expect("write");
        fs::write(
            sessions_dir.join("2026/05/10/other-file.txt"),
            "dummy",
        )
        .expect("write");
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
            ("2026/05/15/rollout-2026-05-15T10-00-00-c.jsonl", (2026, 5, 15)),
            ("2026/05/10/rollout-2026-05-10T10-00-00-a.jsonl", (2026, 5, 10)),
            ("2026/05/12/rollout-2026-05-12T10-00-00-b.jsonl", (2026, 5, 12)),
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
}
