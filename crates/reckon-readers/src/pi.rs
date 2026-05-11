use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use rusqlite::{Connection, OpenFlags};

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

fn home_dir() -> PathBuf {
    env::var("HOME").map(PathBuf::from).expect("HOME not set")
}

fn should_use_sqlite_index(path: &Path, walk_entries: &[PathBuf]) -> bool {
    if !path.exists() {
        return false;
    }

    let Ok(db_mtime) = file_modified_secs(path) else { return false };

    if walk_entries.is_empty() {
        return true;
    }

    let mut latest_jsonl = None;
    for entry in walk_entries {
        let Ok(modified) = file_modified_secs(entry) else { return false };

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

#[cfg(test)]
mod tests {
    use super::*;
    use filetime::{self, FileTime};
    use std::collections::BTreeSet;

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
}
