use std::fs;
use std::path::Path;

use rusqlite::{Connection, Result};

const CACHE_SCHEMA_VERSION: i32 = 2;

/// # Panics
///
/// Panics if the cache database cannot be opened, migrated, or configured.
#[must_use]
pub fn open_cache(path: &Path) -> Connection {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).unwrap_or_else(|error| {
            panic!(
                "failed to create cache directory {}: {error}",
                parent.display()
            )
        });
    }

    let conn = Connection::open(path).unwrap_or_else(|error| {
        panic!("failed to open cache database {}: {error}", path.display())
    });

    apply_wal_and_schema(&conn);
    conn
}

fn apply_wal_and_schema(conn: &Connection) {
    let current = current_user_version(conn).expect("failed to read cache schema version");

    if current == CACHE_SCHEMA_VERSION {
        set_wal_mode(conn).expect("failed to enable WAL mode");
        return;
    }

    conn.execute_batch(
        "
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        ",
    )
    .unwrap_or_else(|error| panic!("failed to enable journal mode: {error}"));

    match current {
        0 => {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS source_files (
                    source TEXT NOT NULL,
                    path TEXT NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    last_offset INTEGER NOT NULL,
                    PRIMARY KEY (source, path)
                );

                CREATE TABLE IF NOT EXISTS events (
                    source TEXT NOT NULL,
                    dedup_key TEXT NOT NULL,
                    month TEXT NOT NULL,
                    model TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    project TEXT,
                    input INTEGER NOT NULL DEFAULT 0,
                    output INTEGER NOT NULL DEFAULT 0,
                    cache_read INTEGER NOT NULL DEFAULT 0,
                    cache_write INTEGER NOT NULL DEFAULT 0,
                    reasoning INTEGER NOT NULL DEFAULT 0,
                    known_cost_usd REAL,
                    byok_usage_inference INTEGER,
                    PRIMARY KEY (source, dedup_key)
                );

                CREATE INDEX IF NOT EXISTS events_month ON events(month);
                ",
            )
            .unwrap_or_else(|error| panic!("failed to create cache schema: {error}"));

            conn.pragma_update(None, "user_version", CACHE_SCHEMA_VERSION)
                .expect("failed to set cache schema version");
        }
        1 => {
            conn.execute_batch(
                "
                ALTER TABLE events ADD COLUMN known_cost_usd REAL;
                ALTER TABLE events ADD COLUMN byok_usage_inference INTEGER;
                ",
            )
            .unwrap_or_else(|error| panic!("failed to migrate cache schema to v2: {error}"));

            conn.pragma_update(None, "user_version", CACHE_SCHEMA_VERSION)
                .expect("failed to set cache schema version");
        }
        other => {
            panic!("unsupported cache schema version: {other}");
        }
    }
}

fn current_user_version(conn: &Connection) -> Result<i32> {
    conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
}

fn set_wal_mode(conn: &Connection) -> Result<()> {
    let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
        row.get::<_, String>(0)
    })?;

    if mode.to_lowercase() != "wal" {
        return Err(rusqlite::Error::ExecuteReturnedResults);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;

    use tempfile::tempdir;

    #[test]
    fn first_run_creates_db_and_sets_version() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("index.sqlite");

        assert!(!path.exists());

        let conn = open_cache(&path);
        assert!(path.exists());

        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
            .expect("read user_version");
        assert_eq!(version, CACHE_SCHEMA_VERSION);

        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('source_files','events')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("count schema tables");
        assert_eq!(count, 2);

        let wal: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .expect("read journal mode");
        assert_eq!(wal.to_lowercase(), "wal");

        conn.close().expect("close db");
    }

    #[test]
    fn v1_database_is_noop_on_reopen() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("index.sqlite");

        {
            let conn = open_cache(&path);
            conn.close().expect("close db");
        }

        {
            let conn = Connection::open(&path).expect("open db direct");
            conn.execute("PRAGMA user_version = 0", [])
                .expect("set version to 0");
            conn.close().expect("close db");
        }

        let conn = open_cache(&path);
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
            .expect("read user_version");
        assert_eq!(version, CACHE_SCHEMA_VERSION);

        let events_index = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='index' AND name='events_month'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("query events index");
        assert_eq!(events_index, 1);

        conn.close().expect("close db");
    }

    #[test]
    fn user_version_zero_runs_migration() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("index.sqlite");

        let conn = open_cache(&path);
        conn.pragma_update(None, "user_version", 0)
            .expect("set version to 0");
        conn.close().expect("close db");

        let conn = open_cache(&path);
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
            .expect("read user_version");
        assert_eq!(version, CACHE_SCHEMA_VERSION);
        let mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .expect("read journal mode");
        assert_eq!(mode.to_lowercase(), "wal");
        conn.close().expect("close db");
    }

    #[test]
    fn concurrent_opens_do_not_corrupt_db() {
        let dir = tempdir().expect("temp dir");
        let path = dir.path().join("index.sqlite");

        let path_for_thread1: PathBuf = path.clone();
        let path_for_thread2: PathBuf = path.clone();
        let path_for_thread3: PathBuf = path;

        let (ready_tx, ready_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (opened_tx, opened_rx) = mpsc::channel::<()>();

        let thread1 = thread::spawn(move || {
            let conn = open_cache(&path_for_thread1);
            ready_tx.send(()).expect("signal ready");
            release_rx.recv().expect("wait release");
            conn.execute(
                "INSERT INTO source_files (source, path, mtime_ns, size_bytes, last_offset) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["claude", "/tmp/test", 0, 0, 0],
            )
            .expect("insert row");
            drop(conn);
        });

        ready_rx.recv().expect("thread1 ready");

        let thread2 = thread::spawn(move || {
            let conn2 = open_cache(&path_for_thread2);
            opened_tx.send(()).expect("thread2 opened");
            conn2
                .execute(
                    "INSERT INTO source_files (source, path, mtime_ns, size_bytes, last_offset) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params!["codex", "/tmp/test", 1, 1, 1],
                )
                .expect("insert row");

            conn2.close().expect("close db");
        });

        opened_rx.recv().expect("thread2 opened");
        release_tx.send(()).expect("release thread1");

        thread1.join().expect("thread1 panic");
        thread2.join().expect("thread2 panic");

        let conn3 = open_cache(&path_for_thread3);
        let exists: i64 = conn3
            .query_row("SELECT count(*) FROM source_files", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count source_files");
        assert_eq!(exists, 2);
        conn3.close().expect("close db");
    }
}
