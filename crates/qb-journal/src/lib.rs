use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use qb_application::{JournalHealthPort, PortError};
use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("journal I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("state schema {found} is newer than supported schema {supported}")]
    StateVersionUnsupported { found: u32, supported: u32 },
    #[error("existing unversioned state requires an explicit migration")]
    MigrationRequired,
    #[error("SQLite quick_check failed: {0}")]
    Integrity(String),
}

pub struct Journal {
    path: PathBuf,
    connection: Mutex<Connection>,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let connection = Connection::open(&path)?;
        configure(&connection)?;
        migrate(&connection)?;

        Ok(Self {
            path,
            connection: Mutex::new(connection),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn schema_version(&self) -> Result<u32, JournalError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let version = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        Ok(version)
    }

    pub fn quick_check(&self) -> Result<(), JournalError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let value: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if value.eq_ignore_ascii_case("ok") {
            Ok(())
        } else {
            Err(JournalError::Integrity(value))
        }
    }
}

impl JournalHealthPort for Journal {
    fn schema_version(&self) -> Result<u32, PortError> {
        Journal::schema_version(self)
            .map_err(|error| PortError::new("JOURNAL_UNAVAILABLE", error.to_string()))
    }

    fn quick_check(&self) -> Result<(), PortError> {
        Journal::quick_check(self)
            .map_err(|error| PortError::new("JOURNAL_UNAVAILABLE", error.to_string()))
    }
}

fn configure(connection: &Connection) -> Result<(), JournalError> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(Duration::from_secs(2))?;
    Ok(())
}

fn migrate(connection: &Connection) -> Result<(), JournalError> {
    let version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;

    if version > SCHEMA_VERSION {
        return Err(JournalError::StateVersionUnsupported {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }

    if version == SCHEMA_VERSION {
        return Ok(());
    }

    if version == 0 && has_user_tables(connection)? {
        return Err(JournalError::MigrationRequired);
    }

    connection.execute_batch(
        r#"
        BEGIN IMMEDIATE;

        CREATE TABLE schema_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL,
            application_min_version TEXT NOT NULL,
            migrated_at TEXT NOT NULL
        );

        CREATE TABLE requests (
            request_id TEXT PRIMARY KEY,
            command_kind TEXT NOT NULL,
            command_fingerprint BLOB NOT NULL,
            state TEXT NOT NULL,
            operation_id TEXT,
            job_id TEXT,
            final_status TEXT,
            response_payload BLOB,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE operations (
            operation_id TEXT PRIMARY KEY,
            request_id TEXT,
            parent_job_id TEXT,
            kind TEXT NOT NULL,
            torrent_identity_key TEXT,
            checkpoint TEXT NOT NULL,
            disposition TEXT NOT NULL,
            revision INTEGER NOT NULL,
            pending_effect_kind TEXT,
            problem_code TEXT,
            state_payload BLOB,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            FOREIGN KEY(request_id) REFERENCES requests(request_id)
        );

        CREATE TABLE operation_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL,
            operation_revision INTEGER NOT NULL,
            event_kind TEXT NOT NULL,
            checkpoint TEXT NOT NULL,
            disposition TEXT NOT NULL,
            payload BLOB,
            created_at TEXT NOT NULL,
            FOREIGN KEY(operation_id) REFERENCES operations(operation_id)
        );

        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            parent_request_id TEXT,
            kind TEXT NOT NULL,
            state TEXT NOT NULL,
            current_operation_id TEXT,
            revision INTEGER NOT NULL,
            payload BLOB,
            problem_code TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            FOREIGN KEY(parent_request_id) REFERENCES requests(request_id)
        );

        CREATE TABLE job_operations (
            job_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            operation_id TEXT NOT NULL,
            PRIMARY KEY(job_id, ordinal),
            FOREIGN KEY(job_id) REFERENCES jobs(job_id),
            FOREIGN KEY(operation_id) REFERENCES operations(operation_id)
        );

        CREATE TABLE policies (
            policy_key TEXT PRIMARY KEY,
            revision INTEGER NOT NULL,
            payload BLOB NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE plans (
            plan_id TEXT PRIMARY KEY,
            policy_revision INTEGER NOT NULL,
            global_guard BLOB NOT NULL,
            payload BLOB NOT NULL,
            created_at TEXT NOT NULL,
            consumed_at TEXT
        );

        INSERT INTO schema_meta(singleton, schema_version, application_min_version, migrated_at)
        VALUES (1, 1, '0.1.0', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

        PRAGMA user_version = 1;
        COMMIT;
        "#,
    )?;

    Ok(())
}

fn has_user_tables(connection: &Connection) -> Result<bool, JournalError> {
    let table: Option<String> = connection
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(table.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_and_reopens_schema() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");

        let journal = Journal::open(&path).expect("create");
        assert_eq!(journal.schema_version().expect("version"), SCHEMA_VERSION);
        journal.quick_check().expect("quick_check");
        drop(journal);

        let reopened = Journal::open(&path).expect("reopen");
        assert_eq!(reopened.schema_version().expect("version"), SCHEMA_VERSION);
    }

    #[test]
    fn rejects_unknown_unversioned_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let connection = Connection::open(&path).expect("sqlite");
        connection
            .execute("CREATE TABLE legacy(value TEXT)", [])
            .expect("legacy table");
        drop(connection);

        let error = Journal::open(&path).err().expect("must reject legacy DB");
        assert!(matches!(error, JournalError::MigrationRequired));
    }
}
