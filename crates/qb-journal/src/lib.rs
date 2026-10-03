use std::{
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use qb_application::{
    mutation::{
        MutationCommand, MutationDisposition, MutationJournal, MutationRecord, RequestReservation,
        TorrentControlAction, FINGERPRINT_VERSION,
    },
    JournalHealthPort, PortError,
};
use qb_domain::{torrent::TorrentId, OperationId, RequestId};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use thiserror::Error;
use uuid::Uuid;

pub const SCHEMA_VERSION: u32 = 2;

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
    #[error("journal state is invalid: {0}")]
    InvalidState(String),
    #[error("operation transition is invalid: {0}")]
    InvalidTransition(String),
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

        let mut connection = Connection::open(&path)?;
        configure(&connection)?;
        migrate(&mut connection)?;

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

    fn reserve_request_inner(
        &self,
        request_id: &RequestId,
        command: &MutationCommand,
    ) -> Result<RequestReservation, JournalError> {
        let command_kind = command.kind();
        let fingerprint = command.fingerprint();
        let command_columns = command_columns(command)?;

        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        if let Some(existing) = load_request(&transaction, request_id.as_str())? {
            let operation_id = OperationId::new(existing.operation_id.clone())
                .map_err(|error| JournalError::InvalidState(error.to_string()))?;

            if existing.fingerprint_version != FINGERPRINT_VERSION
                || existing.command_kind != command_kind
                || existing.command_fingerprint.as_slice() != fingerprint
            {
                return Ok(RequestReservation::Conflict { operation_id });
            }

            let record = load_operation(&transaction, operation_id.as_str())?.ok_or_else(|| {
                JournalError::InvalidState(format!(
                    "request {} references missing operation {}",
                    request_id, operation_id
                ))
            })?;
            transaction.commit()?;
            return Ok(RequestReservation::Replay(record));
        }

        let operation_id = OperationId::new(Uuid::new_v4().to_string())
            .map_err(|error| JournalError::InvalidState(error.to_string()))?;
        let now = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

        transaction.execute(
            &format!(
                "INSERT INTO requests(
                    request_id, fingerprint_version, command_kind, command_fingerprint,
                    operation_id, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, {now})"
            ),
            params![
                request_id.as_str(),
                FINGERPRINT_VERSION,
                command_kind,
                fingerprint.as_slice(),
                operation_id.as_str(),
            ],
        )?;

        transaction.execute(
            &format!(
                "INSERT INTO operations(
                    operation_id, request_id, command_kind,
                    torrent_id, control_action, target_client_count, max_active_downloads,
                    download_limit_bps, upload_limit_bps,
                    checkpoint, disposition, pending_effect_kind, problem_code,
                    revision, created_at, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    'prepared', 'prepared', NULL, NULL, 1, {now}, {now}
                 )"
            ),
            params![
                operation_id.as_str(),
                request_id.as_str(),
                command_kind,
                command_columns.torrent_id,
                command_columns.control_action,
                command_columns.target_client_count,
                command_columns.max_active_downloads,
                command_columns.download_limit_bps,
                command_columns.upload_limit_bps,
            ],
        )?;

        insert_event(
            &transaction,
            operation_id.as_str(),
            1,
            "prepared",
            "prepared",
            MutationDisposition::Prepared,
            None,
            None,
        )?;

        let record = load_operation(&transaction, operation_id.as_str())?
            .ok_or_else(|| JournalError::InvalidState("new operation was not persisted".into()))?;
        transaction.commit()?;
        Ok(RequestReservation::New(record))
    }

    fn transition(
        &self,
        operation_id: &OperationId,
        expected: MutationDisposition,
        next: MutationDisposition,
        checkpoint: &str,
        event_kind: &str,
        pending_effect_kind: Option<&str>,
        problem_code: Option<&str>,
        finished: bool,
    ) -> Result<MutationRecord, JournalError> {
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let current = load_operation(&transaction, operation_id.as_str())?.ok_or_else(|| {
            JournalError::InvalidState(format!("operation {} does not exist", operation_id))
        })?;

        if current.disposition == next
            && current.checkpoint == checkpoint
            && current.pending_effect_kind.as_deref() == pending_effect_kind
            && current.problem_code.as_deref() == problem_code
        {
            transaction.commit()?;
            return Ok(current);
        }

        if current.disposition != expected {
            return Err(JournalError::InvalidTransition(format!(
                "{} cannot move from {} to {}",
                operation_id,
                disposition_name(current.disposition),
                disposition_name(next)
            )));
        }

        let next_revision = current
            .revision()
            .checked_add(1)
            .ok_or_else(|| JournalError::InvalidState("operation revision overflow".into()))?;

        let finished_sql = if finished {
            "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
        } else {
            "NULL"
        };

        let changed = transaction.execute(
            &format!(
                "UPDATE operations
                 SET checkpoint = ?1,
                     disposition = ?2,
                     pending_effect_kind = ?3,
                     problem_code = ?4,
                     revision = ?5,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                     finished_at = {finished_sql}
                 WHERE operation_id = ?6 AND revision = ?7 AND disposition = ?8"
            ),
            params![
                checkpoint,
                disposition_name(next),
                pending_effect_kind,
                problem_code,
                next_revision,
                operation_id.as_str(),
                current.revision,
                disposition_name(expected),
            ],
        )?;

        if changed != 1 {
            return Err(JournalError::InvalidTransition(format!(
                "{} changed while transition was being committed",
                operation_id
            )));
        }

        insert_event(
            &transaction,
            operation_id.as_str(),
            next_revision,
            event_kind,
            checkpoint,
            next,
            pending_effect_kind,
            problem_code,
        )?;

        let record = load_operation(&transaction, operation_id.as_str())?
            .ok_or_else(|| JournalError::InvalidState("updated operation disappeared".into()))?;
        transaction.commit()?;
        Ok(record)
    }
}

impl JournalHealthPort for Journal {
    fn schema_version(&self) -> Result<u32, PortError> {
        Journal::schema_version(self).map_err(map_port_error)
    }

    fn quick_check(&self) -> Result<(), PortError> {
        Journal::quick_check(self).map_err(map_port_error)
    }
}

impl MutationJournal for Journal {
    fn reserve_request(
        &self,
        request_id: &RequestId,
        command: &MutationCommand,
    ) -> Result<RequestReservation, PortError> {
        self.reserve_request_inner(request_id, command)
            .map_err(map_port_error)
    }

    fn mark_effect_pending(
        &self,
        operation_id: &OperationId,
        effect_kind: &str,
    ) -> Result<MutationRecord, PortError> {
        if effect_kind.is_empty() {
            return Err(PortError::new(
                "JOURNAL_STATE_INVALID",
                "effect kind must not be empty",
            ));
        }
        self.transition(
            operation_id,
            MutationDisposition::Prepared,
            MutationDisposition::EffectPending,
            "effect_pending",
            "effect_pending",
            Some(effect_kind),
            None,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_observed_applied(
        &self,
        operation_id: &OperationId,
    ) -> Result<MutationRecord, PortError> {
        let current = self
            .get_operation(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let effect = current.pending_effect_kind.clone();

        self.transition(
            operation_id,
            MutationDisposition::EffectPending,
            MutationDisposition::ObservedApplied,
            "observed_applied",
            "observed_applied",
            effect.as_deref(),
            None,
            false,
        )
        .map_err(map_port_error)
    }

    fn finish(&self, operation_id: &OperationId) -> Result<MutationRecord, PortError> {
        self.transition(
            operation_id,
            MutationDisposition::ObservedApplied,
            MutationDisposition::Finished,
            "finished",
            "finished",
            None,
            None,
            true,
        )
        .map_err(map_port_error)
    }

    fn mark_unknown(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<MutationRecord, PortError> {
        let current = self
            .get_operation(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let effect = current.pending_effect_kind.clone();

        self.transition(
            operation_id,
            MutationDisposition::EffectPending,
            MutationDisposition::Unknown,
            "unknown",
            "unknown",
            effect.as_deref(),
            Some(problem_code),
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<MutationRecord, PortError> {
        let current = self
            .get_operation(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let effect = current.pending_effect_kind.clone();

        self.transition(
            operation_id,
            MutationDisposition::EffectPending,
            MutationDisposition::Failed,
            "failed",
            "failed",
            effect.as_deref(),
            Some(problem_code),
            true,
        )
        .map_err(map_port_error)
    }

    fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<MutationRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        load_operation(&connection, operation_id.as_str()).map_err(map_port_error)
    }
}

fn configure(connection: &Connection) -> Result<(), JournalError> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(Duration::from_secs(2))?;
    Ok(())
}

fn migrate(connection: &mut Connection) -> Result<(), JournalError> {
    let mut version: u32 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;

    if version > SCHEMA_VERSION {
        return Err(JournalError::StateVersionUnsupported {
            found: version,
            supported: SCHEMA_VERSION,
        });
    }

    if version == 0 {
        if has_user_tables(connection)? {
            return Err(JournalError::MigrationRequired);
        }
        create_schema_v1(connection)?;
        version = 1;
    }

    if version == 1 {
        migrate_v1_to_v2(connection)?;
        version = 2;
    }

    if version != SCHEMA_VERSION {
        return Err(JournalError::InvalidState(format!(
            "migration stopped at schema {version}"
        )));
    }

    Ok(())
}

fn create_schema_v1(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE schema_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL,
            application_min_version TEXT NOT NULL,
            migrated_at TEXT NOT NULL
        );

        INSERT INTO schema_meta(singleton, schema_version, application_min_version, migrated_at)
        VALUES (1, 1, '0.1.0', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

        PRAGMA user_version = 1;
        "#,
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v1_to_v2(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE requests (
            request_id TEXT PRIMARY KEY,
            fingerprint_version INTEGER NOT NULL,
            command_kind TEXT NOT NULL,
            command_fingerprint BLOB NOT NULL CHECK(length(command_fingerprint) = 32),
            operation_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE operations (
            operation_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE REFERENCES requests(request_id),
            command_kind TEXT NOT NULL,
            torrent_id TEXT,
            control_action TEXT,
            target_client_count INTEGER CHECK(target_client_count >= 0),
            max_active_downloads INTEGER CHECK(max_active_downloads >= 0),
            download_limit_bps INTEGER CHECK(download_limit_bps >= 0),
            upload_limit_bps INTEGER CHECK(upload_limit_bps >= 0),
            checkpoint TEXT NOT NULL,
            disposition TEXT NOT NULL,
            pending_effect_kind TEXT,
            problem_code TEXT,
            revision INTEGER NOT NULL CHECK(revision > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT
        );

        CREATE TABLE operation_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL REFERENCES operations(operation_id),
            revision INTEGER NOT NULL CHECK(revision > 0),
            event_kind TEXT NOT NULL,
            checkpoint TEXT NOT NULL,
            disposition TEXT NOT NULL,
            pending_effect_kind TEXT,
            problem_code TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(operation_id, revision)
        );

        CREATE TABLE controller_policy (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            revision INTEGER NOT NULL CHECK(revision > 0),
            target_client_count INTEGER CHECK(target_client_count >= 0),
            updated_at TEXT NOT NULL
        );

        INSERT INTO controller_policy(singleton, revision, target_client_count, updated_at)
        VALUES (1, 1, NULL, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

        UPDATE schema_meta
        SET schema_version = 2,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 2;
        "#,
    )?;
    transaction.commit()?;
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

struct StoredRequest {
    fingerprint_version: u32,
    command_kind: String,
    command_fingerprint: Vec<u8>,
    operation_id: String,
}

fn load_request(
    transaction: &Transaction<'_>,
    request_id: &str,
) -> Result<Option<StoredRequest>, JournalError> {
    transaction
        .query_row(
            "SELECT fingerprint_version, command_kind, command_fingerprint, operation_id
             FROM requests WHERE request_id = ?1",
            [request_id],
            |row| {
                Ok(StoredRequest {
                    fingerprint_version: row.get(0)?,
                    command_kind: row.get(1)?,
                    command_fingerprint: row.get(2)?,
                    operation_id: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(JournalError::from)
}

trait QueryOperation {
    fn query_operation(&self, operation_id: &str) -> Result<Option<MutationRecord>, JournalError>;
}

impl QueryOperation for Connection {
    fn query_operation(&self, operation_id: &str) -> Result<Option<MutationRecord>, JournalError> {
        self.query_row(operation_select_sql(), [operation_id], map_operation_row)
            .optional()
            .map_err(JournalError::from)
    }
}

impl QueryOperation for Transaction<'_> {
    fn query_operation(&self, operation_id: &str) -> Result<Option<MutationRecord>, JournalError> {
        self.query_row(operation_select_sql(), [operation_id], map_operation_row)
            .optional()
            .map_err(JournalError::from)
    }
}

fn load_operation(
    query: &impl QueryOperation,
    operation_id: &str,
) -> Result<Option<MutationRecord>, JournalError> {
    query.query_operation(operation_id)
}

fn operation_select_sql() -> &'static str {
    "SELECT request_id, operation_id, command_kind, fingerprint_version,
            command_fingerprint,
            torrent_id, control_action, target_client_count, max_active_downloads,
            download_limit_bps, upload_limit_bps,
            checkpoint, disposition, pending_effect_kind, problem_code, revision
     FROM operations
     JOIN requests USING(request_id)
     WHERE operation_id = ?1"
}

fn map_operation_row(row: &Row<'_>) -> rusqlite::Result<MutationRecord> {
    let request_id: String = row.get(0)?;
    let operation_id: String = row.get(1)?;
    let fingerprint: Vec<u8> = row.get(4)?;

    let request_id = RequestId::new(request_id).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let operation_id = OperationId::new(operation_id).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let command_fingerprint: [u8; 32] = fingerprint.try_into().map_err(|value: Vec<u8>| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Blob,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "command fingerprint must contain 32 bytes",
            )),
        )
    })?;

    let command_kind: String = row.get(2)?;
    let command = decode_command(
        &command_kind,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
    )
    .map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        )
    })?;

    let disposition_text: String = row.get(12)?;
    let disposition = parse_disposition(&disposition_text).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown mutation disposition '{disposition_text}'"),
            )),
        )
    })?;
    let revision: u64 = row.get(15)?;

    Ok(MutationRecord {
        request_id,
        operation_id,
        command,
        fingerprint_version: row.get(3)?,
        command_fingerprint,
        checkpoint: row.get(11)?,
        disposition,
        pending_effect_kind: row.get(13)?,
        problem_code: row.get(14)?,
        revision,
    })
}

struct CommandColumns<'a> {
    torrent_id: Option<&'a str>,
    control_action: Option<&'static str>,
    target_client_count: Option<i64>,
    max_active_downloads: Option<i64>,
    download_limit_bps: Option<i64>,
    upload_limit_bps: Option<i64>,
}

fn command_columns(command: &MutationCommand) -> Result<CommandColumns<'_>, JournalError> {
    match command {
        MutationCommand::TorrentControl { torrent_id, action } => Ok(CommandColumns {
            torrent_id: Some(torrent_id.as_str()),
            control_action: Some(match action {
                TorrentControlAction::Stop => "stop",
                TorrentControlAction::Start => "start",
            }),
            target_client_count: None,
            max_active_downloads: None,
            download_limit_bps: None,
            upload_limit_bps: None,
        }),
        MutationCommand::SetQueueTarget {
            target_client_count,
        } => Ok(CommandColumns {
            torrent_id: None,
            control_action: None,
            target_client_count: Some(i64::from(*target_client_count)),
            max_active_downloads: None,
            download_limit_bps: None,
            upload_limit_bps: None,
        }),
        MutationCommand::SetActiveDownloads {
            max_active_downloads,
        } => Ok(CommandColumns {
            torrent_id: None,
            control_action: None,
            target_client_count: None,
            max_active_downloads: Some(i64::from(*max_active_downloads)),
            download_limit_bps: None,
            upload_limit_bps: None,
        }),
        MutationCommand::SetDownloadLimit { bytes_per_sec } => Ok(CommandColumns {
            torrent_id: None,
            control_action: None,
            target_client_count: None,
            max_active_downloads: None,
            download_limit_bps: optional_u64_to_i64(Some(*bytes_per_sec))?,
            upload_limit_bps: None,
        }),
        MutationCommand::SetUploadLimit { bytes_per_sec } => Ok(CommandColumns {
            torrent_id: None,
            control_action: None,
            target_client_count: None,
            max_active_downloads: None,
            download_limit_bps: None,
            upload_limit_bps: optional_u64_to_i64(Some(*bytes_per_sec))?,
        }),
    }
}

fn optional_u64_to_i64(value: Option<u64>) -> Result<Option<i64>, JournalError> {
    value
        .map(|value| {
            i64::try_from(value).map_err(|_| {
                JournalError::InvalidState("mutation numeric value exceeds SQLite integer range".into())
            })
        })
        .transpose()
}

fn decode_command(
    kind: &str,
    torrent_id: Option<String>,
    control_action: Option<String>,
    target_client_count: Option<i64>,
    max_active_downloads: Option<i64>,
    download_limit_bps: Option<i64>,
    upload_limit_bps: Option<i64>,
) -> Result<MutationCommand, String> {
    match kind {
        "torrent.stop" | "torrent.start" => {
            let torrent_id = TorrentId::new(
                torrent_id.ok_or_else(|| "torrent control is missing torrent_id".to_string())?,
            )
            .map_err(|error| error.to_string())?;
            let expected_action = if kind == "torrent.stop" { "stop" } else { "start" };
            if control_action.as_deref() != Some(expected_action) {
                return Err(format!(
                    "torrent control action does not match command kind {kind}"
                ));
            }
            Ok(MutationCommand::TorrentControl {
                torrent_id,
                action: if expected_action == "stop" {
                    TorrentControlAction::Stop
                } else {
                    TorrentControlAction::Start
                },
            })
        }
        "queue.target.set" => Ok(MutationCommand::SetQueueTarget {
            target_client_count: required_nonnegative_u32(
                target_client_count,
                "target_client_count",
            )?,
        }),
        "queue.downloads.set" => Ok(MutationCommand::SetActiveDownloads {
            max_active_downloads: required_nonnegative_u32(
                max_active_downloads,
                "max_active_downloads",
            )?,
        }),
        "transfer.download_limit.set" => Ok(MutationCommand::SetDownloadLimit {
            bytes_per_sec: required_nonnegative_u64(
                download_limit_bps,
                "download_limit_bps",
            )?,
        }),
        "transfer.upload_limit.set" => Ok(MutationCommand::SetUploadLimit {
            bytes_per_sec: required_nonnegative_u64(upload_limit_bps, "upload_limit_bps")?,
        }),
        other => Err(format!("unknown mutation command kind '{other}'")),
    }
}

fn required_nonnegative_u32(value: Option<i64>, field: &str) -> Result<u32, String> {
    let value = value.ok_or_else(|| format!("{field} is missing"))?;
    u32::try_from(value).map_err(|_| format!("{field} is outside u32 range"))
}

fn required_nonnegative_u64(value: Option<i64>, field: &str) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("{field} is missing"))?;
    u64::try_from(value).map_err(|_| format!("{field} must be non-negative"))
}

fn insert_event(
    transaction: &Transaction<'_>,
    operation_id: &str,
    revision: u64,
    event_kind: &str,
    checkpoint: &str,
    disposition: MutationDisposition,
    pending_effect_kind: Option<&str>,
    problem_code: Option<&str>,
) -> Result<(), JournalError> {
    transaction.execute(
        "INSERT INTO operation_events(
            operation_id, revision, event_kind, checkpoint, disposition,
            pending_effect_kind, problem_code, created_at
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         )",
        params![
            operation_id,
            revision,
            event_kind,
            checkpoint,
            disposition_name(disposition),
            pending_effect_kind,
            problem_code,
        ],
    )?;
    Ok(())
}

fn disposition_name(value: MutationDisposition) -> &'static str {
    match value {
        MutationDisposition::Prepared => "prepared",
        MutationDisposition::EffectPending => "effect_pending",
        MutationDisposition::ObservedApplied => "observed_applied",
        MutationDisposition::Finished => "finished",
        MutationDisposition::Blocked => "blocked",
        MutationDisposition::Unknown => "unknown",
        MutationDisposition::Failed => "failed",
    }
}

fn parse_disposition(value: &str) -> Option<MutationDisposition> {
    match value {
        "prepared" => Some(MutationDisposition::Prepared),
        "effect_pending" => Some(MutationDisposition::EffectPending),
        "observed_applied" => Some(MutationDisposition::ObservedApplied),
        "finished" => Some(MutationDisposition::Finished),
        "blocked" => Some(MutationDisposition::Blocked),
        "unknown" => Some(MutationDisposition::Unknown),
        "failed" => Some(MutationDisposition::Failed),
        _ => None,
    }
}

fn map_port_error(error: JournalError) -> PortError {
    let code = match &error {
        JournalError::InvalidTransition(_) => "OPERATION_TRANSITION_INVALID",
        JournalError::InvalidState(_) => "JOURNAL_STATE_INVALID",
        _ => "JOURNAL_UNAVAILABLE",
    };
    PortError::new(code, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_id(value: &str) -> RequestId {
        RequestId::new(value).expect("request id")
    }

    fn torrent_command(action: TorrentControlAction) -> MutationCommand {
        MutationCommand::TorrentControl {
            torrent_id: TorrentId::new("abcdef0123456789abcdef0123456789abcdef01")
                .expect("torrent id"),
            action,
        }
    }

    #[test]
    fn creates_schema_v2_and_reopens() {
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
    fn migrates_v1_to_v2_additively() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let mut connection = Connection::open(&path).expect("sqlite");
        configure(&connection).expect("configure");
        create_schema_v1(&mut connection).expect("v1 schema");
        drop(connection);

        let journal = Journal::open(&path).expect("migrate");
        assert_eq!(journal.schema_version().expect("version"), 2);

        let connection = journal.connection.lock().expect("journal mutex");
        let table_count: u32 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name IN ('requests','operations','operation_events','controller_policy')",
                [],
                |row| row.get(0),
            )
            .expect("table count");
        assert_eq!(table_count, 4);
    }

    #[test]
    fn request_reservation_replays_and_conflicts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = request_id("pause-1");
        let command = torrent_command(TorrentControlAction::Stop);

        let first = journal
            .reserve_request_inner(&request, &command)
            .expect("reserve");
        let first_operation = match first {
            RequestReservation::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        let replay = journal
            .reserve_request_inner(&request, &command)
            .expect("replay");
        match replay {
            RequestReservation::Replay(record) => {
                assert_eq!(record.operation_id, first_operation);
                assert_eq!(record.disposition, MutationDisposition::Prepared);
            }
            other => panic!("unexpected replay: {other:?}"),
        }

        let conflict = journal
            .reserve_request_inner(
                &request,
                &torrent_command(TorrentControlAction::Start),
            )
            .expect("conflict");
        assert_eq!(
            conflict,
            RequestReservation::Conflict {
                operation_id: first_operation
            }
        );
    }

    #[test]
    fn typed_command_survives_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let request = request_id("queue-1");
        let command = MutationCommand::SetQueueTarget {
            target_client_count: 80,
        };

        let operation_id = {
            let journal = Journal::open(&path).expect("journal");
            match journal
                .reserve_request_inner(&request, &command)
                .expect("reserve")
            {
                RequestReservation::New(record) => record.operation_id,
                other => panic!("unexpected reservation: {other:?}"),
            }
        };

        let reopened = Journal::open(&path).expect("reopen");
        let record = reopened
            .get_operation(&operation_id)
            .expect("get")
            .expect("operation");
        assert_eq!(record.command, command);
        assert_eq!(record.command_fingerprint, command.fingerprint());
    }

    #[test]
    fn mutation_checkpoint_progress_is_monotonic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = request_id("resume-1");

        let reservation = journal
            .reserve_request_inner(
                &request,
                &torrent_command(TorrentControlAction::Start),
            )
            .expect("reserve");
        let operation_id = match reservation {
            RequestReservation::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        let pending = journal
            .mark_effect_pending(&operation_id, "qbit.start")
            .expect("pending");
        assert_eq!(pending.disposition, MutationDisposition::EffectPending);

        let observed = journal
            .mark_observed_applied(&operation_id)
            .expect("observed");
        assert_eq!(observed.disposition, MutationDisposition::ObservedApplied);

        let finished = journal.finish(&operation_id).expect("finish");
        assert_eq!(finished.disposition, MutationDisposition::Finished);

        let backwards = journal.mark_effect_pending(&operation_id, "qbit.start");
        assert!(backwards.is_err());
    }

    #[test]
    fn unknown_preserves_pending_effect_for_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = request_id("pause-unknown");

        let reservation = journal
            .reserve_request_inner(
                &request,
                &torrent_command(TorrentControlAction::Stop),
            )
            .expect("reserve");
        let operation_id = match reservation {
            RequestReservation::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        journal
            .mark_effect_pending(&operation_id, "qbit.stop")
            .expect("pending");
        let unknown = journal
            .mark_unknown(&operation_id, "QBIT_MUTATION_UNCERTAIN")
            .expect("unknown");

        assert_eq!(unknown.disposition, MutationDisposition::Unknown);
        assert_eq!(unknown.pending_effect_kind.as_deref(), Some("qbit.stop"));
        assert_eq!(
            unknown.problem_code.as_deref(),
            Some("QBIT_MUTATION_UNCERTAIN")
        );
    }

    #[test]
    fn rejects_newer_schema_version() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");

        let connection = Connection::open(&path).expect("sqlite");
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .expect("set newer version");
        drop(connection);

        let error = Journal::open(&path)
            .err()
            .expect("must reject newer schema");
        assert!(matches!(
            error,
            JournalError::StateVersionUnsupported {
                found,
                supported
            } if found == SCHEMA_VERSION + 1 && supported == SCHEMA_VERSION
        ));
    }

    #[test]
    fn corrupt_database_is_not_replaced() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        std::fs::write(&path, b"not a sqlite database").expect("write corrupt db");

        let error = Journal::open(&path).err().expect("must reject corrupt db");
        assert!(matches!(error, JournalError::Sqlite(_)));

        let bytes = std::fs::read(&path).expect("read corrupt db");
        assert_eq!(bytes, b"not a sqlite database");
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
