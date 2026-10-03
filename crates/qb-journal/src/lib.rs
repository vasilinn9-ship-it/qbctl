use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use qb_application::{
    admission::{
        AdmissionJournal, AdmissionRecord, AdmissionReservationRequest, AdmissionReservationResult,
        CapacityReservation, ADMISSION_FINGERPRINT_VERSION,
    },
    cleanup::{
        IncomingCleanupIntent, IncomingCleanupJournal, IncomingCleanupRecord,
        IncomingCleanupReservation, IncomingCleanupState, INCOMING_CLEANUP_FINGERPRINT_VERSION,
    },
    completion::{
        CompletionFileRecord, CompletionFileState, CompletionHandoffStrategy, CompletionJournal,
        CompletionPreflight, CompletionRecord, CompletionReservation, CompletionState,
        COMPLETION_FINGERPRINT_VERSION,
    },
    mutation::{
        MutationCommand, MutationDisposition, MutationJournal, MutationRecord, QueueTargetPolicy,
        RequestReservation, TorrentControlAction, FINGERPRINT_VERSION,
    },
    registry::{
        RegisterIncoming, RegisterIncomingResult, RegistryRecord, RegistryState, TorrentRegistry,
    },
    release::{
        ReleaseJournal, ReleaseRecord, ReleaseRequest, ReleaseReservation, ReleaseResolution,
        ReleaseState, RELEASE_FINGERPRINT_VERSION,
    },
    JournalHealthPort, PortError,
};
use qb_domain::{
    torrent::{TorrentId, TorrentIdentity},
    OperationId, RequestId,
};
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction, TransactionBehavior};
use thiserror::Error;
use uuid::Uuid;

pub const SCHEMA_VERSION: u32 = 7;

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
    #[error("torrent identity conflict: {0}")]
    IdentityConflict(String),
    #[error("torrent is already processed: {0}")]
    AlreadyProcessed(String),
}

pub struct Journal {
    path: PathBuf,
    connection: Mutex<Connection>,
}

struct TransitionSpec<'a> {
    expected: MutationDisposition,
    next: MutationDisposition,
    checkpoint: &'a str,
    event_kind: &'a str,
    pending_effect_kind: Option<&'a str>,
    problem_code: Option<&'a str>,
    finished: bool,
}

struct EventSpec<'a> {
    revision: u64,
    event_kind: &'a str,
    checkpoint: &'a str,
    disposition: MutationDisposition,
    pending_effect_kind: Option<&'a str>,
    problem_code: Option<&'a str>,
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
            EventSpec {
                revision: 1,
                event_kind: "prepared",
                checkpoint: "prepared",
                disposition: MutationDisposition::Prepared,
                pending_effect_kind: None,
                problem_code: None,
            },
        )?;

        let record = load_operation(&transaction, operation_id.as_str())?
            .ok_or_else(|| JournalError::InvalidState("new operation was not persisted".into()))?;
        transaction.commit()?;
        Ok(RequestReservation::New(record))
    }

    fn transition(
        &self,
        operation_id: &OperationId,
        spec: TransitionSpec<'_>,
    ) -> Result<MutationRecord, JournalError> {
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let current = load_operation(&transaction, operation_id.as_str())?.ok_or_else(|| {
            JournalError::InvalidState(format!("operation {} does not exist", operation_id))
        })?;

        if current.disposition == spec.next
            && current.checkpoint == spec.checkpoint
            && current.pending_effect_kind.as_deref() == spec.pending_effect_kind
            && current.problem_code.as_deref() == spec.problem_code
        {
            transaction.commit()?;
            return Ok(current);
        }

        if current.disposition != spec.expected {
            return Err(JournalError::InvalidTransition(format!(
                "{} cannot move from {} to {}",
                operation_id,
                disposition_name(current.disposition),
                disposition_name(spec.next)
            )));
        }

        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| JournalError::InvalidState("operation revision overflow".into()))?;

        let finished_sql = if spec.finished {
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
                spec.checkpoint,
                disposition_name(spec.next),
                spec.pending_effect_kind,
                spec.problem_code,
                next_revision,
                operation_id.as_str(),
                current.revision,
                disposition_name(spec.expected),
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
            EventSpec {
                revision: next_revision,
                event_kind: spec.event_kind,
                checkpoint: spec.checkpoint,
                disposition: spec.next,
                pending_effect_kind: spec.pending_effect_kind,
                problem_code: spec.problem_code,
            },
        )?;

        let record = load_operation(&transaction, operation_id.as_str())?
            .ok_or_else(|| JournalError::InvalidState("updated operation disappeared".into()))?;
        transaction.commit()?;
        Ok(record)
    }
    fn transition_admission(
        &self,
        operation_id: &OperationId,
        spec: TransitionSpec<'_>,
        mark_processing: bool,
        release_reservation: bool,
    ) -> Result<AdmissionRecord, JournalError> {
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let current =
            load_admission_record(&transaction, operation_id.as_str())?.ok_or_else(|| {
                JournalError::InvalidState(format!(
                    "admission operation {} does not exist",
                    operation_id
                ))
            })?;

        if current.disposition == spec.next
            && current.checkpoint == spec.checkpoint
            && current.pending_effect_kind.as_deref() == spec.pending_effect_kind
            && current.problem_code.as_deref() == spec.problem_code
        {
            transaction.commit()?;
            return Ok(current);
        }

        if current.disposition != spec.expected {
            return Err(JournalError::InvalidTransition(format!(
                "{} cannot move admission from {} to {}",
                operation_id,
                disposition_name(current.disposition),
                disposition_name(spec.next)
            )));
        }

        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| JournalError::InvalidState("operation revision overflow".into()))?;
        let finished_sql = if spec.finished {
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
                 WHERE operation_id = ?6
                   AND command_kind = 'admission.add'
                   AND revision = ?7
                   AND disposition = ?8"
            ),
            params![
                spec.checkpoint,
                disposition_name(spec.next),
                spec.pending_effect_kind,
                spec.problem_code,
                next_revision,
                operation_id.as_str(),
                current.revision,
                disposition_name(spec.expected),
            ],
        )?;
        if changed != 1 {
            return Err(JournalError::InvalidTransition(format!(
                "{} changed while admission transition was being committed",
                operation_id
            )));
        }

        if mark_processing {
            let changed = transaction.execute(
                "UPDATE torrent_registry
                 SET state = 'processing',
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE registry_id = ?1
                   AND operation_id = ?2
                   AND state = 'incoming'",
                params![current.registry_id, operation_id.as_str()],
            )?;
            if changed != 1 {
                return Err(JournalError::InvalidState(
                    "admission receipt could not advance registry to processing".into(),
                ));
            }
        }

        if release_reservation {
            let changed = transaction.execute(
                "UPDATE admission_reservations
                 SET reservation_state = 'released',
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE operation_id = ?1
                   AND reservation_state = 'active'",
                [operation_id.as_str()],
            )?;
            if changed != 1 {
                return Err(JournalError::InvalidState(
                    "admission failure could not release capacity reservation".into(),
                ));
            }

            let changed = transaction.execute(
                "UPDATE torrent_registry
                 SET operation_id = NULL,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE registry_id = ?1
                   AND operation_id = ?2
                   AND state = 'incoming'",
                params![current.registry_id, operation_id.as_str()],
            )?;
            if changed != 1 {
                return Err(JournalError::InvalidState(
                    "failed admission could not detach incoming registry".into(),
                ));
            }
        }

        insert_event(
            &transaction,
            operation_id.as_str(),
            EventSpec {
                revision: next_revision,
                event_kind: spec.event_kind,
                checkpoint: spec.checkpoint,
                disposition: spec.next,
                pending_effect_kind: spec.pending_effect_kind,
                problem_code: spec.problem_code,
            },
        )?;

        let record =
            load_admission_record(&transaction, operation_id.as_str())?.ok_or_else(|| {
                JournalError::InvalidState("updated admission operation disappeared".into())
            })?;
        transaction.commit()?;
        Ok(record)
    }

    fn apply_queue_target_inner(
        &self,
        operation_id: &OperationId,
        target_client_count: u32,
    ) -> Result<MutationRecord, JournalError> {
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = load_operation(&transaction, operation_id.as_str())?.ok_or_else(|| {
            JournalError::InvalidState(format!("operation {} does not exist", operation_id))
        })?;

        if current.disposition == MutationDisposition::Finished {
            return Ok(current);
        }

        match &current.command {
            MutationCommand::SetQueueTarget {
                target_client_count: expected,
            } if *expected == target_client_count => {}
            _ => {
                return Err(JournalError::InvalidTransition(format!(
                    "{} is not the matching queue-target operation",
                    operation_id
                )));
            }
        }

        if current.disposition != MutationDisposition::Prepared {
            return Err(JournalError::InvalidTransition(format!(
                "{} cannot apply queue target from {}",
                operation_id,
                disposition_name(current.disposition)
            )));
        }

        transaction.execute(
            "UPDATE controller_policy
             SET target_client_count = ?1,
                 revision = revision + 1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE singleton = 1",
            [i64::from(target_client_count)],
        )?;

        let next_revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| JournalError::InvalidState("operation revision overflow".into()))?;
        let changed = transaction.execute(
            "UPDATE operations
             SET checkpoint = 'finished',
                 disposition = 'finished',
                 pending_effect_kind = NULL,
                 problem_code = NULL,
                 revision = ?1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?2 AND revision = ?3 AND disposition = 'prepared'",
            params![next_revision, operation_id.as_str(), current.revision],
        )?;
        if changed != 1 {
            return Err(JournalError::InvalidTransition(format!(
                "{} changed while queue target was being committed",
                operation_id
            )));
        }

        insert_event(
            &transaction,
            operation_id.as_str(),
            EventSpec {
                revision: next_revision,
                event_kind: "finished",
                checkpoint: "finished",
                disposition: MutationDisposition::Finished,
                pending_effect_kind: None,
                problem_code: None,
            },
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

impl TorrentRegistry for Journal {
    fn find_by_identity(
        &self,
        identity: &TorrentIdentity,
    ) -> Result<Option<RegistryRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let ids = matching_registry_ids(&connection, identity).map_err(map_port_error)?;
        match ids.as_slice() {
            [] => Ok(None),
            [registry_id] => load_registry_record(&connection, registry_id).map_err(map_port_error),
            _ => Err(PortError::new(
                "IDENTITY_CONFLICT",
                "torrent identity aliases resolve to multiple registry records",
            )),
        }
    }

    fn get_by_id(&self, registry_id: &str) -> Result<Option<RegistryRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        load_registry_record(&connection, registry_id).map_err(map_port_error)
    }

    fn register_incoming(
        &self,
        candidate: &RegisterIncoming,
    ) -> Result<RegisterIncomingResult, PortError> {
        if candidate.source_relative.is_empty() {
            return Err(PortError::new(
                "PATH_POLICY_VIOLATION",
                "registry source path must not be empty",
            ));
        }

        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let ids =
            matching_registry_ids(&transaction, &candidate.identity).map_err(map_port_error)?;

        let result = match ids.as_slice() {
            [] => {
                let registry_id = Uuid::new_v4().to_string();
                transaction
                    .execute(
                        "INSERT INTO torrent_registry(
                            registry_id,
                            canonical_identity,
                            state,
                            source_relative,
                            source_metainfo_digest,
                            operation_id,
                            archive_ref,
                            handoff_file_count,
                            handoff_receipt_count,
                            updated_at
                         ) VALUES (
                            ?1, ?2, 'incoming', ?3, ?4, NULL, NULL, 0, 0,
                            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                         )",
                        params![
                            registry_id,
                            canonical_identity(&candidate.identity),
                            candidate.source_relative,
                            candidate.source_metainfo_digest.as_slice(),
                        ],
                    )
                    .map_err(JournalError::from)
                    .map_err(map_port_error)?;
                insert_identity_aliases(&transaction, &registry_id, &candidate.identity)
                    .map_err(map_port_error)?;
                let record = load_registry_record(&transaction, &registry_id)
                    .map_err(map_port_error)?
                    .ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "new registry record disappeared before commit",
                        )
                    })?;
                RegisterIncomingResult::Registered(record)
            }
            [registry_id] => {
                let existing = load_registry_record(&transaction, registry_id)
                    .map_err(map_port_error)?
                    .ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "registry alias references a missing record",
                        )
                    })?;
                if existing.source_metainfo_digest != candidate.source_metainfo_digest {
                    return Err(PortError::new(
                        "IDENTITY_CONFLICT",
                        "same torrent identity is represented by different metainfo bytes",
                    ));
                }

                insert_identity_aliases(&transaction, registry_id, &candidate.identity)
                    .map_err(map_port_error)?;
                let record = load_registry_record(&transaction, registry_id)
                    .map_err(map_port_error)?
                    .ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "registry record disappeared after alias update",
                        )
                    })?;
                transaction
                    .execute(
                        "UPDATE torrent_registry
                         SET canonical_identity = ?1,
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                         WHERE registry_id = ?2",
                        params![canonical_identity(&record.identity), registry_id],
                    )
                    .map_err(JournalError::from)
                    .map_err(map_port_error)?;
                RegisterIncomingResult::AlreadyPresent(record)
            }
            _ => {
                return Err(PortError::new(
                    "IDENTITY_CONFLICT",
                    "torrent identity aliases resolve to multiple registry records",
                ));
            }
        };

        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        Ok(result)
    }

    fn list_processing(&self) -> Result<Vec<RegistryRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT registry_id
                 FROM torrent_registry
                 WHERE state = 'processing'
                 ORDER BY source_relative COLLATE NOCASE, source_relative, registry_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let mut records = Vec::new();
        for row in rows {
            let registry_id = row.map_err(JournalError::from).map_err(map_port_error)?;
            let record = load_registry_record(&connection, &registry_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "processing registry record disappeared during enumeration",
                    )
                })?;
            records.push(record);
        }
        Ok(records)
    }
}

impl IncomingCleanupJournal for Journal {
    fn reserve_cleanup(
        &self,
        intent: &IncomingCleanupIntent,
    ) -> Result<IncomingCleanupReservation, PortError> {
        if intent.canonical_path.is_empty()
            || intent.redundant_path.is_empty()
            || intent.canonical_path == intent.redundant_path
        {
            return Err(PortError::new(
                "CLEANUP_REQUEST_INVALID",
                "cleanup paths must be non-empty and distinct",
            ));
        }

        let fingerprint = intent.fingerprint();
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let existing_id: Option<String> = transaction
            .query_row(
                "SELECT cleanup_id
                 FROM incoming_cleanup_intents
                 WHERE fingerprint_version = ?1
                   AND cleanup_fingerprint = ?2",
                params![INCOMING_CLEANUP_FINGERPRINT_VERSION, fingerprint.as_slice()],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some(cleanup_id) = existing_id {
            let record = load_cleanup_record(&transaction, &cleanup_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "cleanup fingerprint references a missing intent",
                    )
                })?;
            transaction
                .commit()
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            return Ok(IncomingCleanupReservation::Replay(record));
        }

        let conflicting: Option<String> = transaction
            .query_row(
                "SELECT cleanup_id
                 FROM incoming_cleanup_intents
                 WHERE redundant_path = ?1
                   AND state = 'prepared'
                 LIMIT 1",
                [&intent.redundant_path],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if conflicting.is_some() {
            return Err(PortError::new(
                "SOURCE_AMBIGUOUS",
                "redundant Incoming path already has a different active cleanup intent",
            ));
        }

        let cleanup_id = Uuid::new_v4().to_string();
        transaction
            .execute(
                "INSERT INTO incoming_cleanup_intents(
                    cleanup_id,
                    fingerprint_version,
                    cleanup_fingerprint,
                    canonical_path,
                    canonical_volume_id,
                    canonical_file_id,
                    canonical_size,
                    canonical_modified_marker,
                    redundant_path,
                    redundant_volume_id,
                    redundant_file_id,
                    redundant_size,
                    redundant_modified_marker,
                    source_sha256,
                    state,
                    problem_code,
                    revision,
                    created_at,
                    updated_at,
                    finished_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                    ?11, ?12, ?13, ?14, 'prepared', NULL, 1,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    NULL
                 )",
                params![
                    cleanup_id,
                    INCOMING_CLEANUP_FINGERPRINT_VERSION,
                    fingerprint.as_slice(),
                    intent.canonical_path,
                    intent
                        .canonical_evidence
                        .identity
                        .volume_id
                        .to_be_bytes()
                        .as_slice(),
                    intent
                        .canonical_evidence
                        .identity
                        .file_id
                        .to_be_bytes()
                        .as_slice(),
                    intent.canonical_evidence.size.to_be_bytes().as_slice(),
                    intent
                        .canonical_evidence
                        .modified_marker
                        .to_be_bytes()
                        .as_slice(),
                    intent.redundant_path,
                    intent
                        .redundant_evidence
                        .identity
                        .volume_id
                        .to_be_bytes()
                        .as_slice(),
                    intent
                        .redundant_evidence
                        .identity
                        .file_id
                        .to_be_bytes()
                        .as_slice(),
                    intent.redundant_evidence.size.to_be_bytes().as_slice(),
                    intent
                        .redundant_evidence
                        .modified_marker
                        .to_be_bytes()
                        .as_slice(),
                    intent.source_sha256.as_slice(),
                ],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        insert_cleanup_event(&transaction, &cleanup_id, 1, "prepared", None)
            .map_err(map_port_error)?;

        let record = load_cleanup_record(&transaction, &cleanup_id)
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "new cleanup intent disappeared before commit",
                )
            })?;
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        Ok(IncomingCleanupReservation::New(record))
    }

    fn list_recoverable_cleanups(&self) -> Result<Vec<IncomingCleanupRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT cleanup_id
                 FROM incoming_cleanup_intents
                 WHERE state = 'prepared'
                 ORDER BY created_at, cleanup_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let mut records = Vec::new();
        for row in rows {
            let cleanup_id = row.map_err(JournalError::from).map_err(map_port_error)?;
            let record = load_cleanup_record(&connection, &cleanup_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "recoverable cleanup intent disappeared",
                    )
                })?;
            records.push(record);
        }
        Ok(records)
    }

    fn mark_cleanup_deleted(&self, cleanup_id: &str) -> Result<IncomingCleanupRecord, PortError> {
        transition_cleanup(self, cleanup_id, IncomingCleanupState::Deleted, None)
    }

    fn mark_cleanup_blocked(
        &self,
        cleanup_id: &str,
        problem_code: &str,
    ) -> Result<IncomingCleanupRecord, PortError> {
        transition_cleanup(
            self,
            cleanup_id,
            IncomingCleanupState::Blocked,
            Some(problem_code),
        )
    }
}

impl AdmissionJournal for Journal {
    fn reserve_admission(
        &self,
        request: &AdmissionReservationRequest,
    ) -> Result<AdmissionReservationResult, PortError> {
        if request.source_relative.is_empty() || request.working_save_path.is_empty() {
            return Err(PortError::new(
                "ADMISSION_REQUEST_INVALID",
                "admission source and Working save path must not be empty",
            ));
        }

        let fingerprint = request.fingerprint();
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        if let Some(existing) =
            load_request(&transaction, request.request_id.as_str()).map_err(map_port_error)?
        {
            let operation_id = OperationId::new(existing.operation_id.clone())
                .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
            if existing.fingerprint_version != ADMISSION_FINGERPRINT_VERSION
                || existing.command_kind != "admission.add"
                || existing.command_fingerprint.as_slice() != fingerprint
            {
                return Ok(AdmissionReservationResult::Conflict { operation_id });
            }

            let record = load_admission_record(&transaction, operation_id.as_str())
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "admission request references a missing reservation",
                    )
                })?;
            transaction
                .commit()
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            return Ok(AdmissionReservationResult::Replay(record));
        }

        let registry_ids =
            matching_registry_ids(&transaction, &request.identity).map_err(map_port_error)?;
        let registry_id = match registry_ids.as_slice() {
            [] => {
                let registry_id = Uuid::new_v4().to_string();
                transaction
                    .execute(
                        "INSERT INTO torrent_registry(
                            registry_id,
                            canonical_identity,
                            state,
                            source_relative,
                            source_metainfo_digest,
                            operation_id,
                            archive_ref,
                            handoff_file_count,
                            handoff_receipt_count,
                            updated_at
                         ) VALUES (
                            ?1, ?2, 'incoming', ?3, ?4, NULL, NULL, 0, 0,
                            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                         )",
                        params![
                            registry_id,
                            canonical_identity(&request.identity),
                            request.source_relative,
                            request.source_metainfo_digest.as_slice(),
                        ],
                    )
                    .map_err(JournalError::from)
                    .map_err(map_port_error)?;
                insert_identity_aliases(&transaction, &registry_id, &request.identity)
                    .map_err(map_port_error)?;
                registry_id
            }
            [registry_id] => {
                let record = load_registry_record(&transaction, registry_id)
                    .map_err(map_port_error)?
                    .ok_or_else(|| {
                        PortError::new(
                            "JOURNAL_STATE_INVALID",
                            "registry alias references a missing record",
                        )
                    })?;
                if record.source_metainfo_digest != request.source_metainfo_digest {
                    return Err(PortError::new(
                        "IDENTITY_CONFLICT",
                        "same torrent identity is represented by different metainfo bytes",
                    ));
                }
                if record.state != RegistryState::Incoming {
                    return Err(PortError::new(
                        "ALREADY_PROCESSED",
                        "torrent identity is already processing or finished",
                    ));
                }
                if record.operation_id.is_some() {
                    return Err(PortError::new(
                        "IDENTITY_CONFLICT",
                        "torrent identity is already linked to another admission operation",
                    ));
                }

                insert_identity_aliases(&transaction, registry_id, &request.identity)
                    .map_err(map_port_error)?;
                transaction
                    .execute(
                        "UPDATE torrent_registry
                         SET canonical_identity = ?1,
                             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                         WHERE registry_id = ?2",
                        params![canonical_identity(&request.identity), registry_id],
                    )
                    .map_err(JournalError::from)
                    .map_err(map_port_error)?;
                registry_id.clone()
            }
            _ => {
                return Err(PortError::new(
                    "IDENTITY_CONFLICT",
                    "torrent identity aliases resolve to multiple registry records",
                ));
            }
        };

        let retained_capacity: Option<(String, Vec<u8>, Vec<u8>, String)> = transaction
            .query_row(
                "SELECT admission_operation_id, working_volume_id, retained_bytes, working_save_path
                 FROM retained_capacity
                 WHERE registry_id = ?1",
                [&registry_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some((_, retained_volume, retained_bytes, retained_path)) = &retained_capacity {
            let retained_volume =
                decode_u64_blob(retained_volume, "working_volume_id").map_err(map_port_error)?;
            let retained_bytes =
                decode_u64_blob(retained_bytes, "retained_bytes").map_err(map_port_error)?;
            if retained_volume != request.working_volume_id
                || retained_bytes != request.reserved_bytes
                || !retained_path.eq_ignore_ascii_case(&request.working_save_path)
            {
                return Err(PortError::new(
                    "CAPACITY_OWNERSHIP_CONFLICT",
                    "retained Working capacity does not match the new admission reservation",
                ));
            }
        }

        let operation_id = OperationId::new(Uuid::new_v4().to_string())
            .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
        let now = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

        transaction
            .execute(
                &format!(
                    "INSERT INTO requests(
                        request_id, fingerprint_version, command_kind, command_fingerprint,
                        operation_id, created_at
                     ) VALUES (?1, ?2, 'admission.add', ?3, ?4, {now})"
                ),
                params![
                    request.request_id.as_str(),
                    ADMISSION_FINGERPRINT_VERSION,
                    fingerprint.as_slice(),
                    operation_id.as_str(),
                ],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        transaction
            .execute(
                &format!(
                    "INSERT INTO operations(
                        operation_id, request_id, command_kind,
                        torrent_id, control_action, target_client_count, max_active_downloads,
                        download_limit_bps, upload_limit_bps,
                        checkpoint, disposition, pending_effect_kind, problem_code,
                        revision, created_at, updated_at
                     ) VALUES (
                        ?1, ?2, 'admission.add',
                        NULL, NULL, NULL, NULL, NULL, NULL,
                        'prepared', 'prepared', NULL, NULL, 1, {now}, {now}
                     )"
                ),
                params![operation_id.as_str(), request.request_id.as_str()],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        insert_event(
            &transaction,
            operation_id.as_str(),
            EventSpec {
                revision: 1,
                event_kind: "prepared",
                checkpoint: "prepared",
                disposition: MutationDisposition::Prepared,
                pending_effect_kind: None,
                problem_code: None,
            },
        )
        .map_err(map_port_error)?;

        transaction
            .execute(
                &format!(
                    "INSERT INTO admission_reservations(
                        operation_id,
                        registry_id,
                        source_relative,
                        source_volume_id,
                        source_file_id,
                        source_size,
                        source_modified_marker,
                        source_metainfo_digest,
                        working_volume_id,
                        reserved_bytes,
                        working_save_path,
                        reservation_state,
                        created_at,
                        updated_at
                     ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                        'active', {now}, {now}
                     )"
                ),
                params![
                    operation_id.as_str(),
                    registry_id,
                    request.source_relative,
                    request
                        .source_evidence
                        .identity
                        .volume_id
                        .to_be_bytes()
                        .as_slice(),
                    request
                        .source_evidence
                        .identity
                        .file_id
                        .to_be_bytes()
                        .as_slice(),
                    request.source_evidence.size.to_be_bytes().as_slice(),
                    request
                        .source_evidence
                        .modified_marker
                        .to_be_bytes()
                        .as_slice(),
                    request.source_metainfo_digest.as_slice(),
                    request.working_volume_id.to_be_bytes().as_slice(),
                    request.reserved_bytes.to_be_bytes().as_slice(),
                    request.working_save_path,
                ],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        if retained_capacity.is_some() {
            let changed = transaction
                .execute(
                    "DELETE FROM retained_capacity
                     WHERE registry_id = ?1",
                    [&registry_id],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            if changed != 1 {
                return Err(PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "retained capacity changed during admission ownership transfer",
                ));
            }
        }

        let changed = transaction
            .execute(
                "UPDATE torrent_registry
                 SET operation_id = ?1,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE registry_id = ?2
                   AND state = 'incoming'
                   AND operation_id IS NULL",
                params![operation_id.as_str(), registry_id],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if changed != 1 {
            return Err(PortError::new(
                "JOURNAL_STATE_INVALID",
                "registry could not be atomically attached to admission operation",
            ));
        }

        let record = load_admission_record(&transaction, operation_id.as_str())
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "new admission reservation disappeared before commit",
                )
            })?;
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        Ok(AdmissionReservationResult::New(record))
    }

    fn get_admission(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<AdmissionRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        load_admission_record(&connection, operation_id.as_str()).map_err(map_port_error)
    }

    fn list_recoverable_admissions(&self) -> Result<Vec<AdmissionRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT operation_id
                 FROM operations
                 WHERE command_kind = 'admission.add'
                   AND disposition IN ('prepared','effect_pending','observed_applied','unknown')
                 ORDER BY created_at, operation_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let mut records = Vec::new();
        for row in rows {
            let operation_id = row.map_err(JournalError::from).map_err(map_port_error)?;
            let record = load_admission_record(&connection, &operation_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "recoverable admission operation has no reservation detail",
                    )
                })?;
            records.push(record);
        }
        Ok(records)
    }

    fn capacity_reservations(
        &self,
        working_volume_id: u64,
    ) -> Result<Vec<CapacityReservation>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let volume = working_volume_id.to_be_bytes();
        let mut statement = connection
            .prepare(
                "SELECT responsible, bytes
                 FROM (
                    SELECT operation_id AS responsible,
                           reserved_bytes AS bytes
                    FROM admission_reservations
                    WHERE working_volume_id = ?1
                      AND reservation_state = 'active'
                    UNION ALL
                    SELECT 'retained:' || source_relative AS responsible,
                           retained_bytes AS bytes
                    FROM retained_capacity
                    WHERE working_volume_id = ?1
                 )
                 ORDER BY responsible",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([volume.as_slice()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let mut reservations = Vec::new();
        for row in rows {
            let (responsible, bytes) = row.map_err(JournalError::from).map_err(map_port_error)?;
            reservations.push(CapacityReservation {
                responsible,
                volume_id: working_volume_id,
                bytes: decode_u64_blob(&bytes, "reserved_bytes").map_err(map_port_error)?,
            });
        }
        Ok(reservations)
    }

    fn mark_admission_effect_pending(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError> {
        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::Prepared,
                next: MutationDisposition::EffectPending,
                checkpoint: "effect_pending",
                event_kind: "effect_pending",
                pending_effect_kind: Some("qbit.add"),
                problem_code: None,
                finished: false,
            },
            false,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_admission_not_submitted(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError> {
        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::Prepared,
                next: MutationDisposition::Blocked,
                checkpoint: "not_submitted",
                event_kind: "not_submitted",
                pending_effect_kind: None,
                problem_code: Some(problem_code),
                finished: false,
            },
            false,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_admission_retry_ready(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError> {
        let current = self
            .get_admission(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let (expected, checkpoint) = match current.disposition {
            MutationDisposition::Blocked => (MutationDisposition::Blocked, "retry_ready"),
            MutationDisposition::EffectPending => {
                (MutationDisposition::EffectPending, "observed_not_applied")
            }
            MutationDisposition::Unknown => (MutationDisposition::Unknown, "observed_not_applied"),
            other => {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    format!(
                        "{} cannot become admission retry-ready from {}",
                        operation_id,
                        disposition_name(other)
                    ),
                ));
            }
        };

        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected,
                next: MutationDisposition::Prepared,
                checkpoint,
                event_kind: "retry_ready",
                pending_effect_kind: None,
                problem_code: None,
                finished: false,
            },
            false,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_admission_unknown(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError> {
        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::EffectPending,
                next: MutationDisposition::Unknown,
                checkpoint: "unknown",
                event_kind: "unknown",
                pending_effect_kind: Some("qbit.add"),
                problem_code: Some(problem_code),
                finished: false,
            },
            false,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_admission_observed_applied(
        &self,
        operation_id: &OperationId,
    ) -> Result<AdmissionRecord, PortError> {
        let current = self
            .get_admission(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let expected = match current.disposition {
            MutationDisposition::EffectPending | MutationDisposition::Unknown => {
                current.disposition
            }
            other => {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    format!(
                        "{} cannot record admission receipt from {}",
                        operation_id,
                        disposition_name(other)
                    ),
                ));
            }
        };

        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected,
                next: MutationDisposition::ObservedApplied,
                checkpoint: "observed_applied",
                event_kind: "observed_applied",
                pending_effect_kind: Some("qbit.add"),
                problem_code: None,
                finished: false,
            },
            true,
            false,
        )
        .map_err(map_port_error)
    }

    fn finish_admission(&self, operation_id: &OperationId) -> Result<AdmissionRecord, PortError> {
        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::ObservedApplied,
                next: MutationDisposition::Finished,
                checkpoint: "finished",
                event_kind: "finished",
                pending_effect_kind: None,
                problem_code: None,
                finished: true,
            },
            false,
            false,
        )
        .map_err(map_port_error)
    }

    fn mark_admission_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<AdmissionRecord, PortError> {
        self.transition_admission(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::EffectPending,
                next: MutationDisposition::Failed,
                checkpoint: "failed",
                event_kind: "failed",
                pending_effect_kind: Some("qbit.add"),
                problem_code: Some(problem_code),
                finished: true,
            },
            false,
            true,
        )
        .map_err(map_port_error)
    }
}

impl CompletionJournal for Journal {
    fn lookup_completion_request(
        &self,
        request: &qb_application::completion::CompletionRequest,
    ) -> Result<Option<CompletionReservation>, PortError> {
        let fingerprint = request.fingerprint();
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let existing: Option<(String, u32, Vec<u8>)> = connection
            .query_row(
                "SELECT operation_id, fingerprint_version, completion_fingerprint
                 FROM completion_operations
                 WHERE request_id = ?1",
                [request.request_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let Some((operation_id, fingerprint_version, stored_fingerprint)) = existing else {
            return Ok(None);
        };
        let operation_id = OperationId::new(operation_id)
            .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
        if fingerprint_version != COMPLETION_FINGERPRINT_VERSION
            || stored_fingerprint.as_slice() != fingerprint
        {
            return Ok(Some(CompletionReservation::Conflict { operation_id }));
        }
        let record = load_completion_record(&connection, operation_id.as_str())
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "completion request references a missing operation",
                )
            })?;
        Ok(Some(CompletionReservation::Replay(record)))
    }

    fn reserve_completion(
        &self,
        preflight: &CompletionPreflight,
    ) -> Result<CompletionReservation, PortError> {
        if preflight.registry_id.trim().is_empty()
            || preflight.source_relative.trim().is_empty()
            || preflight.working_save_path.trim().is_empty()
            || preflight.files.is_empty()
        {
            return Err(PortError::new(
                "COMPLETION_REQUEST_INVALID",
                "completion preflight must include registry, source, Working path and files",
            ));
        }

        let fingerprint = preflight.fingerprint();
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let existing: Option<(String, u32, Vec<u8>)> = transaction
            .query_row(
                "SELECT operation_id, fingerprint_version, completion_fingerprint
                 FROM completion_operations
                 WHERE request_id = ?1",
                [preflight.request_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some((operation_id, fingerprint_version, stored_fingerprint)) = existing {
            let operation_id = OperationId::new(operation_id)
                .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
            if fingerprint_version != COMPLETION_FINGERPRINT_VERSION
                || stored_fingerprint.as_slice() != fingerprint
            {
                return Ok(CompletionReservation::Conflict { operation_id });
            }
            let record = load_completion_record(&transaction, operation_id.as_str())
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "completion request references a missing operation",
                    )
                })?;
            transaction
                .commit()
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            return Ok(CompletionReservation::Replay(record));
        }

        let registry = load_registry_record(&transaction, &preflight.registry_id)
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "COMPLETION_TARGET_NOT_FOUND",
                    "completion registry record does not exist",
                )
            })?;
        if registry.state != RegistryState::Processing {
            return Err(PortError::new(
                "COMPLETION_NOT_PROCESSING",
                "completion requires a Processing registry record",
            ));
        }
        if registry.identity != preflight.identity
            || registry.source_relative != preflight.source_relative
            || registry.source_metainfo_digest != preflight.source_metainfo_digest
        {
            return Err(PortError::new(
                "COMPLETION_PREFLIGHT_STALE",
                "completion preflight no longer matches authoritative registry identity/source",
            ));
        }
        if registry.archive_ref.is_some() || registry.handoff_receipt_count != 0 {
            return Err(PortError::new(
                "COMPLETION_ALREADY_STARTED",
                "registry already contains completion handoff evidence",
            ));
        }

        let active: Option<String> = transaction
            .query_row(
                "SELECT operation_id
                 FROM completion_operations
                 WHERE registry_id = ?1
                   AND state NOT IN ('finished','blocked','failed')
                 LIMIT 1",
                [&preflight.registry_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some(operation_id) = active {
            let operation_id = OperationId::new(operation_id)
                .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
            return Ok(CompletionReservation::ActiveConflict { operation_id });
        }

        let operation_id = OperationId::new(Uuid::new_v4().to_string())
            .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
        let identity_v1 = preflight.identity.v1.map(|value| value.to_vec());
        let identity_v2 = preflight.identity.v2.map(|value| value.to_vec());

        transaction
            .execute(
                "INSERT INTO completion_operations(
                    operation_id,
                    request_id,
                    fingerprint_version,
                    completion_fingerprint,
                    registry_id,
                    torrent_id,
                    identity_v1,
                    identity_v2,
                    source_relative,
                    source_volume_id,
                    source_file_id,
                    source_size,
                    source_modified_marker,
                    source_metainfo_digest,
                    working_volume_id,
                    completed_volume_id,
                    archive_volume_id,
                    working_save_path,
                    total_bytes,
                    payload_strategy,
                    archive_strategy,
                    state,
                    problem_code,
                    revision,
                    created_at,
                    updated_at,
                    finished_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                    ?18, ?19, ?20, ?21, 'prepared', NULL, 1,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    NULL
                 )",
                params![
                    operation_id.as_str(),
                    preflight.request_id.as_str(),
                    COMPLETION_FINGERPRINT_VERSION,
                    fingerprint.as_slice(),
                    preflight.registry_id,
                    preflight.torrent_id.as_str(),
                    identity_v1,
                    identity_v2,
                    preflight.source_relative,
                    preflight
                        .source_evidence
                        .identity
                        .volume_id
                        .to_be_bytes()
                        .as_slice(),
                    preflight
                        .source_evidence
                        .identity
                        .file_id
                        .to_be_bytes()
                        .as_slice(),
                    preflight.source_evidence.size.to_be_bytes().as_slice(),
                    preflight
                        .source_evidence
                        .modified_marker
                        .to_be_bytes()
                        .as_slice(),
                    preflight.source_metainfo_digest.as_slice(),
                    preflight.working_volume_id.to_be_bytes().as_slice(),
                    preflight.completed_volume_id.to_be_bytes().as_slice(),
                    preflight.archive_volume_id.to_be_bytes().as_slice(),
                    preflight.working_save_path,
                    preflight.total_bytes.to_be_bytes().as_slice(),
                    completion_strategy_name(preflight.payload_strategy()),
                    completion_strategy_name(preflight.archive_strategy()),
                ],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        for (index, file) in preflight.files.iter().enumerate() {
            let file_index = u32::try_from(index).map_err(|_| {
                PortError::new(
                    "COMPLETION_REQUEST_INVALID",
                    "completion manifest has more files than supported by the journal",
                )
            })?;
            transaction
                .execute(
                    "INSERT INTO operation_files(
                        operation_id,
                        file_index,
                        relative_path,
                        expected_size,
                        source_volume_id,
                        source_file_id,
                        source_size,
                        source_modified_marker,
                        handoff_strategy,
                        state,
                        temp_relative,
                        destination_volume_id,
                        destination_file_id,
                        destination_size,
                        destination_modified_marker,
                        destination_sha256,
                        problem_code,
                        revision,
                        created_at,
                        updated_at
                     ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                        'prepared', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     )",
                    params![
                        operation_id.as_str(),
                        i64::from(file_index),
                        file.relative_path,
                        file.size.to_be_bytes().as_slice(),
                        file.source_evidence
                            .identity
                            .volume_id
                            .to_be_bytes()
                            .as_slice(),
                        file.source_evidence
                            .identity
                            .file_id
                            .to_be_bytes()
                            .as_slice(),
                        file.source_evidence.size.to_be_bytes().as_slice(),
                        file.source_evidence
                            .modified_marker
                            .to_be_bytes()
                            .as_slice(),
                        completion_strategy_name(preflight.payload_strategy()),
                    ],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
        }

        insert_completion_event(&transaction, operation_id.as_str(), 1, "prepared", None)
            .map_err(map_port_error)?;

        let record = load_completion_record(&transaction, operation_id.as_str())
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "completion operation disappeared before commit",
                )
            })?;
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        Ok(CompletionReservation::New(record))
    }

    fn get_completion(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CompletionRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        load_completion_record(&connection, operation_id.as_str()).map_err(map_port_error)
    }

    fn list_recoverable_completions(&self) -> Result<Vec<CompletionRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT operation_id
                 FROM completion_operations
                 WHERE state NOT IN ('finished','blocked','failed')
                 ORDER BY created_at, operation_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let mut records = Vec::new();
        for row in rows {
            let operation_id = row.map_err(JournalError::from).map_err(map_port_error)?;
            let record = load_completion_record(&connection, &operation_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "recoverable completion disappeared during enumeration",
                    )
                })?;
            records.push(record);
        }
        Ok(records)
    }

    fn mark_stop_pending(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::Prepared],
            CompletionState::StopPending,
            None,
        )
    }

    fn mark_unknown_stop(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::StopPending],
            CompletionState::UnknownStop,
            Some(problem_code),
        )
    }

    fn retry_stop(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::UnknownStop, CompletionState::StopPending],
            CompletionState::Prepared,
            None,
        )
    }

    fn mark_stopped(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::StopPending, CompletionState::UnknownStop],
            CompletionState::Stopped,
            None,
        )
    }

    fn mark_completion_blocked(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[
                CompletionState::Prepared,
                CompletionState::StopPending,
                CompletionState::UnknownStop,
                CompletionState::Stopped,
                CompletionState::ArchivePending,
                CompletionState::UnknownArchive,
                CompletionState::PayloadPending,
                CompletionState::RemoveRecordPending,
                CompletionState::UnknownRemoveRecord,
            ],
            CompletionState::Blocked,
            Some(problem_code),
        )
    }

    fn mark_completion_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[
                CompletionState::StopPending,
                CompletionState::RemoveRecordPending,
            ],
            CompletionState::Failed,
            Some(problem_code),
        )
    }

    fn mark_archive_pending(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::Stopped],
            CompletionState::ArchivePending,
            None,
        )
    }

    fn mark_unknown_archive(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::ArchivePending],
            CompletionState::UnknownArchive,
            Some(problem_code),
        )
    }

    fn retry_archive(&self, operation_id: &OperationId) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[
                CompletionState::ArchivePending,
                CompletionState::UnknownArchive,
            ],
            CompletionState::Stopped,
            None,
        )
    }

    fn mark_archive_destination_receipted(
        &self,
        operation_id: &OperationId,
        destination: &qb_application::storage::FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError> {
        receipt_completion_archive_destination(
            self,
            operation_id,
            destination,
            destination_sha256,
        )
    }

    fn mark_archive_receipted(
        &self,
        operation_id: &OperationId,
        destination: &qb_application::storage::FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError> {
        receipt_completion_archive(self, operation_id, destination, destination_sha256)
    }

    fn mark_file_move_pending(
        &self,
        operation_id: &OperationId,
        file_index: u32,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::MovePending,
            None,
            None,
            None,
        )
    }

    fn mark_file_unknown_move(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::UnknownMove,
            None,
            None,
            Some(problem_code),
        )
    }

    fn mark_file_destination_receipted(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        destination: &qb_application::storage::FileEvidence,
        destination_sha256: [u8; 32],
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::DestinationReceipted,
            Some(destination),
            Some(destination_sha256),
            None,
        )
    }

    fn mark_file_source_delete_pending(
        &self,
        operation_id: &OperationId,
        file_index: u32,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::SourceDeletePending,
            None,
            None,
            None,
        )
    }

    fn mark_file_unknown_source_delete(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::UnknownSourceDelete,
            None,
            None,
            Some(problem_code),
        )
    }

    fn mark_file_handed_off(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        destination: &qb_application::storage::FileEvidence,
        destination_sha256: Option<[u8; 32]>,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::HandedOff,
            Some(destination),
            destination_sha256,
            None,
        )
    }

    fn mark_file_blocked(
        &self,
        operation_id: &OperationId,
        file_index: u32,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion_file(
            self,
            operation_id,
            file_index,
            CompletionFileState::Blocked,
            None,
            None,
            Some(problem_code),
        )
    }

    fn mark_payload_handed_off(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError> {
        finish_completion_payload(self, operation_id)
    }

    fn mark_unknown_remove_record(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::RemoveRecordPending],
            CompletionState::UnknownRemoveRecord,
            Some(problem_code),
        )
    }

    fn retry_remove_record(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError> {
        transition_completion(
            self,
            operation_id,
            &[CompletionState::UnknownRemoveRecord],
            CompletionState::RemoveRecordPending,
            None,
        )
    }

    fn finish_completion(
        &self,
        operation_id: &OperationId,
    ) -> Result<CompletionRecord, PortError> {
        finish_completion_operation(self, operation_id)
    }
}

impl ReleaseJournal for Journal {
    fn reserve_release(&self, request: &ReleaseRequest) -> Result<ReleaseReservation, PortError> {
        if request.registry_id.trim().is_empty() {
            return Err(PortError::new(
                "RELEASE_REQUEST_INVALID",
                "release registry id must not be empty",
            ));
        }

        let fingerprint = request.fingerprint();
        let mut connection = self.connection.lock().expect("journal mutex poisoned");
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let existing: Option<(String, u32, Vec<u8>)> = transaction
            .query_row(
                "SELECT operation_id, fingerprint_version, release_fingerprint
                 FROM release_operations
                 WHERE request_id = ?1",
                [request.request_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some((operation_id, fingerprint_version, stored_fingerprint)) = existing {
            let operation_id = OperationId::new(operation_id)
                .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
            if fingerprint_version != RELEASE_FINGERPRINT_VERSION
                || stored_fingerprint.as_slice() != fingerprint
            {
                return Ok(ReleaseReservation::Conflict { operation_id });
            }
            let record = load_release_record(&transaction, operation_id.as_str())
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "release request references a missing operation",
                    )
                })?;
            transaction
                .commit()
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            return Ok(ReleaseReservation::Replay(record));
        }

        let registry = load_registry_record(&transaction, &request.registry_id)
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "RELEASE_TARGET_NOT_FOUND",
                    "release registry record does not exist",
                )
            })?;
        if registry.state != RegistryState::Processing {
            return Err(PortError::new(
                "RELEASE_TARGET_NOT_PROCESSING",
                "only a processing registry record can be released from qBittorrent",
            ));
        }
        let admission_operation_id = registry
            .operation_id
            .as_deref()
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "processing registry record has no admission operation",
                )
            })
            .and_then(|value| {
                OperationId::new(value.to_owned())
                    .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))
            })?;

        let active_release: Option<String> = transaction
            .query_row(
                "SELECT operation_id
                 FROM release_operations
                 WHERE registry_id = ?1
                   AND state IN (
                       'prepared','stop_pending','stopped','unknown_stop',
                       'delete_pending','unknown_delete'
                   )
                 LIMIT 1",
                [&request.registry_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some(operation_id) = active_release {
            let operation_id = OperationId::new(operation_id)
                .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
            return Ok(ReleaseReservation::Conflict { operation_id });
        }

        let admission = load_admission_record(&transaction, admission_operation_id.as_str())
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "release target admission record does not exist",
                )
            })?;
        if admission.disposition != MutationDisposition::Finished {
            return Err(PortError::new(
                "RELEASE_ADMISSION_NOT_FINISHED",
                "release requires a finished admission operation",
            ));
        }

        let retained: Option<(String, Vec<u8>, Vec<u8>, String)> = transaction
            .query_row(
                "SELECT admission_operation_id, working_volume_id, retained_bytes, working_save_path
                 FROM retained_capacity
                 WHERE registry_id = ?1",
                [&request.registry_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if let Some((owner_admission, volume, bytes, save_path)) = &retained {
            if owner_admission != admission_operation_id.as_str()
                || decode_u64_blob(volume, "working_volume_id").map_err(map_port_error)?
                    != admission.working_volume_id
                || decode_u64_blob(bytes, "retained_bytes").map_err(map_port_error)?
                    != admission.reserved_bytes
                || !save_path.eq_ignore_ascii_case(&admission.working_save_path)
            {
                return Err(PortError::new(
                    "CAPACITY_OWNERSHIP_CONFLICT",
                    "existing retained capacity does not match the release target admission",
                ));
            }
        } else if !admission.reservation_active {
            return Err(PortError::new(
                "CAPACITY_OWNERSHIP_MISSING",
                "release target has neither active admission nor retained capacity ownership",
            ));
        }

        let operation_id = OperationId::new(Uuid::new_v4().to_string())
            .map_err(|error| PortError::new("JOURNAL_STATE_INVALID", error.to_string()))?;
        let v1 = admission.identity.v1.map(|value| value.to_vec());
        let v2 = admission.identity.v2.map(|value| value.to_vec());
        transaction
            .execute(
                "INSERT INTO release_operations(
                    operation_id,
                    request_id,
                    fingerprint_version,
                    release_fingerprint,
                    registry_id,
                    admission_operation_id,
                    identity_v1,
                    identity_v2,
                    source_relative,
                    source_volume_id,
                    source_file_id,
                    source_size,
                    source_modified_marker,
                    source_metainfo_digest,
                    working_volume_id,
                    retained_bytes,
                    working_save_path,
                    state,
                    resolution,
                    problem_code,
                    revision,
                    created_at,
                    updated_at,
                    finished_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                    ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                    'prepared', NULL, NULL, 1,
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    NULL
                 )",
                params![
                    operation_id.as_str(),
                    request.request_id.as_str(),
                    RELEASE_FINGERPRINT_VERSION,
                    fingerprint.as_slice(),
                    request.registry_id,
                    admission_operation_id.as_str(),
                    v1,
                    v2,
                    admission.source_relative,
                    admission
                        .source_evidence
                        .identity
                        .volume_id
                        .to_be_bytes()
                        .as_slice(),
                    admission
                        .source_evidence
                        .identity
                        .file_id
                        .to_be_bytes()
                        .as_slice(),
                    admission.source_evidence.size.to_be_bytes().as_slice(),
                    admission
                        .source_evidence
                        .modified_marker
                        .to_be_bytes()
                        .as_slice(),
                    admission.source_metainfo_digest.as_slice(),
                    admission.working_volume_id.to_be_bytes().as_slice(),
                    admission.reserved_bytes.to_be_bytes().as_slice(),
                    admission.working_save_path,
                ],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        insert_release_event(&transaction, operation_id.as_str(), 1, "prepared", None)
            .map_err(map_port_error)?;

        if retained.is_some() {
            let changed = transaction
                .execute(
                    "UPDATE retained_capacity
                     SET release_operation_id = ?1,
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE registry_id = ?2
                       AND admission_operation_id = ?3",
                    params![
                        operation_id.as_str(),
                        request.registry_id,
                        admission_operation_id.as_str()
                    ],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            if changed != 1 {
                return Err(PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "retained capacity changed during release reservation",
                ));
            }
        } else {
            let changed = transaction
                .execute(
                    "UPDATE admission_reservations
                     SET reservation_state = 'released',
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE operation_id = ?1
                       AND reservation_state = 'active'",
                    [admission_operation_id.as_str()],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            if changed != 1 {
                return Err(PortError::new(
                    "CAPACITY_OWNERSHIP_CONFLICT",
                    "active admission capacity could not transfer to retained ownership",
                ));
            }
            transaction
                .execute(
                    "INSERT INTO retained_capacity(
                        registry_id,
                        admission_operation_id,
                        release_operation_id,
                        source_relative,
                        working_volume_id,
                        retained_bytes,
                        working_save_path,
                        created_at,
                        updated_at
                     ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     )",
                    params![
                        request.registry_id,
                        admission_operation_id.as_str(),
                        operation_id.as_str(),
                        admission.source_relative,
                        admission.working_volume_id.to_be_bytes().as_slice(),
                        admission.reserved_bytes.to_be_bytes().as_slice(),
                        admission.working_save_path,
                    ],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
        }

        let record = load_release_record(&transaction, operation_id.as_str())
            .map_err(map_port_error)?
            .ok_or_else(|| {
                PortError::new(
                    "JOURNAL_STATE_INVALID",
                    "new release operation disappeared before commit",
                )
            })?;
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        Ok(ReleaseReservation::New(record))
    }

    fn list_recoverable_releases(&self) -> Result<Vec<ReleaseRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT operation_id
                 FROM release_operations
                 WHERE state IN (
                    'prepared','stop_pending','stopped','unknown_stop',
                    'delete_pending','unknown_delete'
                 )
                 ORDER BY created_at, operation_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let mut records = Vec::new();
        for row in rows {
            let operation_id = row.map_err(JournalError::from).map_err(map_port_error)?;
            let record = load_release_record(&connection, &operation_id)
                .map_err(map_port_error)?
                .ok_or_else(|| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "recoverable release operation disappeared",
                    )
                })?;
            records.push(record);
        }
        Ok(records)
    }

    fn mark_stop_pending(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::Prepared],
            ReleaseState::StopPending,
            None,
        )
    }

    fn mark_stopped(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[
                ReleaseState::Prepared,
                ReleaseState::StopPending,
                ReleaseState::UnknownStop,
            ],
            ReleaseState::Stopped,
            None,
        )
    }

    fn mark_unknown_stop(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::StopPending],
            ReleaseState::UnknownStop,
            Some(problem_code),
        )
    }

    fn retry_stop(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::UnknownStop, ReleaseState::StopPending],
            ReleaseState::Prepared,
            None,
        )
    }

    fn mark_delete_pending(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::Stopped],
            ReleaseState::DeletePending,
            None,
        )
    }

    fn mark_unknown_delete(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::DeletePending],
            ReleaseState::UnknownDelete,
            Some(problem_code),
        )
    }

    fn retry_delete(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::UnknownDelete, ReleaseState::DeletePending],
            ReleaseState::Stopped,
            None,
        )
    }

    fn mark_release_blocked(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[
                ReleaseState::Prepared,
                ReleaseState::StopPending,
                ReleaseState::Stopped,
                ReleaseState::UnknownStop,
                ReleaseState::DeletePending,
                ReleaseState::UnknownDelete,
            ],
            ReleaseState::Blocked,
            Some(problem_code),
        )
    }

    fn mark_release_failed(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<ReleaseRecord, PortError> {
        transition_release(
            self,
            operation_id,
            &[ReleaseState::StopPending, ReleaseState::DeletePending],
            ReleaseState::Failed,
            Some(problem_code),
        )
    }

    fn finish_release(&self, operation_id: &OperationId) -> Result<ReleaseRecord, PortError> {
        finish_release_with_resolution(
            self,
            operation_id,
            &[ReleaseState::DeletePending, ReleaseState::UnknownDelete],
            ReleaseResolution::PreservedIncomplete,
            true,
        )
    }

    fn finish_became_complete(
        &self,
        operation_id: &OperationId,
    ) -> Result<ReleaseRecord, PortError> {
        finish_release_with_resolution(
            self,
            operation_id,
            &[
                ReleaseState::Prepared,
                ReleaseState::StopPending,
                ReleaseState::Stopped,
                ReleaseState::UnknownStop,
            ],
            ReleaseResolution::BecameComplete,
            false,
        )
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
            TransitionSpec {
                expected: MutationDisposition::Prepared,
                next: MutationDisposition::EffectPending,
                checkpoint: "effect_pending",
                event_kind: "effect_pending",
                pending_effect_kind: Some(effect_kind),
                problem_code: None,
                finished: false,
            },
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
        let expected = match current.disposition {
            MutationDisposition::EffectPending
            | MutationDisposition::Unknown
            | MutationDisposition::Blocked => current.disposition,
            other => {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    format!(
                        "{} cannot mark observed applied from {}",
                        operation_id,
                        disposition_name(other)
                    ),
                ));
            }
        };

        self.transition(
            operation_id,
            TransitionSpec {
                expected,
                next: MutationDisposition::ObservedApplied,
                checkpoint: "observed_applied",
                event_kind: "observed_applied",
                pending_effect_kind: effect.as_deref(),
                problem_code: None,
                finished: false,
            },
        )
        .map_err(map_port_error)
    }

    fn mark_retry_ready(&self, operation_id: &OperationId) -> Result<MutationRecord, PortError> {
        let current = self
            .get_operation(operation_id)?
            .ok_or_else(|| PortError::new("OPERATION_NOT_FOUND", operation_id.to_string()))?;
        let expected = match current.disposition {
            MutationDisposition::EffectPending
            | MutationDisposition::Blocked
            | MutationDisposition::Unknown => current.disposition,
            other => {
                return Err(PortError::new(
                    "OPERATION_TRANSITION_INVALID",
                    format!(
                        "{} cannot become retry-ready from {}",
                        operation_id,
                        disposition_name(other)
                    ),
                ));
            }
        };

        let checkpoint = if expected == MutationDisposition::Blocked {
            "retry_ready"
        } else {
            "observed_not_applied"
        };

        self.transition(
            operation_id,
            TransitionSpec {
                expected,
                next: MutationDisposition::Prepared,
                checkpoint,
                event_kind: "retry_ready",
                pending_effect_kind: None,
                problem_code: None,
                finished: false,
            },
        )
        .map_err(map_port_error)
    }

    fn finish(&self, operation_id: &OperationId) -> Result<MutationRecord, PortError> {
        self.transition(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::ObservedApplied,
                next: MutationDisposition::Finished,
                checkpoint: "finished",
                event_kind: "finished",
                pending_effect_kind: None,
                problem_code: None,
                finished: true,
            },
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
            TransitionSpec {
                expected: MutationDisposition::EffectPending,
                next: MutationDisposition::Unknown,
                checkpoint: "unknown",
                event_kind: "unknown",
                pending_effect_kind: effect.as_deref(),
                problem_code: Some(problem_code),
                finished: false,
            },
        )
        .map_err(map_port_error)
    }

    fn mark_blocked(
        &self,
        operation_id: &OperationId,
        problem_code: &str,
    ) -> Result<MutationRecord, PortError> {
        self.transition(
            operation_id,
            TransitionSpec {
                expected: MutationDisposition::Prepared,
                next: MutationDisposition::Blocked,
                checkpoint: "blocked",
                event_kind: "blocked",
                pending_effect_kind: None,
                problem_code: Some(problem_code),
                finished: false,
            },
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
            TransitionSpec {
                expected: MutationDisposition::EffectPending,
                next: MutationDisposition::Failed,
                checkpoint: "failed",
                event_kind: "failed",
                pending_effect_kind: effect.as_deref(),
                problem_code: Some(problem_code),
                finished: true,
            },
        )
        .map_err(map_port_error)
    }

    fn get_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<MutationRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        load_operation(&*connection, operation_id.as_str()).map_err(map_port_error)
    }

    fn list_recoverable(&self) -> Result<Vec<MutationRecord>, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let mut statement = connection
            .prepare(
                "SELECT operations.request_id,
                        operations.operation_id,
                        operations.command_kind,
                        requests.fingerprint_version,
                        requests.command_fingerprint,
                        operations.torrent_id,
                        operations.control_action,
                        operations.target_client_count,
                        operations.max_active_downloads,
                        operations.download_limit_bps,
                        operations.upload_limit_bps,
                        operations.checkpoint,
                        operations.disposition,
                        operations.pending_effect_kind,
                        operations.problem_code,
                        operations.revision
                 FROM operations
                 JOIN requests ON requests.request_id = operations.request_id
                 WHERE operations.disposition IN ('prepared','effect_pending','observed_applied','unknown')
                   AND operations.command_kind != 'admission.add'
                 ORDER BY operations.created_at, operations.operation_id",
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        let rows = statement
            .query_map([], map_operation_row)
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row.map_err(JournalError::from).map_err(map_port_error)?);
        }
        Ok(records)
    }

    fn queue_target(&self) -> Result<QueueTargetPolicy, PortError> {
        let connection = self.connection.lock().expect("journal mutex poisoned");
        let (revision, value): (i64, Option<i64>) = connection
            .query_row(
                "SELECT revision, target_client_count FROM controller_policy WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;

        let revision = u64::try_from(revision)
            .map_err(|_| PortError::new("JOURNAL_STATE_INVALID", "policy revision is negative"))?;
        let target_client_count = value
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    PortError::new(
                        "JOURNAL_STATE_INVALID",
                        "target_client_count is outside u32 range",
                    )
                })
            })
            .transpose()?;

        Ok(QueueTargetPolicy {
            revision,
            target_client_count,
        })
    }

    fn apply_queue_target(
        &self,
        operation_id: &OperationId,
        target_client_count: u32,
    ) -> Result<MutationRecord, PortError> {
        self.apply_queue_target_inner(operation_id, target_client_count)
            .map_err(map_port_error)
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

    if version == 2 {
        migrate_v2_to_v3(connection)?;
        version = 3;
    }

    if version == 3 {
        migrate_v3_to_v4(connection)?;
        version = 4;
    }

    if version == 4 {
        migrate_v4_to_v5(connection)?;
        version = 5;
    }

    if version == 5 {
        migrate_v5_to_v6(connection)?;
        version = 6;
    }

    if version == 6 {
        migrate_v6_to_v7(connection)?;
        version = 7;
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

fn migrate_v2_to_v3(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE torrent_registry (
            registry_id TEXT PRIMARY KEY,
            canonical_identity TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('incoming','processing','finished')),
            source_relative TEXT NOT NULL,
            source_metainfo_digest BLOB NOT NULL CHECK(length(source_metainfo_digest) = 32),
            operation_id TEXT REFERENCES operations(operation_id),
            archive_ref TEXT,
            handoff_file_count INTEGER NOT NULL DEFAULT 0 CHECK(handoff_file_count >= 0),
            handoff_receipt_count INTEGER NOT NULL DEFAULT 0 CHECK(handoff_receipt_count >= 0),
            updated_at TEXT NOT NULL
        );

        CREATE TABLE torrent_aliases (
            alias_kind TEXT NOT NULL CHECK(alias_kind IN ('v1','v2')),
            alias_hash BLOB NOT NULL,
            registry_id TEXT NOT NULL REFERENCES torrent_registry(registry_id),
            PRIMARY KEY(alias_kind, alias_hash),
            CHECK(
                (alias_kind = 'v1' AND length(alias_hash) = 20)
                OR (alias_kind = 'v2' AND length(alias_hash) = 32)
            )
        );

        CREATE INDEX torrent_aliases_registry_id_idx
        ON torrent_aliases(registry_id);

        UPDATE schema_meta
        SET schema_version = 3,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 3;
        "#,
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v3_to_v4(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE admission_reservations (
            operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),
            registry_id TEXT NOT NULL REFERENCES torrent_registry(registry_id),
            source_relative TEXT NOT NULL CHECK(length(source_relative) > 0),
            source_volume_id BLOB NOT NULL CHECK(length(source_volume_id) = 8),
            source_file_id BLOB NOT NULL CHECK(length(source_file_id) = 8),
            source_size BLOB NOT NULL CHECK(length(source_size) = 8),
            source_modified_marker BLOB NOT NULL CHECK(length(source_modified_marker) = 16),
            source_metainfo_digest BLOB NOT NULL CHECK(length(source_metainfo_digest) = 32),
            working_volume_id BLOB NOT NULL CHECK(length(working_volume_id) = 8),
            reserved_bytes BLOB NOT NULL CHECK(length(reserved_bytes) = 8),
            working_save_path TEXT NOT NULL CHECK(length(working_save_path) > 0),
            reservation_state TEXT NOT NULL
                CHECK(reservation_state IN ('active','released')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX admission_reservations_volume_state_idx
        ON admission_reservations(working_volume_id, reservation_state);

        CREATE UNIQUE INDEX admission_reservations_active_registry_idx
        ON admission_reservations(registry_id)
        WHERE reservation_state = 'active';

        UPDATE schema_meta
        SET schema_version = 4,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 4;
        "#,
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v4_to_v5(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE incoming_cleanup_intents (
            cleanup_id TEXT PRIMARY KEY,
            fingerprint_version INTEGER NOT NULL,
            cleanup_fingerprint BLOB NOT NULL CHECK(length(cleanup_fingerprint) = 32),
            canonical_path TEXT NOT NULL CHECK(length(canonical_path) > 0),
            canonical_volume_id BLOB NOT NULL CHECK(length(canonical_volume_id) = 8),
            canonical_file_id BLOB NOT NULL CHECK(length(canonical_file_id) = 8),
            canonical_size BLOB NOT NULL CHECK(length(canonical_size) = 8),
            canonical_modified_marker BLOB NOT NULL CHECK(length(canonical_modified_marker) = 16),
            redundant_path TEXT NOT NULL CHECK(length(redundant_path) > 0),
            redundant_volume_id BLOB NOT NULL CHECK(length(redundant_volume_id) = 8),
            redundant_file_id BLOB NOT NULL CHECK(length(redundant_file_id) = 8),
            redundant_size BLOB NOT NULL CHECK(length(redundant_size) = 8),
            redundant_modified_marker BLOB NOT NULL CHECK(length(redundant_modified_marker) = 16),
            source_sha256 BLOB NOT NULL CHECK(length(source_sha256) = 32),
            state TEXT NOT NULL CHECK(state IN ('prepared','deleted','blocked')),
            problem_code TEXT,
            revision INTEGER NOT NULL CHECK(revision > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            UNIQUE(fingerprint_version, cleanup_fingerprint)
        );

        CREATE UNIQUE INDEX incoming_cleanup_active_path_idx
        ON incoming_cleanup_intents(redundant_path)
        WHERE state = 'prepared';

        CREATE TABLE incoming_cleanup_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            cleanup_id TEXT NOT NULL REFERENCES incoming_cleanup_intents(cleanup_id),
            revision INTEGER NOT NULL CHECK(revision > 0),
            event_kind TEXT NOT NULL CHECK(event_kind IN ('prepared','deleted','blocked')),
            problem_code TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(cleanup_id, revision)
        );

        UPDATE schema_meta
        SET schema_version = 5,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 5;
        "#,
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v5_to_v6(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE release_operations (
            operation_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE,
            fingerprint_version INTEGER NOT NULL,
            release_fingerprint BLOB NOT NULL CHECK(length(release_fingerprint) = 32),
            registry_id TEXT NOT NULL REFERENCES torrent_registry(registry_id),
            admission_operation_id TEXT NOT NULL REFERENCES admission_reservations(operation_id),
            identity_v1 BLOB CHECK(identity_v1 IS NULL OR length(identity_v1) = 20),
            identity_v2 BLOB CHECK(identity_v2 IS NULL OR length(identity_v2) = 32),
            source_relative TEXT NOT NULL CHECK(length(source_relative) > 0),
            source_volume_id BLOB NOT NULL CHECK(length(source_volume_id) = 8),
            source_file_id BLOB NOT NULL CHECK(length(source_file_id) = 8),
            source_size BLOB NOT NULL CHECK(length(source_size) = 8),
            source_modified_marker BLOB NOT NULL CHECK(length(source_modified_marker) = 16),
            source_metainfo_digest BLOB NOT NULL CHECK(length(source_metainfo_digest) = 32),
            working_volume_id BLOB NOT NULL CHECK(length(working_volume_id) = 8),
            retained_bytes BLOB NOT NULL CHECK(length(retained_bytes) = 8),
            working_save_path TEXT NOT NULL CHECK(length(working_save_path) > 0),
            state TEXT NOT NULL CHECK(state IN (
                'prepared',
                'stop_pending',
                'stopped',
                'unknown_stop',
                'delete_pending',
                'unknown_delete',
                'finished',
                'blocked',
                'failed'
            )),
            resolution TEXT CHECK(resolution IS NULL OR resolution IN ('preserved_incomplete', 'became_complete')),
            problem_code TEXT,
            revision INTEGER NOT NULL CHECK(revision > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            CHECK(identity_v1 IS NOT NULL OR identity_v2 IS NOT NULL)
        );

        CREATE UNIQUE INDEX release_active_registry_idx
        ON release_operations(registry_id)
        WHERE state IN (
            'prepared',
            'stop_pending',
            'stopped',
            'unknown_stop',
            'delete_pending',
            'unknown_delete'
        );

        CREATE INDEX release_recovery_idx
        ON release_operations(state, created_at, operation_id);

        CREATE TABLE release_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL REFERENCES release_operations(operation_id),
            revision INTEGER NOT NULL CHECK(revision > 0),
            event_kind TEXT NOT NULL,
            state TEXT NOT NULL,
            problem_code TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(operation_id, revision)
        );

        CREATE TABLE retained_capacity (
            registry_id TEXT PRIMARY KEY REFERENCES torrent_registry(registry_id),
            admission_operation_id TEXT NOT NULL UNIQUE REFERENCES admission_reservations(operation_id),
            release_operation_id TEXT NOT NULL REFERENCES release_operations(operation_id),
            source_relative TEXT NOT NULL CHECK(length(source_relative) > 0),
            working_volume_id BLOB NOT NULL CHECK(length(working_volume_id) = 8),
            retained_bytes BLOB NOT NULL CHECK(length(retained_bytes) = 8),
            working_save_path TEXT NOT NULL CHECK(length(working_save_path) > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX retained_capacity_volume_idx
        ON retained_capacity(working_volume_id);

        UPDATE schema_meta
        SET schema_version = 6,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 6;
        "#,
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v6_to_v7(connection: &mut Connection) -> Result<(), JournalError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        r#"
        CREATE TABLE completion_operations (
            operation_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL UNIQUE,
            fingerprint_version INTEGER NOT NULL,
            completion_fingerprint BLOB NOT NULL CHECK(length(completion_fingerprint) = 32),
            registry_id TEXT NOT NULL REFERENCES torrent_registry(registry_id),
            torrent_id TEXT NOT NULL CHECK(length(torrent_id) = 40),
            identity_v1 BLOB CHECK(identity_v1 IS NULL OR length(identity_v1) = 20),
            identity_v2 BLOB CHECK(identity_v2 IS NULL OR length(identity_v2) = 32),
            source_relative TEXT NOT NULL CHECK(length(source_relative) > 0),
            source_volume_id BLOB NOT NULL CHECK(length(source_volume_id) = 8),
            source_file_id BLOB NOT NULL CHECK(length(source_file_id) = 8),
            source_size BLOB NOT NULL CHECK(length(source_size) = 8),
            source_modified_marker BLOB NOT NULL CHECK(length(source_modified_marker) = 16),
            source_metainfo_digest BLOB NOT NULL CHECK(length(source_metainfo_digest) = 32),
            working_volume_id BLOB NOT NULL CHECK(length(working_volume_id) = 8),
            completed_volume_id BLOB NOT NULL CHECK(length(completed_volume_id) = 8),
            archive_volume_id BLOB NOT NULL CHECK(length(archive_volume_id) = 8),
            working_save_path TEXT NOT NULL CHECK(length(working_save_path) > 0),
            total_bytes BLOB NOT NULL CHECK(length(total_bytes) = 8),
            payload_strategy TEXT NOT NULL CHECK(payload_strategy IN ('same_volume','cross_volume')),
            archive_strategy TEXT NOT NULL CHECK(archive_strategy IN ('same_volume','cross_volume')),
            archive_destination_volume_id BLOB CHECK(archive_destination_volume_id IS NULL OR length(archive_destination_volume_id) = 8),
            archive_destination_file_id BLOB CHECK(archive_destination_file_id IS NULL OR length(archive_destination_file_id) = 8),
            archive_destination_size BLOB CHECK(archive_destination_size IS NULL OR length(archive_destination_size) = 8),
            archive_destination_modified_marker BLOB CHECK(archive_destination_modified_marker IS NULL OR length(archive_destination_modified_marker) = 16),
            archive_destination_sha256 BLOB CHECK(archive_destination_sha256 IS NULL OR length(archive_destination_sha256) = 32),
            state TEXT NOT NULL CHECK(state IN (
                'prepared',
                'stop_pending',
                'unknown_stop',
                'stopped',
                'archive_pending',
                'unknown_archive',
                'payload_pending',
                'remove_record_pending',
                'unknown_remove_record',
                'finished',
                'blocked',
                'failed'
            )),
            problem_code TEXT,
            revision INTEGER NOT NULL CHECK(revision > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            finished_at TEXT,
            CHECK(identity_v1 IS NOT NULL OR identity_v2 IS NOT NULL)
        );

        CREATE UNIQUE INDEX completion_active_registry_idx
        ON completion_operations(registry_id)
        WHERE state NOT IN ('finished','blocked','failed');

        CREATE INDEX completion_recovery_idx
        ON completion_operations(state, created_at, operation_id);

        CREATE TABLE completion_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_id TEXT NOT NULL REFERENCES completion_operations(operation_id),
            revision INTEGER NOT NULL CHECK(revision > 0),
            event_kind TEXT NOT NULL,
            state TEXT NOT NULL,
            problem_code TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(operation_id, revision)
        );

        CREATE TABLE operation_files (
            operation_id TEXT NOT NULL REFERENCES completion_operations(operation_id),
            file_index INTEGER NOT NULL CHECK(file_index >= 0),
            relative_path TEXT NOT NULL CHECK(length(relative_path) > 0),
            expected_size BLOB NOT NULL CHECK(length(expected_size) = 8),
            source_volume_id BLOB NOT NULL CHECK(length(source_volume_id) = 8),
            source_file_id BLOB NOT NULL CHECK(length(source_file_id) = 8),
            source_size BLOB NOT NULL CHECK(length(source_size) = 8),
            source_modified_marker BLOB NOT NULL CHECK(length(source_modified_marker) = 16),
            handoff_strategy TEXT NOT NULL CHECK(handoff_strategy IN ('same_volume','cross_volume')),
            state TEXT NOT NULL CHECK(state IN (
                'prepared',
                'move_pending',
                'unknown_move',
                'destination_receipted',
                'source_delete_pending',
                'unknown_source_delete',
                'handed_off',
                'blocked',
                'failed'
            )),
            temp_relative TEXT,
            destination_volume_id BLOB CHECK(destination_volume_id IS NULL OR length(destination_volume_id) = 8),
            destination_file_id BLOB CHECK(destination_file_id IS NULL OR length(destination_file_id) = 8),
            destination_size BLOB CHECK(destination_size IS NULL OR length(destination_size) = 8),
            destination_modified_marker BLOB CHECK(destination_modified_marker IS NULL OR length(destination_modified_marker) = 16),
            destination_sha256 BLOB CHECK(destination_sha256 IS NULL OR length(destination_sha256) = 32),
            problem_code TEXT,
            revision INTEGER NOT NULL CHECK(revision > 0),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(operation_id, file_index)
        );

        CREATE UNIQUE INDEX operation_files_path_idx
        ON operation_files(operation_id, relative_path COLLATE NOCASE);

        UPDATE schema_meta
        SET schema_version = 7,
            application_min_version = '0.1.0',
            migrated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE singleton = 1;

        PRAGMA user_version = 7;
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

fn identity_aliases(identity: &TorrentIdentity) -> Vec<(&'static str, Vec<u8>)> {
    let mut aliases = Vec::with_capacity(2);
    if let Some(v1) = identity.v1 {
        aliases.push(("v1", v1.to_vec()));
    }
    if let Some(v2) = identity.v2 {
        aliases.push(("v2", v2.to_vec()));
    }
    aliases
}

fn canonical_identity(identity: &TorrentIdentity) -> String {
    if let Some(v2) = identity.v2 {
        format!("v2:{}", hex_bytes(&v2))
    } else if let Some(v1) = identity.v1 {
        format!("v1:{}", hex_bytes(&v1))
    } else {
        unreachable!("TorrentIdentity is non-empty by construction")
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn matching_registry_ids(
    connection: &Connection,
    identity: &TorrentIdentity,
) -> Result<Vec<String>, JournalError> {
    let mut ids = BTreeSet::new();
    for (kind, hash) in identity_aliases(identity) {
        let registry_id: Option<String> = connection
            .query_row(
                "SELECT registry_id FROM torrent_aliases
                 WHERE alias_kind = ?1 AND alias_hash = ?2",
                params![kind, hash],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(registry_id) = registry_id {
            ids.insert(registry_id);
        }
    }
    Ok(ids.into_iter().collect())
}

fn insert_identity_aliases(
    connection: &Connection,
    registry_id: &str,
    identity: &TorrentIdentity,
) -> Result<(), JournalError> {
    for (kind, hash) in identity_aliases(identity) {
        let existing: Option<String> = connection
            .query_row(
                "SELECT registry_id FROM torrent_aliases
                 WHERE alias_kind = ?1 AND alias_hash = ?2",
                params![kind, hash],
                |row| row.get(0),
            )
            .optional()?;
        match existing {
            Some(existing) if existing != registry_id => {
                return Err(JournalError::IdentityConflict(format!(
                    "{kind} alias is already owned by another registry record"
                )));
            }
            Some(_) => {}
            None => {
                connection.execute(
                    "INSERT INTO torrent_aliases(alias_kind, alias_hash, registry_id)
                     VALUES (?1, ?2, ?3)",
                    params![kind, hash, registry_id],
                )?;
            }
        }
    }
    Ok(())
}

struct StoredRegistryRow {
    registry_id: String,
    state: String,
    source_relative: String,
    source_metainfo_digest: Vec<u8>,
    operation_id: Option<String>,
    archive_ref: Option<String>,
    handoff_file_count: i64,
    handoff_receipt_count: i64,
}

fn load_registry_record(
    connection: &Connection,
    registry_id: &str,
) -> Result<Option<RegistryRecord>, JournalError> {
    let row: Option<StoredRegistryRow> = connection
        .query_row(
            "SELECT registry_id,
                        state,
                        source_relative,
                        source_metainfo_digest,
                        operation_id,
                        archive_ref,
                        handoff_file_count,
                        handoff_receipt_count
                 FROM torrent_registry
                 WHERE registry_id = ?1",
            [registry_id],
            |row| {
                Ok(StoredRegistryRow {
                    registry_id: row.get(0)?,
                    state: row.get(1)?,
                    source_relative: row.get(2)?,
                    source_metainfo_digest: row.get(3)?,
                    operation_id: row.get(4)?,
                    archive_ref: row.get(5)?,
                    handoff_file_count: row.get(6)?,
                    handoff_receipt_count: row.get(7)?,
                })
            },
        )
        .optional()?;

    let Some(StoredRegistryRow {
        registry_id,
        state,
        source_relative,
        source_metainfo_digest: digest,
        operation_id,
        archive_ref,
        handoff_file_count,
        handoff_receipt_count,
    }) = row
    else {
        return Ok(None);
    };

    let source_metainfo_digest: [u8; 32] = digest.try_into().map_err(|value: Vec<u8>| {
        JournalError::InvalidState(format!(
            "registry source digest has {} bytes instead of 32",
            value.len()
        ))
    })?;
    let handoff_file_count = u32::try_from(handoff_file_count)
        .map_err(|_| JournalError::InvalidState("negative registry file count".into()))?;
    let handoff_receipt_count = u32::try_from(handoff_receipt_count)
        .map_err(|_| JournalError::InvalidState("negative registry receipt count".into()))?;

    let mut v1 = None;
    let mut v2 = None;
    let mut statement = connection.prepare(
        "SELECT alias_kind, alias_hash
         FROM torrent_aliases
         WHERE registry_id = ?1
         ORDER BY alias_kind",
    )?;
    let rows = statement.query_map([registry_id.as_str()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    for row in rows {
        let (kind, bytes) = row?;
        match kind.as_str() {
            "v1" => {
                let hash: [u8; 20] = bytes.try_into().map_err(|value: Vec<u8>| {
                    rusqlite::Error::FromSqlConversionFailure(
                        value.len(),
                        rusqlite::types::Type::Blob,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "v1 registry alias must contain 20 bytes",
                        )),
                    )
                })?;
                if v1.replace(hash).is_some() {
                    return Err(JournalError::InvalidState(
                        "registry contains multiple v1 aliases".into(),
                    ));
                }
            }
            "v2" => {
                let hash: [u8; 32] = bytes.try_into().map_err(|value: Vec<u8>| {
                    rusqlite::Error::FromSqlConversionFailure(
                        value.len(),
                        rusqlite::types::Type::Blob,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "v2 registry alias must contain 32 bytes",
                        )),
                    )
                })?;
                if v2.replace(hash).is_some() {
                    return Err(JournalError::InvalidState(
                        "registry contains multiple v2 aliases".into(),
                    ));
                }
            }
            other => {
                return Err(JournalError::InvalidState(format!(
                    "unknown registry alias kind '{other}'"
                )));
            }
        }
    }

    let identity = TorrentIdentity::new(v1, v2)
        .ok_or_else(|| JournalError::InvalidState("registry has no identity aliases".into()))?;
    let state = match state.as_str() {
        "incoming" => RegistryState::Incoming,
        "processing" => RegistryState::Processing,
        "finished" => RegistryState::Finished,
        other => {
            return Err(JournalError::InvalidState(format!(
                "unknown registry state '{other}'"
            )));
        }
    };

    Ok(Some(RegistryRecord {
        registry_id,
        identity,
        state,
        source_relative,
        source_metainfo_digest,
        operation_id,
        archive_ref,
        handoff_file_count,
        handoff_receipt_count,
    }))
}

struct StoredCompletionRow {
    request_id: String,
    operation_id: String,
    registry_id: String,
    torrent_id: String,
    identity_v1: Option<Vec<u8>>,
    identity_v2: Option<Vec<u8>>,
    source_relative: String,
    source_volume_id: Vec<u8>,
    source_file_id: Vec<u8>,
    source_size: Vec<u8>,
    source_modified_marker: Vec<u8>,
    source_metainfo_digest: Vec<u8>,
    working_volume_id: Vec<u8>,
    completed_volume_id: Vec<u8>,
    archive_volume_id: Vec<u8>,
    working_save_path: String,
    total_bytes: Vec<u8>,
    archive_destination_volume_id: Option<Vec<u8>>,
    archive_destination_file_id: Option<Vec<u8>>,
    archive_destination_size: Option<Vec<u8>>,
    archive_destination_modified_marker: Option<Vec<u8>>,
    archive_destination_sha256: Option<Vec<u8>>,
    state: String,
    problem_code: Option<String>,
    revision: u64,
}

fn load_completion_record(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<CompletionRecord>, JournalError> {
    let row: Option<StoredCompletionRow> = connection
        .query_row(
            "SELECT request_id,
                    operation_id,
                    registry_id,
                    torrent_id,
                    identity_v1,
                    identity_v2,
                    source_relative,
                    source_volume_id,
                    source_file_id,
                    source_size,
                    source_modified_marker,
                    source_metainfo_digest,
                    working_volume_id,
                    completed_volume_id,
                    archive_volume_id,
                    working_save_path,
                    total_bytes,
                    archive_destination_volume_id,
                    archive_destination_file_id,
                    archive_destination_size,
                    archive_destination_modified_marker,
                    archive_destination_sha256,
                    state,
                    problem_code,
                    revision
             FROM completion_operations
             WHERE operation_id = ?1",
            [operation_id],
            |row| {
                Ok(StoredCompletionRow {
                    request_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    registry_id: row.get(2)?,
                    torrent_id: row.get(3)?,
                    identity_v1: row.get(4)?,
                    identity_v2: row.get(5)?,
                    source_relative: row.get(6)?,
                    source_volume_id: row.get(7)?,
                    source_file_id: row.get(8)?,
                    source_size: row.get(9)?,
                    source_modified_marker: row.get(10)?,
                    source_metainfo_digest: row.get(11)?,
                    working_volume_id: row.get(12)?,
                    completed_volume_id: row.get(13)?,
                    archive_volume_id: row.get(14)?,
                    working_save_path: row.get(15)?,
                    total_bytes: row.get(16)?,
                    archive_destination_volume_id: row.get(17)?,
                    archive_destination_file_id: row.get(18)?,
                    archive_destination_size: row.get(19)?,
                    archive_destination_modified_marker: row.get(20)?,
                    archive_destination_sha256: row.get(21)?,
                    state: row.get(22)?,
                    problem_code: row.get(23)?,
                    revision: row.get(24)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };

    let request_id = RequestId::new(row.request_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let operation_id = OperationId::new(row.operation_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let torrent_id = TorrentId::new(row.torrent_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let identity_v1 = row
        .identity_v1
        .map(|value| {
            value.try_into().map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "completion v1 identity has {} bytes instead of 20",
                    value.len()
                ))
            })
        })
        .transpose()?;
    let identity_v2 = row
        .identity_v2
        .map(|value| {
            value.try_into().map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "completion v2 identity has {} bytes instead of 32",
                    value.len()
                ))
            })
        })
        .transpose()?;
    let identity = TorrentIdentity::new(identity_v1, identity_v2)
        .ok_or_else(|| JournalError::InvalidState("completion has no torrent identity".into()))?;
    let source_metainfo_digest: [u8; 32] =
        row.source_metainfo_digest
            .try_into()
            .map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "completion source digest has {} bytes instead of 32",
                    value.len()
                ))
            })?;
    let state = parse_completion_state(&row.state).ok_or_else(|| {
        JournalError::InvalidState(format!("unknown completion state '{}'", row.state))
    })?;

    let mut statement = connection.prepare(
        "SELECT file_index,
                relative_path,
                expected_size,
                source_volume_id,
                source_file_id,
                source_size,
                source_modified_marker,
                handoff_strategy,
                state,
                destination_volume_id,
                destination_file_id,
                destination_size,
                destination_modified_marker,
                destination_sha256,
                problem_code,
                revision
         FROM operation_files
         WHERE operation_id = ?1
         ORDER BY file_index",
    )?;
    let rows = statement.query_map([operation_id.as_str()], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Vec<u8>>(2)?,
            row.get::<_, Vec<u8>>(3)?,
            row.get::<_, Vec<u8>>(4)?,
            row.get::<_, Vec<u8>>(5)?,
            row.get::<_, Vec<u8>>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, String>(8)?,
            row.get::<_, Option<Vec<u8>>>(9)?,
            row.get::<_, Option<Vec<u8>>>(10)?,
            row.get::<_, Option<Vec<u8>>>(11)?,
            row.get::<_, Option<Vec<u8>>>(12)?,
            row.get::<_, Option<Vec<u8>>>(13)?,
            row.get::<_, Option<String>>(14)?,
            row.get::<_, u64>(15)?,
        ))
    })?;

    let mut files = Vec::new();
    for row in rows {
        let (
            file_index,
            relative_path,
            expected_size,
            source_volume_id,
            source_file_id,
            source_size,
            source_modified_marker,
            strategy,
            state,
            destination_volume_id,
            destination_file_id,
            destination_size,
            destination_modified_marker,
            destination_sha256,
            problem_code,
            revision,
        ) = row?;
        let index = u32::try_from(file_index)
            .map_err(|_| JournalError::InvalidState("completion file index is negative".into()))?;
        let strategy = parse_completion_strategy(&strategy).ok_or_else(|| {
            JournalError::InvalidState(format!("unknown completion handoff strategy '{strategy}'"))
        })?;
        let state = parse_completion_file_state(&state).ok_or_else(|| {
            JournalError::InvalidState(format!("unknown completion file state '{state}'"))
        })?;
        files.push(CompletionFileRecord {
            index,
            relative_path,
            size: decode_u64_blob(&expected_size, "expected_size")?,
            source_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: decode_u64_blob(&source_volume_id, "source_volume_id")?,
                    file_id: decode_u64_blob(&source_file_id, "source_file_id")?,
                },
                size: decode_u64_blob(&source_size, "source_size")?,
                modified_marker: decode_u128_blob(
                    &source_modified_marker,
                    "source_modified_marker",
                )?,
            },
            strategy,
            state,
            destination_evidence: decode_optional_file_evidence(
                destination_volume_id.as_deref(),
                destination_file_id.as_deref(),
                destination_size.as_deref(),
                destination_modified_marker.as_deref(),
                "destination",
            )?,
            destination_sha256: destination_sha256
                .map(|value| {
                    value.try_into().map_err(|value: Vec<u8>| {
                        JournalError::InvalidState(format!(
                            "completion destination sha256 has {} bytes instead of 32",
                            value.len()
                        ))
                    })
                })
                .transpose()?,
            problem_code,
            revision,
        });
    }

    Ok(Some(CompletionRecord {
        request_id,
        operation_id,
        registry_id: row.registry_id,
        torrent_id,
        identity,
        source_relative: row.source_relative,
        source_evidence: qb_application::storage::FileEvidence {
            identity: qb_application::storage::FileIdentity {
                volume_id: decode_u64_blob(&row.source_volume_id, "source_volume_id")?,
                file_id: decode_u64_blob(&row.source_file_id, "source_file_id")?,
            },
            size: decode_u64_blob(&row.source_size, "source_size")?,
            modified_marker: decode_u128_blob(
                &row.source_modified_marker,
                "source_modified_marker",
            )?,
        },
        source_metainfo_digest,
        working_volume_id: decode_u64_blob(&row.working_volume_id, "working_volume_id")?,
        completed_volume_id: decode_u64_blob(&row.completed_volume_id, "completed_volume_id")?,
        archive_volume_id: decode_u64_blob(&row.archive_volume_id, "archive_volume_id")?,
        working_save_path: row.working_save_path,
        total_bytes: decode_u64_blob(&row.total_bytes, "total_bytes")?,
        archive_destination_evidence: decode_optional_file_evidence(
            row.archive_destination_volume_id.as_deref(),
            row.archive_destination_file_id.as_deref(),
            row.archive_destination_size.as_deref(),
            row.archive_destination_modified_marker.as_deref(),
            "archive_destination",
        )?,
        archive_sha256: row
            .archive_destination_sha256
            .map(|value| {
                value.try_into().map_err(|value: Vec<u8>| {
                    JournalError::InvalidState(format!(
                        "archive destination sha256 has {} bytes instead of 32",
                        value.len()
                    ))
                })
            })
            .transpose()?,
        state,
        problem_code: row.problem_code,
        revision: row.revision,
        files,
    }))
}

fn completion_strategy_name(strategy: CompletionHandoffStrategy) -> &'static str {
    match strategy {
        CompletionHandoffStrategy::SameVolume => "same_volume",
        CompletionHandoffStrategy::CrossVolume => "cross_volume",
    }
}

fn parse_completion_strategy(value: &str) -> Option<CompletionHandoffStrategy> {
    match value {
        "same_volume" => Some(CompletionHandoffStrategy::SameVolume),
        "cross_volume" => Some(CompletionHandoffStrategy::CrossVolume),
        _ => None,
    }
}

fn completion_state_name(state: CompletionState) -> &'static str {
    match state {
        CompletionState::Prepared => "prepared",
        CompletionState::StopPending => "stop_pending",
        CompletionState::UnknownStop => "unknown_stop",
        CompletionState::Stopped => "stopped",
        CompletionState::ArchivePending => "archive_pending",
        CompletionState::UnknownArchive => "unknown_archive",
        CompletionState::PayloadPending => "payload_pending",
        CompletionState::RemoveRecordPending => "remove_record_pending",
        CompletionState::UnknownRemoveRecord => "unknown_remove_record",
        CompletionState::Finished => "finished",
        CompletionState::Blocked => "blocked",
        CompletionState::Failed => "failed",
    }
}

fn parse_completion_state(value: &str) -> Option<CompletionState> {
    match value {
        "prepared" => Some(CompletionState::Prepared),
        "stop_pending" => Some(CompletionState::StopPending),
        "unknown_stop" => Some(CompletionState::UnknownStop),
        "stopped" => Some(CompletionState::Stopped),
        "archive_pending" => Some(CompletionState::ArchivePending),
        "unknown_archive" => Some(CompletionState::UnknownArchive),
        "payload_pending" => Some(CompletionState::PayloadPending),
        "remove_record_pending" => Some(CompletionState::RemoveRecordPending),
        "unknown_remove_record" => Some(CompletionState::UnknownRemoveRecord),
        "finished" => Some(CompletionState::Finished),
        "blocked" => Some(CompletionState::Blocked),
        "failed" => Some(CompletionState::Failed),
        _ => None,
    }
}

fn parse_completion_file_state(value: &str) -> Option<CompletionFileState> {
    match value {
        "prepared" => Some(CompletionFileState::Prepared),
        "move_pending" => Some(CompletionFileState::MovePending),
        "unknown_move" => Some(CompletionFileState::UnknownMove),
        "destination_receipted" => Some(CompletionFileState::DestinationReceipted),
        "source_delete_pending" => Some(CompletionFileState::SourceDeletePending),
        "unknown_source_delete" => Some(CompletionFileState::UnknownSourceDelete),
        "handed_off" => Some(CompletionFileState::HandedOff),
        "blocked" => Some(CompletionFileState::Blocked),
        "failed" => Some(CompletionFileState::Failed),
        _ => None,
    }
}

fn insert_completion_event(
    transaction: &Transaction<'_>,
    operation_id: &str,
    revision: u64,
    state: &str,
    problem_code: Option<&str>,
) -> Result<(), JournalError> {
    transaction.execute(
        "INSERT INTO completion_events(
            operation_id, revision, event_kind, state, problem_code, created_at
         ) VALUES (
            ?1, ?2, ?3, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         )",
        params![operation_id, revision, state, problem_code],
    )?;
    Ok(())
}

fn transition_completion(
    journal: &Journal,
    operation_id: &OperationId,
    expected: &[CompletionState],
    next: CompletionState,
    problem_code: Option<&str>,
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;

    if current.state == next && current.problem_code.as_deref() == problem_code {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if !expected.contains(&current.state) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            format!(
                "completion {} cannot move from {} to {}",
                operation_id,
                completion_state_name(current.state),
                completion_state_name(next)
            ),
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "completion revision overflow"))?;
    let terminal = matches!(
        next,
        CompletionState::Finished | CompletionState::Blocked | CompletionState::Failed
    );
    let changed = transaction
        .execute(
            "UPDATE completion_operations
             SET state = ?1,
                 problem_code = ?2,
                 revision = ?3,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 finished_at = CASE
                     WHEN ?4 != 0 THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     ELSE NULL
                 END
             WHERE operation_id = ?5
               AND revision = ?6
               AND state = ?7",
            params![
                completion_state_name(next),
                problem_code,
                revision,
                if terminal { 1_i64 } else { 0_i64 },
                operation_id.as_str(),
                current.revision,
                completion_state_name(current.state),
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion operation changed concurrently",
        ));
    }

    insert_completion_event(
        &transaction,
        operation_id.as_str(),
        revision,
        completion_state_name(next),
        problem_code,
    )
    .map_err(map_port_error)?;
    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion operation disappeared after transition",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn finish_completion_payload(
    journal: &Journal,
    operation_id: &OperationId,
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
    if current.state != CompletionState::PayloadPending {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "payload completion requires PayloadPending state",
        ));
    }
    if current.files.is_empty()
        || current
            .files
            .iter()
            .any(|file| file.state != CompletionFileState::HandedOff)
    {
        return Err(PortError::new(
            "PAYLOAD_RECEIPTS_INCOMPLETE",
            "every completion file must be HandedOff before qBittorrent record removal",
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "completion revision overflow"))?;
    let changed = transaction
        .execute(
            "UPDATE completion_operations
             SET state = 'remove_record_pending',
                 problem_code = NULL,
                 revision = ?1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?2
               AND revision = ?3
               AND state = 'payload_pending'",
            params![revision, operation_id.as_str(), current.revision],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion payload receipt gate changed concurrently",
        ));
    }
    insert_completion_event(
        &transaction,
        operation_id.as_str(),
        revision,
        "remove_record_pending",
        None,
    )
    .map_err(map_port_error)?;
    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion disappeared after payload receipt gate",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn finish_completion_operation(
    journal: &Journal,
    operation_id: &OperationId,
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;

    if current.state == CompletionState::Finished {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if !matches!(
        current.state,
        CompletionState::RemoveRecordPending | CompletionState::UnknownRemoveRecord
    ) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion can finish only after qBittorrent record-removal intent",
        ));
    }
    if current.archive_destination_evidence.is_none()
        || current.archive_sha256.is_none()
        || current.files.is_empty()
        || current.files.iter().any(|file| {
            file.state != CompletionFileState::HandedOff || file.destination_evidence.is_none()
        })
    {
        return Err(PortError::new(
            "HANDOFF_RECEIPTS_INCOMPLETE",
            "completion cannot finish before Archive and every payload file have durable receipts",
        ));
    }

    let registry = load_registry_record(&transaction, &current.registry_id)
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion registry record disappeared before finish",
            )
        })?;
    if registry.state != RegistryState::Processing
        || registry.identity != current.identity
        || registry.source_relative != current.source_relative
        || registry.source_metainfo_digest != current.source_metainfo_digest
    {
        return Err(PortError::new(
            "COMPLETION_REGISTRY_STALE",
            "registry no longer matches the durable completion operation",
        ));
    }
    let admission_operation_id = registry.operation_id.as_deref().ok_or_else(|| {
        PortError::new(
            "CAPACITY_OWNERSHIP_CONFLICT",
            "processing registry has no admission capacity owner",
        )
    })?;

    let active_reservation: i64 = transaction
        .query_row(
            "SELECT COUNT(*)
             FROM admission_reservations
             WHERE operation_id = ?1
               AND reservation_state = 'active'",
            [admission_operation_id],
            |row| row.get(0),
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let retained_capacity: i64 = transaction
        .query_row(
            "SELECT COUNT(*)
             FROM retained_capacity
             WHERE registry_id = ?1",
            [&current.registry_id],
            |row| row.get(0),
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    match (active_reservation, retained_capacity) {
        (1, 0) => {
            let changed = transaction
                .execute(
                    "UPDATE admission_reservations
                     SET reservation_state = 'released',
                         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                     WHERE operation_id = ?1
                       AND reservation_state = 'active'",
                    [admission_operation_id],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            if changed != 1 {
                return Err(PortError::new(
                    "CAPACITY_OWNERSHIP_CONFLICT",
                    "completion could not release active Working capacity",
                ));
            }
        }
        (0, 1) => {
            let changed = transaction
                .execute(
                    "DELETE FROM retained_capacity
                     WHERE registry_id = ?1",
                    [&current.registry_id],
                )
                .map_err(JournalError::from)
                .map_err(map_port_error)?;
            if changed != 1 {
                return Err(PortError::new(
                    "CAPACITY_OWNERSHIP_CONFLICT",
                    "completion could not release retained Working capacity",
                ));
            }
        }
        _ => {
            return Err(PortError::new(
                "CAPACITY_OWNERSHIP_CONFLICT",
                "completion requires exactly one active or retained Working capacity owner",
            ));
        }
    }

    let file_count = i64::try_from(current.files.len()).map_err(|_| {
        PortError::new(
            "JOURNAL_STATE_INVALID",
            "completion file count does not fit registry storage",
        )
    })?;
    let registry_changed = transaction
        .execute(
            "UPDATE torrent_registry
             SET state = 'finished',
                 operation_id = NULL,
                 archive_ref = ?1,
                 handoff_file_count = ?2,
                 handoff_receipt_count = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE registry_id = ?3
               AND state = 'processing'
               AND operation_id = ?4",
            params![
                current.source_relative,
                file_count,
                current.registry_id,
                admission_operation_id,
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if registry_changed != 1 {
        return Err(PortError::new(
            "JOURNAL_STATE_INVALID",
            "completion could not atomically mark registry Finished",
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "completion revision overflow"))?;
    let changed = transaction
        .execute(
            "UPDATE completion_operations
             SET state = 'finished',
                 problem_code = NULL,
                 revision = ?1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?2
               AND revision = ?3
               AND state IN ('remove_record_pending','unknown_remove_record')",
            params![revision, operation_id.as_str(), current.revision],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion changed concurrently while finishing",
        ));
    }
    insert_completion_event(
        &transaction,
        operation_id.as_str(),
        revision,
        "finished",
        None,
    )
    .map_err(map_port_error)?;

    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion disappeared after finish",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn receipt_completion_archive_destination(
    journal: &Journal,
    operation_id: &OperationId,
    destination: &qb_application::storage::FileEvidence,
    destination_sha256: [u8; 32],
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
    if !matches!(
        current.state,
        CompletionState::ArchivePending | CompletionState::UnknownArchive
    ) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "Archive destination receipt requires ArchivePending or UnknownArchive",
        ));
    }
    if destination.identity.volume_id != current.archive_volume_id
        || destination.size != current.source_evidence.size
    {
        return Err(PortError::new(
            "ARCHIVE_RECEIPT_INVALID",
            "Archive destination evidence does not match the completion source",
        ));
    }
    if current.archive_destination_evidence.as_ref() == Some(destination)
        && current.archive_sha256 == Some(destination_sha256)
    {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if current.archive_destination_evidence.is_some() || current.archive_sha256.is_some() {
        return Err(PortError::new(
            "ARCHIVE_RECEIPT_CONFLICT",
            "Archive destination receipt already exists with different evidence",
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "completion revision overflow"))?;
    let changed = transaction
        .execute(
            "UPDATE completion_operations
             SET archive_destination_volume_id = ?1,
                 archive_destination_file_id = ?2,
                 archive_destination_size = ?3,
                 archive_destination_modified_marker = ?4,
                 archive_destination_sha256 = ?5,
                 problem_code = NULL,
                 revision = ?6,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?7
               AND revision = ?8
               AND state IN ('archive_pending','unknown_archive')",
            params![
                destination.identity.volume_id.to_be_bytes().as_slice(),
                destination.identity.file_id.to_be_bytes().as_slice(),
                destination.size.to_be_bytes().as_slice(),
                destination.modified_marker.to_be_bytes().as_slice(),
                destination_sha256.as_slice(),
                revision,
                operation_id.as_str(),
                current.revision,
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "Archive destination receipt changed concurrently",
        ));
    }
    insert_completion_event(
        &transaction,
        operation_id.as_str(),
        revision,
        completion_state_name(current.state),
        None,
    )
    .map_err(map_port_error)?;
    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion disappeared after Archive destination receipt",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn receipt_completion_archive(
    journal: &Journal,
    operation_id: &OperationId,
    destination: &qb_application::storage::FileEvidence,
    destination_sha256: [u8; 32],
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
    if !matches!(
        current.state,
        CompletionState::ArchivePending | CompletionState::UnknownArchive
    ) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "archive receipt requires ArchivePending or UnknownArchive",
        ));
    }
    if destination.identity.volume_id != current.archive_volume_id
        || destination.size != current.source_evidence.size
    {
        return Err(PortError::new(
            "ARCHIVE_RECEIPT_INVALID",
            "archive destination evidence does not match the completion source",
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "completion revision overflow"))?;
    let changed = transaction
        .execute(
            "UPDATE completion_operations
             SET archive_destination_volume_id = ?1,
                 archive_destination_file_id = ?2,
                 archive_destination_size = ?3,
                 archive_destination_modified_marker = ?4,
                 archive_destination_sha256 = ?5,
                 state = 'payload_pending',
                 problem_code = NULL,
                 revision = ?6,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?7
               AND revision = ?8
               AND state IN ('archive_pending','unknown_archive')",
            params![
                destination.identity.volume_id.to_be_bytes().as_slice(),
                destination.identity.file_id.to_be_bytes().as_slice(),
                destination.size.to_be_bytes().as_slice(),
                destination.modified_marker.to_be_bytes().as_slice(),
                destination_sha256.as_slice(),
                revision,
                operation_id.as_str(),
                current.revision,
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion archive receipt changed concurrently",
        ));
    }
    insert_completion_event(
        &transaction,
        operation_id.as_str(),
        revision,
        "payload_pending",
        None,
    )
    .map_err(map_port_error)?;
    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion disappeared after archive receipt",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn transition_completion_file(
    journal: &Journal,
    operation_id: &OperationId,
    file_index: u32,
    next: CompletionFileState,
    destination: Option<&qb_application::storage::FileEvidence>,
    destination_sha256: Option<[u8; 32]>,
    problem_code: Option<&str>,
) -> Result<CompletionRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("COMPLETION_NOT_FOUND", operation_id.to_string()))?;
    if current.state != CompletionState::PayloadPending {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "file handoff requires completion PayloadPending state",
        ));
    }
    let file = current
        .files
        .iter()
        .find(|file| file.index == file_index)
        .ok_or_else(|| PortError::new("COMPLETION_FILE_NOT_FOUND", file_index.to_string()))?;

    if file.state == next
        && file.problem_code.as_deref() == problem_code
        && file.destination_evidence.as_ref() == destination
        && file.destination_sha256 == destination_sha256
    {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    let allowed = match next {
        CompletionFileState::MovePending => matches!(
            file.state,
            CompletionFileState::Prepared | CompletionFileState::UnknownMove
        ),
        CompletionFileState::UnknownMove => file.state == CompletionFileState::MovePending,
        CompletionFileState::DestinationReceipted => matches!(
            file.state,
            CompletionFileState::MovePending | CompletionFileState::UnknownMove
        ),
        CompletionFileState::SourceDeletePending => matches!(
            file.state,
            CompletionFileState::DestinationReceipted | CompletionFileState::UnknownSourceDelete
        ),
        CompletionFileState::UnknownSourceDelete => {
            file.state == CompletionFileState::SourceDeletePending
        }
        CompletionFileState::HandedOff => matches!(
            file.state,
            CompletionFileState::MovePending
                | CompletionFileState::UnknownMove
                | CompletionFileState::SourceDeletePending
                | CompletionFileState::UnknownSourceDelete
        ),
        CompletionFileState::Blocked => matches!(
            file.state,
            CompletionFileState::Prepared
                | CompletionFileState::MovePending
                | CompletionFileState::UnknownMove
                | CompletionFileState::DestinationReceipted
                | CompletionFileState::SourceDeletePending
                | CompletionFileState::UnknownSourceDelete
        ),
        _ => false,
    };
    if !allowed {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            format!(
                "completion file {} cannot move from {} to {}",
                file.relative_path,
                completion_file_state_name(file.state),
                completion_file_state_name(next)
            ),
        ));
    }

    let revision = file.revision.checked_add(1).ok_or_else(|| {
        PortError::new("JOURNAL_STATE_INVALID", "completion file revision overflow")
    })?;
    let (destination_volume_id, destination_file_id, destination_size, destination_modified_marker) =
        match destination {
            Some(destination) => (
                Some(destination.identity.volume_id.to_be_bytes().to_vec()),
                Some(destination.identity.file_id.to_be_bytes().to_vec()),
                Some(destination.size.to_be_bytes().to_vec()),
                Some(destination.modified_marker.to_be_bytes().to_vec()),
            ),
            None => (None, None, None, None),
        };
    let destination_sha256 = destination_sha256.map(|value| value.to_vec());

    let changed = transaction
        .execute(
            "UPDATE operation_files
             SET state = ?1,
                 destination_volume_id = COALESCE(?2, destination_volume_id),
                 destination_file_id = COALESCE(?3, destination_file_id),
                 destination_size = COALESCE(?4, destination_size),
                 destination_modified_marker = COALESCE(?5, destination_modified_marker),
                 destination_sha256 = COALESCE(?6, destination_sha256),
                 problem_code = ?7,
                 revision = ?8,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?9
               AND file_index = ?10
               AND revision = ?11
               AND state = ?12",
            params![
                completion_file_state_name(next),
                destination_volume_id,
                destination_file_id,
                destination_size,
                destination_modified_marker,
                destination_sha256,
                problem_code,
                revision,
                operation_id.as_str(),
                i64::from(file_index),
                file.revision,
                completion_file_state_name(file.state),
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "completion file changed concurrently",
        ));
    }

    let record = load_completion_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "completion disappeared after file transition",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn completion_file_state_name(state: CompletionFileState) -> &'static str {
    match state {
        CompletionFileState::Prepared => "prepared",
        CompletionFileState::MovePending => "move_pending",
        CompletionFileState::UnknownMove => "unknown_move",
        CompletionFileState::DestinationReceipted => "destination_receipted",
        CompletionFileState::SourceDeletePending => "source_delete_pending",
        CompletionFileState::UnknownSourceDelete => "unknown_source_delete",
        CompletionFileState::HandedOff => "handed_off",
        CompletionFileState::Blocked => "blocked",
        CompletionFileState::Failed => "failed",
    }
}

struct StoredCleanupRow {
    cleanup_id: String,
    canonical_path: String,
    canonical_volume_id: Vec<u8>,
    canonical_file_id: Vec<u8>,
    canonical_size: Vec<u8>,
    canonical_modified_marker: Vec<u8>,
    redundant_path: String,
    redundant_volume_id: Vec<u8>,
    redundant_file_id: Vec<u8>,
    redundant_size: Vec<u8>,
    redundant_modified_marker: Vec<u8>,
    source_sha256: Vec<u8>,
    state: String,
    problem_code: Option<String>,
    revision: u64,
}

fn load_cleanup_record(
    connection: &Connection,
    cleanup_id: &str,
) -> Result<Option<IncomingCleanupRecord>, JournalError> {
    let row = connection
        .query_row(
            "SELECT cleanup_id,
                    canonical_path,
                    canonical_volume_id,
                    canonical_file_id,
                    canonical_size,
                    canonical_modified_marker,
                    redundant_path,
                    redundant_volume_id,
                    redundant_file_id,
                    redundant_size,
                    redundant_modified_marker,
                    source_sha256,
                    state,
                    problem_code,
                    revision
             FROM incoming_cleanup_intents
             WHERE cleanup_id = ?1",
            [cleanup_id],
            |row| {
                Ok(StoredCleanupRow {
                    cleanup_id: row.get(0)?,
                    canonical_path: row.get(1)?,
                    canonical_volume_id: row.get(2)?,
                    canonical_file_id: row.get(3)?,
                    canonical_size: row.get(4)?,
                    canonical_modified_marker: row.get(5)?,
                    redundant_path: row.get(6)?,
                    redundant_volume_id: row.get(7)?,
                    redundant_file_id: row.get(8)?,
                    redundant_size: row.get(9)?,
                    redundant_modified_marker: row.get(10)?,
                    source_sha256: row.get(11)?,
                    state: row.get(12)?,
                    problem_code: row.get(13)?,
                    revision: row.get(14)?,
                })
            },
        )
        .optional()?;

    let Some(row) = row else {
        return Ok(None);
    };

    let state = match row.state.as_str() {
        "prepared" => IncomingCleanupState::Prepared,
        "deleted" => IncomingCleanupState::Deleted,
        "blocked" => IncomingCleanupState::Blocked,
        other => {
            return Err(JournalError::InvalidState(format!(
                "unknown Incoming cleanup state '{other}'"
            )));
        }
    };
    let source_sha256: [u8; 32] = row.source_sha256.try_into().map_err(|value: Vec<u8>| {
        JournalError::InvalidState(format!(
            "cleanup source digest has {} bytes instead of 32",
            value.len()
        ))
    })?;

    Ok(Some(IncomingCleanupRecord {
        cleanup_id: row.cleanup_id,
        intent: IncomingCleanupIntent {
            canonical_path: row.canonical_path,
            canonical_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: decode_u64_blob(&row.canonical_volume_id, "canonical_volume_id")?,
                    file_id: decode_u64_blob(&row.canonical_file_id, "canonical_file_id")?,
                },
                size: decode_u64_blob(&row.canonical_size, "canonical_size")?,
                modified_marker: decode_u128_blob(
                    &row.canonical_modified_marker,
                    "canonical_modified_marker",
                )?,
            },
            redundant_path: row.redundant_path,
            redundant_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: decode_u64_blob(&row.redundant_volume_id, "redundant_volume_id")?,
                    file_id: decode_u64_blob(&row.redundant_file_id, "redundant_file_id")?,
                },
                size: decode_u64_blob(&row.redundant_size, "redundant_size")?,
                modified_marker: decode_u128_blob(
                    &row.redundant_modified_marker,
                    "redundant_modified_marker",
                )?,
            },
            source_sha256,
        },
        state,
        problem_code: row.problem_code,
        revision: row.revision,
    }))
}

fn insert_cleanup_event(
    transaction: &Transaction<'_>,
    cleanup_id: &str,
    revision: u64,
    event_kind: &str,
    problem_code: Option<&str>,
) -> Result<(), JournalError> {
    transaction.execute(
        "INSERT INTO incoming_cleanup_events(
            cleanup_id, revision, event_kind, problem_code, created_at
         ) VALUES (
            ?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         )",
        params![cleanup_id, revision, event_kind, problem_code],
    )?;
    Ok(())
}

fn transition_cleanup(
    journal: &Journal,
    cleanup_id: &str,
    next: IncomingCleanupState,
    problem_code: Option<&str>,
) -> Result<IncomingCleanupRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;

    let current = load_cleanup_record(&transaction, cleanup_id)
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("CLEANUP_NOT_FOUND", cleanup_id.to_string()))?;
    if current.state == next {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if current.state != IncomingCleanupState::Prepared {
        return Err(PortError::new(
            "CLEANUP_TRANSITION_INVALID",
            format!("cleanup {cleanup_id} is not prepared"),
        ));
    }

    let (state, event_kind) = match next {
        IncomingCleanupState::Deleted => ("deleted", "deleted"),
        IncomingCleanupState::Blocked => ("blocked", "blocked"),
        IncomingCleanupState::Prepared => {
            return Err(PortError::new(
                "CLEANUP_TRANSITION_INVALID",
                "cleanup cannot transition back to prepared",
            ));
        }
    };
    let revision = current.revision.checked_add(1).ok_or_else(|| {
        PortError::new(
            "JOURNAL_STATE_INVALID",
            "Incoming cleanup revision overflow",
        )
    })?;

    let changed = transaction
        .execute(
            "UPDATE incoming_cleanup_intents
             SET state = ?1,
                 problem_code = ?2,
                 revision = ?3,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE cleanup_id = ?4
               AND state = 'prepared'
               AND revision = ?5",
            params![state, problem_code, revision, cleanup_id, current.revision],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "CLEANUP_TRANSITION_INVALID",
            "Incoming cleanup changed concurrently",
        ));
    }

    insert_cleanup_event(&transaction, cleanup_id, revision, event_kind, problem_code)
        .map_err(map_port_error)?;

    let record = load_cleanup_record(&transaction, cleanup_id)
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "Incoming cleanup disappeared after transition",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

struct StoredReleaseRow {
    request_id: String,
    operation_id: String,
    registry_id: String,
    admission_operation_id: String,
    identity_v1: Option<Vec<u8>>,
    identity_v2: Option<Vec<u8>>,
    source_relative: String,
    source_volume_id: Vec<u8>,
    source_file_id: Vec<u8>,
    source_size: Vec<u8>,
    source_modified_marker: Vec<u8>,
    source_metainfo_digest: Vec<u8>,
    working_volume_id: Vec<u8>,
    retained_bytes: Vec<u8>,
    working_save_path: String,
    state: String,
    resolution: Option<String>,
    problem_code: Option<String>,
    revision: u64,
}

fn load_release_record(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<ReleaseRecord>, JournalError> {
    let row: Option<StoredReleaseRow> = connection
        .query_row(
            "SELECT request_id,
                    operation_id,
                    registry_id,
                    admission_operation_id,
                    identity_v1,
                    identity_v2,
                    source_relative,
                    source_volume_id,
                    source_file_id,
                    source_size,
                    source_modified_marker,
                    source_metainfo_digest,
                    working_volume_id,
                    retained_bytes,
                    working_save_path,
                    state,
                    resolution,
                    problem_code,
                    revision
             FROM release_operations
             WHERE operation_id = ?1",
            [operation_id],
            |row| {
                Ok(StoredReleaseRow {
                    request_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    registry_id: row.get(2)?,
                    admission_operation_id: row.get(3)?,
                    identity_v1: row.get(4)?,
                    identity_v2: row.get(5)?,
                    source_relative: row.get(6)?,
                    source_volume_id: row.get(7)?,
                    source_file_id: row.get(8)?,
                    source_size: row.get(9)?,
                    source_modified_marker: row.get(10)?,
                    source_metainfo_digest: row.get(11)?,
                    working_volume_id: row.get(12)?,
                    retained_bytes: row.get(13)?,
                    working_save_path: row.get(14)?,
                    state: row.get(15)?,
                    resolution: row.get(16)?,
                    problem_code: row.get(17)?,
                    revision: row.get(18)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };

    let request_id = RequestId::new(row.request_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let operation_id = OperationId::new(row.operation_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let admission_operation_id = OperationId::new(row.admission_operation_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let v1 = row
        .identity_v1
        .map(|value| {
            value.try_into().map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "release v1 identity has {} bytes instead of 20",
                    value.len()
                ))
            })
        })
        .transpose()?;
    let v2 = row
        .identity_v2
        .map(|value| {
            value.try_into().map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "release v2 identity has {} bytes instead of 32",
                    value.len()
                ))
            })
        })
        .transpose()?;
    let identity = TorrentIdentity::new(v1, v2)
        .ok_or_else(|| JournalError::InvalidState("release has no torrent identity".into()))?;
    let source_metainfo_digest: [u8; 32] =
        row.source_metainfo_digest
            .try_into()
            .map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "release source digest has {} bytes instead of 32",
                    value.len()
                ))
            })?;
    let state = parse_release_state(&row.state).ok_or_else(|| {
        JournalError::InvalidState(format!("unknown release state '{}'", row.state))
    })?;
    let resolution = row
        .resolution
        .as_deref()
        .map(parse_release_resolution)
        .transpose()?;

    Ok(Some(ReleaseRecord {
        request_id,
        operation_id,
        registry_id: row.registry_id,
        admission_operation_id,
        identity,
        source_relative: row.source_relative,
        source_evidence: qb_application::storage::FileEvidence {
            identity: qb_application::storage::FileIdentity {
                volume_id: decode_u64_blob(&row.source_volume_id, "source_volume_id")?,
                file_id: decode_u64_blob(&row.source_file_id, "source_file_id")?,
            },
            size: decode_u64_blob(&row.source_size, "source_size")?,
            modified_marker: decode_u128_blob(
                &row.source_modified_marker,
                "source_modified_marker",
            )?,
        },
        source_metainfo_digest,
        working_volume_id: decode_u64_blob(&row.working_volume_id, "working_volume_id")?,
        retained_bytes: decode_u64_blob(&row.retained_bytes, "retained_bytes")?,
        working_save_path: row.working_save_path,
        state,
        resolution,
        problem_code: row.problem_code,
        revision: row.revision,
    }))
}

fn release_state_name(state: ReleaseState) -> &'static str {
    match state {
        ReleaseState::Prepared => "prepared",
        ReleaseState::StopPending => "stop_pending",
        ReleaseState::Stopped => "stopped",
        ReleaseState::UnknownStop => "unknown_stop",
        ReleaseState::DeletePending => "delete_pending",
        ReleaseState::UnknownDelete => "unknown_delete",
        ReleaseState::Finished => "finished",
        ReleaseState::Blocked => "blocked",
        ReleaseState::Failed => "failed",
    }
}

fn parse_release_state(state: &str) -> Option<ReleaseState> {
    match state {
        "prepared" => Some(ReleaseState::Prepared),
        "stop_pending" => Some(ReleaseState::StopPending),
        "stopped" => Some(ReleaseState::Stopped),
        "unknown_stop" => Some(ReleaseState::UnknownStop),
        "delete_pending" => Some(ReleaseState::DeletePending),
        "unknown_delete" => Some(ReleaseState::UnknownDelete),
        "finished" => Some(ReleaseState::Finished),
        "blocked" => Some(ReleaseState::Blocked),
        "failed" => Some(ReleaseState::Failed),
        _ => None,
    }
}

fn release_resolution_name(resolution: ReleaseResolution) -> &'static str {
    match resolution {
        ReleaseResolution::PreservedIncomplete => "preserved_incomplete",
        ReleaseResolution::BecameComplete => "became_complete",
    }
}

fn parse_release_resolution(value: &str) -> Result<ReleaseResolution, JournalError> {
    match value {
        "preserved_incomplete" => Ok(ReleaseResolution::PreservedIncomplete),
        "became_complete" => Ok(ReleaseResolution::BecameComplete),
        other => Err(JournalError::InvalidState(format!(
            "unknown release resolution '{other}'"
        ))),
    }
}

fn insert_release_event(
    transaction: &Transaction<'_>,
    operation_id: &str,
    revision: u64,
    state: &str,
    problem_code: Option<&str>,
) -> Result<(), JournalError> {
    transaction.execute(
        "INSERT INTO release_events(
            operation_id, revision, event_kind, state, problem_code, created_at
         ) VALUES (
            ?1, ?2, ?3, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         )",
        params![operation_id, revision, state, problem_code],
    )?;
    Ok(())
}

fn transition_release(
    journal: &Journal,
    operation_id: &OperationId,
    expected: &[ReleaseState],
    next: ReleaseState,
    problem_code: Option<&str>,
) -> Result<ReleaseRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_release_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("RELEASE_NOT_FOUND", operation_id.to_string()))?;

    if current.state == next && current.problem_code.as_deref() == problem_code {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if !expected.contains(&current.state) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            format!(
                "release {} cannot move from {} to {}",
                operation_id,
                release_state_name(current.state),
                release_state_name(next)
            ),
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "release revision overflow"))?;
    let finished = matches!(
        next,
        ReleaseState::Finished | ReleaseState::Blocked | ReleaseState::Failed
    );
    let finished_sql = if finished {
        "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"
    } else {
        "NULL"
    };
    let changed = transaction
        .execute(
            &format!(
                "UPDATE release_operations
                 SET state = ?1,
                     problem_code = ?2,
                     revision = ?3,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                     finished_at = {finished_sql}
                 WHERE operation_id = ?4
                   AND revision = ?5
                   AND state = ?6"
            ),
            params![
                release_state_name(next),
                problem_code,
                revision,
                operation_id.as_str(),
                current.revision,
                release_state_name(current.state),
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "release operation changed concurrently",
        ));
    }

    insert_release_event(
        &transaction,
        operation_id.as_str(),
        revision,
        release_state_name(next),
        problem_code,
    )
    .map_err(map_port_error)?;

    let record = load_release_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "release operation disappeared after transition",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

fn finish_release_with_resolution(
    journal: &Journal,
    operation_id: &OperationId,
    expected: &[ReleaseState],
    resolution: ReleaseResolution,
    return_to_incoming: bool,
) -> Result<ReleaseRecord, PortError> {
    let mut connection = journal.connection.lock().expect("journal mutex poisoned");
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    let current = load_release_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| PortError::new("RELEASE_NOT_FOUND", operation_id.to_string()))?;

    if current.state == ReleaseState::Finished && current.resolution == Some(resolution) {
        transaction
            .commit()
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        return Ok(current);
    }
    if !expected.contains(&current.state) {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            format!(
                "release {} cannot finish from {} with {}",
                operation_id,
                release_state_name(current.state),
                release_resolution_name(resolution)
            ),
        ));
    }

    let revision = current
        .revision
        .checked_add(1)
        .ok_or_else(|| PortError::new("JOURNAL_STATE_INVALID", "release revision overflow"))?;
    let changed = transaction
        .execute(
            "UPDATE release_operations
             SET state = 'finished',
                 resolution = ?1,
                 problem_code = NULL,
                 revision = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 finished_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE operation_id = ?3
               AND revision = ?4
               AND state = ?5",
            params![
                release_resolution_name(resolution),
                revision,
                operation_id.as_str(),
                current.revision,
                release_state_name(current.state),
            ],
        )
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    if changed != 1 {
        return Err(PortError::new(
            "OPERATION_TRANSITION_INVALID",
            "release operation changed concurrently while finishing",
        ));
    }

    if return_to_incoming {
        let changed = transaction
            .execute(
                "UPDATE torrent_registry
                 SET state = 'incoming',
                     operation_id = NULL,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                 WHERE registry_id = ?1
                   AND state = 'processing'
                   AND operation_id = ?2",
                params![current.registry_id, current.admission_operation_id.as_str()],
            )
            .map_err(JournalError::from)
            .map_err(map_port_error)?;
        if changed != 1 {
            return Err(PortError::new(
                "JOURNAL_STATE_INVALID",
                "preserved-incomplete release could not return registry ownership to Incoming",
            ));
        }
    }

    insert_release_event(
        &transaction,
        operation_id.as_str(),
        revision,
        "finished",
        None,
    )
    .map_err(map_port_error)?;

    let record = load_release_record(&transaction, operation_id.as_str())
        .map_err(map_port_error)?
        .ok_or_else(|| {
            PortError::new(
                "JOURNAL_STATE_INVALID",
                "release operation disappeared after finish",
            )
        })?;
    transaction
        .commit()
        .map_err(JournalError::from)
        .map_err(map_port_error)?;
    Ok(record)
}

struct StoredAdmissionRow {
    request_id: String,
    operation_id: String,
    registry_id: String,
    source_relative: String,
    source_volume_id: Vec<u8>,
    source_file_id: Vec<u8>,
    source_size: Vec<u8>,
    source_modified_marker: Vec<u8>,
    source_metainfo_digest: Vec<u8>,
    working_volume_id: Vec<u8>,
    working_save_path: String,
    reserved_bytes: Vec<u8>,
    reservation_state: String,
    checkpoint: String,
    pending_effect_kind: Option<String>,
    problem_code: Option<String>,
    revision: u64,
}

fn load_admission_record(
    connection: &Connection,
    operation_id: &str,
) -> Result<Option<AdmissionRecord>, JournalError> {
    let row: Option<StoredAdmissionRow> = connection
        .query_row(
            "SELECT requests.request_id,
                    operations.operation_id,
                    admission_reservations.registry_id,
                    admission_reservations.source_relative,
                    admission_reservations.source_volume_id,
                    admission_reservations.source_file_id,
                    admission_reservations.source_size,
                    admission_reservations.source_modified_marker,
                    admission_reservations.source_metainfo_digest,
                    admission_reservations.working_volume_id,
                    admission_reservations.working_save_path,
                    admission_reservations.reserved_bytes,
                    admission_reservations.reservation_state,
                    operations.checkpoint,
                    operations.pending_effect_kind,
                    operations.problem_code,
                    operations.revision
             FROM admission_reservations
             JOIN operations
               ON operations.operation_id = admission_reservations.operation_id
             JOIN requests
               ON requests.request_id = operations.request_id
             WHERE admission_reservations.operation_id = ?1
               AND operations.command_kind = 'admission.add'",
            [operation_id],
            |row| {
                Ok(StoredAdmissionRow {
                    request_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    registry_id: row.get(2)?,
                    source_relative: row.get(3)?,
                    source_volume_id: row.get(4)?,
                    source_file_id: row.get(5)?,
                    source_size: row.get(6)?,
                    source_modified_marker: row.get(7)?,
                    source_metainfo_digest: row.get(8)?,
                    working_volume_id: row.get(9)?,
                    working_save_path: row.get(10)?,
                    reserved_bytes: row.get(11)?,
                    reservation_state: row.get(12)?,
                    checkpoint: row.get(13)?,
                    pending_effect_kind: row.get(14)?,
                    problem_code: row.get(15)?,
                    revision: row.get(16)?,
                })
            },
        )
        .optional()?;

    let Some(row) = row else {
        return Ok(None);
    };

    let StoredAdmissionRow {
        request_id,
        operation_id,
        registry_id,
        source_relative,
        source_volume_id,
        source_file_id,
        source_size,
        source_modified_marker,
        source_metainfo_digest,
        working_volume_id,
        working_save_path,
        reserved_bytes,
        reservation_state,
        checkpoint,
        pending_effect_kind,
        problem_code,
        revision,
    } = row;

    let request_id = RequestId::new(request_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let operation_id = OperationId::new(operation_id)
        .map_err(|error| JournalError::InvalidState(error.to_string()))?;
    let registry = load_registry_record(connection, &registry_id)?.ok_or_else(|| {
        JournalError::InvalidState("admission reservation references missing registry".into())
    })?;
    let reservation_active = match reservation_state.as_str() {
        "active" => true,
        "released" => false,
        other => {
            return Err(JournalError::InvalidState(format!(
                "unknown admission reservation state '{other}'"
            )));
        }
    };
    if reservation_active && registry.operation_id.as_deref() != Some(operation_id.as_str()) {
        return Err(JournalError::InvalidState(
            "active admission registry link does not match reservation".into(),
        ));
    }

    let disposition_text: String = connection.query_row(
        "SELECT disposition FROM operations WHERE operation_id = ?1",
        [operation_id.as_str()],
        |row| row.get(0),
    )?;
    let disposition = parse_disposition(&disposition_text).ok_or_else(|| {
        JournalError::InvalidState(format!(
            "unknown admission disposition '{disposition_text}'"
        ))
    })?;

    let source_metainfo_digest: [u8; 32] =
        source_metainfo_digest
            .try_into()
            .map_err(|value: Vec<u8>| {
                JournalError::InvalidState(format!(
                    "admission metainfo digest has {} bytes instead of 32",
                    value.len()
                ))
            })?;

    Ok(Some(AdmissionRecord {
        request_id,
        operation_id,
        registry_id,
        identity: registry.identity,
        source_relative,
        source_evidence: qb_application::storage::FileEvidence {
            identity: qb_application::storage::FileIdentity {
                volume_id: decode_u64_blob(&source_volume_id, "source_volume_id")?,
                file_id: decode_u64_blob(&source_file_id, "source_file_id")?,
            },
            size: decode_u64_blob(&source_size, "source_size")?,
            modified_marker: decode_u128_blob(&source_modified_marker, "source_modified_marker")?,
        },
        source_metainfo_digest,
        working_volume_id: decode_u64_blob(&working_volume_id, "working_volume_id")?,
        reserved_bytes: decode_u64_blob(&reserved_bytes, "reserved_bytes")?,
        working_save_path,
        reservation_active,
        checkpoint,
        disposition,
        pending_effect_kind,
        problem_code,
        revision,
    }))
}

fn decode_optional_file_evidence(
    volume_id: Option<&[u8]>,
    file_id: Option<&[u8]>,
    size: Option<&[u8]>,
    modified_marker: Option<&[u8]>,
    prefix: &str,
) -> Result<Option<qb_application::storage::FileEvidence>, JournalError> {
    match (volume_id, file_id, size, modified_marker) {
        (None, None, None, None) => Ok(None),
        (Some(volume_id), Some(file_id), Some(size), Some(modified_marker)) => {
            Ok(Some(qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: decode_u64_blob(volume_id, &format!("{prefix}_volume_id"))?,
                    file_id: decode_u64_blob(file_id, &format!("{prefix}_file_id"))?,
                },
                size: decode_u64_blob(size, &format!("{prefix}_size"))?,
                modified_marker: decode_u128_blob(
                    modified_marker,
                    &format!("{prefix}_modified_marker"),
                )?,
            }))
        }
        _ => Err(JournalError::InvalidState(format!(
            "{prefix} evidence columns must be either all null or all present"
        ))),
    }
}

fn decode_u64_blob(bytes: &[u8], field: &str) -> Result<u64, JournalError> {
    let value: [u8; 8] = bytes
        .try_into()
        .map_err(|_| JournalError::InvalidState(format!("{field} must contain exactly 8 bytes")))?;
    Ok(u64::from_be_bytes(value))
}

fn decode_u128_blob(bytes: &[u8], field: &str) -> Result<u128, JournalError> {
    let value: [u8; 16] = bytes.try_into().map_err(|_| {
        JournalError::InvalidState(format!("{field} must contain exactly 16 bytes"))
    })?;
    Ok(u128::from_be_bytes(value))
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
    "SELECT operations.request_id,
            operations.operation_id,
            operations.command_kind,
            requests.fingerprint_version,
            requests.command_fingerprint,
            operations.torrent_id,
            operations.control_action,
            operations.target_client_count,
            operations.max_active_downloads,
            operations.download_limit_bps,
            operations.upload_limit_bps,
            operations.checkpoint,
            operations.disposition,
            operations.pending_effect_kind,
            operations.problem_code,
            operations.revision
     FROM operations
     JOIN requests ON requests.request_id = operations.request_id
     WHERE operations.operation_id = ?1"
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
                JournalError::InvalidState(
                    "mutation numeric value exceeds SQLite integer range".into(),
                )
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
            let expected_action = if kind == "torrent.stop" {
                "stop"
            } else {
                "start"
            };
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
            bytes_per_sec: required_nonnegative_u64(download_limit_bps, "download_limit_bps")?,
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
    spec: EventSpec<'_>,
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
            spec.revision,
            spec.event_kind,
            spec.checkpoint,
            disposition_name(spec.disposition),
            spec.pending_effect_kind,
            spec.problem_code,
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
        JournalError::IdentityConflict(_) => "IDENTITY_CONFLICT",
        JournalError::AlreadyProcessed(_) => "ALREADY_PROCESSED",
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
    fn creates_current_schema_and_reopens() {
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
    fn migrates_v1_through_current_schema_additively() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let mut connection = Connection::open(&path).expect("sqlite");
        configure(&connection).expect("configure");
        create_schema_v1(&mut connection).expect("v1 schema");
        drop(connection);

        let journal = Journal::open(&path).expect("migrate");
        assert_eq!(journal.schema_version().expect("version"), SCHEMA_VERSION);

        let connection = journal.connection.lock().expect("journal mutex");
        let table_count: u32 = connection
            .query_row(
                "SELECT count(*) FROM sqlite_master
                 WHERE type='table' AND name IN (
                    'requests',
                    'operations',
                    'operation_events',
                    'controller_policy',
                    'torrent_registry',
                    'torrent_aliases',
                    'admission_reservations',
                    'incoming_cleanup_intents',
                    'incoming_cleanup_events',
                    'release_operations',
                    'release_events',
                    'retained_capacity',
                    'completion_operations',
                    'completion_events',
                    'operation_files'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("table count");
        assert_eq!(table_count, 15);
    }

    fn cleanup_intent() -> IncomingCleanupIntent {
        IncomingCleanupIntent {
            canonical_path: "a.torrent".into(),
            canonical_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: 7,
                    file_id: 11,
                },
                size: 12,
                modified_marker: 13,
            },
            redundant_path: "b.torrent".into(),
            redundant_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: 7,
                    file_id: 21,
                },
                size: 12,
                modified_marker: 23,
            },
            source_sha256: [0x55; 32],
        }
    }

    #[test]
    fn incoming_cleanup_intent_replays_persists_and_receipts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let intent = cleanup_intent();

        let cleanup_id = {
            let journal = Journal::open(&path).expect("journal");
            let first = match journal.reserve_cleanup(&intent).expect("reserve") {
                IncomingCleanupReservation::New(record) => record,
                other => panic!("unexpected cleanup reservation: {other:?}"),
            };
            assert_eq!(first.state, IncomingCleanupState::Prepared);
            assert_eq!(first.revision, 1);

            match journal.reserve_cleanup(&intent).expect("replay") {
                IncomingCleanupReservation::Replay(record) => {
                    assert_eq!(record.cleanup_id, first.cleanup_id);
                }
                other => panic!("unexpected cleanup replay: {other:?}"),
            }
            assert_eq!(
                journal
                    .list_recoverable_cleanups()
                    .expect("recoverable")
                    .len(),
                1
            );
            first.cleanup_id
        };

        let reopened = Journal::open(&path).expect("reopen");
        let deleted = reopened.mark_cleanup_deleted(&cleanup_id).expect("receipt");
        assert_eq!(deleted.state, IncomingCleanupState::Deleted);
        assert_eq!(deleted.revision, 2);
        assert!(reopened
            .list_recoverable_cleanups()
            .expect("recoverable")
            .is_empty());

        match reopened.reserve_cleanup(&intent).expect("replay deleted") {
            IncomingCleanupReservation::Replay(record) => {
                assert_eq!(record.cleanup_id, cleanup_id);
                assert_eq!(record.state, IncomingCleanupState::Deleted);
            }
            other => panic!("unexpected deleted replay: {other:?}"),
        }
    }

    #[test]
    fn incoming_cleanup_blocked_receipt_is_terminal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let intent = cleanup_intent();
        let cleanup_id = match journal.reserve_cleanup(&intent).expect("reserve") {
            IncomingCleanupReservation::New(record) => record.cleanup_id,
            other => panic!("unexpected cleanup reservation: {other:?}"),
        };

        let blocked = journal
            .mark_cleanup_blocked(&cleanup_id, "SOURCE_AMBIGUOUS")
            .expect("block");
        assert_eq!(blocked.state, IncomingCleanupState::Blocked);
        assert_eq!(blocked.problem_code.as_deref(), Some("SOURCE_AMBIGUOUS"));
        assert!(journal
            .list_recoverable_cleanups()
            .expect("recoverable")
            .is_empty());
    }

    #[test]
    fn registry_aliases_persist_and_hybrid_extends_existing_identity() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let v1 = [0x11; 20];
        let v2 = [0x22; 32];

        let registry_id = {
            let journal = Journal::open(&path).expect("journal");
            let registered = journal
                .register_incoming(&RegisterIncoming {
                    identity: TorrentIdentity::new(Some(v1), None).expect("v1"),
                    source_relative: "a.torrent".into(),
                    source_metainfo_digest: [0x31; 32],
                })
                .expect("register v1");
            let record = match registered {
                RegisterIncomingResult::Registered(record) => record,
                other => panic!("unexpected registration: {other:?}"),
            };

            let extended = journal
                .register_incoming(&RegisterIncoming {
                    identity: TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid"),
                    source_relative: "b.torrent".into(),
                    source_metainfo_digest: [0x31; 32],
                })
                .expect("extend aliases");
            match extended {
                RegisterIncomingResult::AlreadyPresent(existing) => {
                    assert_eq!(existing.registry_id, record.registry_id);
                    assert_eq!(
                        existing.identity,
                        TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid")
                    );
                    assert_eq!(existing.source_relative, "a.torrent");
                }
                other => panic!("unexpected extension: {other:?}"),
            }
            record.registry_id
        };

        let reopened = Journal::open(&path).expect("reopen");
        let found = reopened
            .find_by_identity(&TorrentIdentity::new(None, Some(v2)).expect("v2"))
            .expect("lookup")
            .expect("record");
        assert_eq!(found.registry_id, registry_id);
        assert_eq!(
            found.identity,
            TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid")
        );
    }

    #[test]
    fn registry_rejects_same_identity_with_different_metainfo_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let identity = TorrentIdentity::new(Some([0x71; 20]), None).expect("identity");

        journal
            .register_incoming(&RegisterIncoming {
                identity: identity.clone(),
                source_relative: "a.torrent".into(),
                source_metainfo_digest: [0x81; 32],
            })
            .expect("first registration");

        let error = journal
            .register_incoming(&RegisterIncoming {
                identity,
                source_relative: "b.torrent".into(),
                source_metainfo_digest: [0x82; 32],
            })
            .expect_err("different bytes must conflict");

        assert_eq!(error.code, "IDENTITY_CONFLICT");
    }

    #[test]
    fn registry_rejects_hybrid_bridge_between_two_existing_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let v1 = [0x41; 20];
        let v2 = [0x52; 32];

        journal
            .register_incoming(&RegisterIncoming {
                identity: TorrentIdentity::new(Some(v1), None).expect("v1"),
                source_relative: "v1.torrent".into(),
                source_metainfo_digest: [0x61; 32],
            })
            .expect("register v1");
        journal
            .register_incoming(&RegisterIncoming {
                identity: TorrentIdentity::new(None, Some(v2)).expect("v2"),
                source_relative: "v2.torrent".into(),
                source_metainfo_digest: [0x62; 32],
            })
            .expect("register v2");

        let error = journal
            .register_incoming(&RegisterIncoming {
                identity: TorrentIdentity::new(Some(v1), Some(v2)).expect("hybrid"),
                source_relative: "hybrid.torrent".into(),
                source_metainfo_digest: [0x63; 32],
            })
            .expect_err("bridge must conflict");

        assert_eq!(error.code, "IDENTITY_CONFLICT");
        assert!(journal
            .find_by_identity(&TorrentIdentity::new(Some(v1), None).expect("v1"))
            .expect("find v1")
            .is_some());
        assert!(journal
            .find_by_identity(&TorrentIdentity::new(None, Some(v2)).expect("v2"))
            .expect("find v2")
            .is_some());
    }

    fn admission_request(id: &str) -> AdmissionReservationRequest {
        AdmissionReservationRequest {
            request_id: RequestId::new(id).expect("request id"),
            identity: TorrentIdentity::new(Some([0x91; 20]), Some([0xa2; 32])).expect("identity"),
            source_relative: "candidate.torrent".into(),
            source_evidence: qb_application::storage::FileEvidence {
                identity: qb_application::storage::FileIdentity {
                    volume_id: 7,
                    file_id: 11,
                },
                size: 512,
                modified_marker: 99,
            },
            source_metainfo_digest: [0xb3; 32],
            working_volume_id: 42,
            reserved_bytes: 4096,
            working_save_path: r"C:\Managed\Working".into(),
        }
    }

    #[test]
    fn admission_reservation_is_atomic_replayable_and_persists() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let request = admission_request("admission-1");

        let operation_id = {
            let journal = Journal::open(&path).expect("journal");
            let record = match journal.reserve_admission(&request).expect("reserve") {
                AdmissionReservationResult::New(record) => record,
                other => panic!("unexpected reservation: {other:?}"),
            };
            assert_eq!(record.disposition, MutationDisposition::Prepared);
            assert_eq!(record.checkpoint, "prepared");
            assert_eq!(record.source_evidence, request.source_evidence);
            assert_eq!(
                record.source_metainfo_digest,
                request.source_metainfo_digest
            );
            assert_eq!(record.working_volume_id, 42);
            assert_eq!(record.reserved_bytes, 4096);

            let replay = journal.reserve_admission(&request).expect("replay");
            match replay {
                AdmissionReservationResult::Replay(existing) => {
                    assert_eq!(existing.operation_id, record.operation_id);
                    assert_eq!(existing.registry_id, record.registry_id);
                }
                other => panic!("unexpected replay: {other:?}"),
            }

            let reservations = journal
                .capacity_reservations(42)
                .expect("capacity reservations");
            assert_eq!(reservations.len(), 1);
            assert_eq!(reservations[0].responsible, record.operation_id.as_str());
            assert_eq!(reservations[0].bytes, 4096);
            record.operation_id
        };

        let reopened = Journal::open(&path).expect("reopen");
        let record = reopened
            .get_admission(&operation_id)
            .expect("get admission")
            .expect("admission");
        assert_eq!(record.request_id, request.request_id);
        assert_eq!(record.identity, request.identity);
        assert_eq!(record.source_evidence, request.source_evidence);
        assert_eq!(
            reopened
                .list_recoverable_admissions()
                .expect("recoverable")
                .len(),
            1
        );
        assert!(reopened
            .list_recoverable()
            .expect("mutation recovery")
            .is_empty());
    }

    #[test]
    fn admission_request_id_conflict_does_not_create_second_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let first = admission_request("admission-conflict");
        let first_operation = match journal.reserve_admission(&first).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        let mut changed = first.clone();
        changed.reserved_bytes += 1;
        match journal.reserve_admission(&changed).expect("conflict") {
            AdmissionReservationResult::Conflict { operation_id } => {
                assert_eq!(operation_id, first_operation);
            }
            other => panic!("unexpected result: {other:?}"),
        }

        assert_eq!(
            journal
                .capacity_reservations(42)
                .expect("reservations")
                .len(),
            1
        );
    }

    #[test]
    fn admission_effect_receipt_moves_registry_to_processing_and_finishes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = admission_request("admission-receipt");
        let operation_id = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        let pending = journal
            .mark_admission_effect_pending(&operation_id)
            .expect("effect pending");
        assert_eq!(pending.disposition, MutationDisposition::EffectPending);
        assert_eq!(pending.pending_effect_kind.as_deref(), Some("qbit.add"));

        let observed = journal
            .mark_admission_observed_applied(&operation_id)
            .expect("observed applied");
        assert_eq!(observed.disposition, MutationDisposition::ObservedApplied);
        assert_eq!(
            journal
                .find_by_identity(&request.identity)
                .expect("registry")
                .expect("record")
                .state,
            RegistryState::Processing
        );

        let finished = journal.finish_admission(&operation_id).expect("finish");
        assert_eq!(finished.disposition, MutationDisposition::Finished);
        assert!(finished.reservation_active);
        assert_eq!(
            journal.capacity_reservations(42).expect("capacity").len(),
            1
        );
    }

    #[test]
    fn admission_unknown_keeps_effect_and_reservation_for_observation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = admission_request("admission-unknown");
        let operation_id = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        journal
            .mark_admission_effect_pending(&operation_id)
            .expect("effect pending");
        let unknown = journal
            .mark_admission_unknown(&operation_id, "QBIT_MUTATION_UNCERTAIN")
            .expect("unknown");

        assert_eq!(unknown.disposition, MutationDisposition::Unknown);
        assert_eq!(unknown.pending_effect_kind.as_deref(), Some("qbit.add"));
        assert!(unknown.reservation_active);
        assert_eq!(
            journal
                .list_recoverable_admissions()
                .expect("recoverable")
                .len(),
            1
        );
    }

    #[test]
    fn admission_unknown_can_become_retry_ready_only_after_external_observation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = admission_request("admission-unknown-retry");
        let operation_id = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        journal
            .mark_admission_effect_pending(&operation_id)
            .expect("effect pending");
        journal
            .mark_admission_unknown(&operation_id, "QBIT_MUTATION_UNCERTAIN")
            .expect("unknown");

        let retry = journal
            .mark_admission_retry_ready(&operation_id)
            .expect("retry ready");
        assert_eq!(retry.disposition, MutationDisposition::Prepared);
        assert_eq!(retry.checkpoint, "observed_not_applied");
        assert!(retry.pending_effect_kind.is_none());
        assert!(retry.reservation_active);
    }

    #[test]
    fn definitive_admission_failure_releases_capacity_and_detaches_registry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = admission_request("admission-failed");
        let operation_id = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        journal
            .mark_admission_effect_pending(&operation_id)
            .expect("effect pending");
        let failed = journal
            .mark_admission_failed(&operation_id, "QBIT_MUTATION_REJECTED")
            .expect("failed");
        assert_eq!(failed.disposition, MutationDisposition::Failed);
        assert!(!failed.reservation_active);
        assert!(journal
            .capacity_reservations(42)
            .expect("capacity")
            .is_empty());

        let registry = journal
            .find_by_identity(&request.identity)
            .expect("registry")
            .expect("record");
        assert_eq!(registry.state, RegistryState::Incoming);
        assert!(registry.operation_id.is_none());

        let mut retry = request.clone();
        retry.request_id = RequestId::new("admission-after-failure").expect("request id");
        assert!(matches!(
            journal.reserve_admission(&retry).expect("new reserve"),
            AdmissionReservationResult::New(_)
        ));
    }

    #[test]
    fn admission_preflight_block_is_explicitly_not_submitted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = admission_request("admission-blocked");
        let operation_id = match journal.reserve_admission(&request).expect("reserve") {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected reservation: {other:?}"),
        };

        let blocked = journal
            .mark_admission_not_submitted(&operation_id, "INSUFFICIENT_CAPACITY")
            .expect("blocked");
        assert_eq!(blocked.disposition, MutationDisposition::Blocked);
        assert_eq!(blocked.checkpoint, "not_submitted");
        assert!(blocked.pending_effect_kind.is_none());

        let retry = journal
            .mark_admission_retry_ready(&operation_id)
            .expect("retry ready");
        assert_eq!(retry.disposition, MutationDisposition::Prepared);
        assert_eq!(retry.checkpoint, "retry_ready");
    }

    fn finished_admission(journal: &Journal, request_id_value: &str) -> AdmissionRecord {
        let request = admission_request(request_id_value);
        let operation_id = match journal
            .reserve_admission(&request)
            .expect("reserve admission")
        {
            AdmissionReservationResult::New(record) => record.operation_id,
            other => panic!("unexpected admission reservation: {other:?}"),
        };
        journal
            .mark_admission_effect_pending(&operation_id)
            .expect("admission pending");
        journal
            .mark_admission_observed_applied(&operation_id)
            .expect("admission observed");
        journal
            .finish_admission(&operation_id)
            .expect("admission finished")
    }

    fn completion_preflight(
        admission: &AdmissionRecord,
        request_id_value: &str,
    ) -> CompletionPreflight {
        CompletionPreflight {
            request_id: RequestId::new(request_id_value).expect("request id"),
            registry_id: admission.registry_id.clone(),
            torrent_id: admission
                .identity
                .qbit_selector_ids()
                .into_iter()
                .next()
                .expect("selector"),
            identity: admission.identity.clone(),
            source_relative: admission.source_relative.clone(),
            source_evidence: admission.source_evidence.clone(),
            source_metainfo_digest: admission.source_metainfo_digest,
            working_volume_id: admission.working_volume_id,
            completed_volume_id: admission.working_volume_id,
            archive_volume_id: admission.source_evidence.identity.volume_id,
            working_save_path: admission.working_save_path.clone(),
            total_bytes: 4096,
            files: vec![
                qb_application::completion::CompletionFilePlan {
                    relative_path: "dir/a.bin".into(),
                    size: 1024,
                    source_evidence: qb_application::storage::FileEvidence {
                        identity: qb_application::storage::FileIdentity {
                            volume_id: admission.working_volume_id,
                            file_id: 101,
                        },
                        size: 1024,
                        modified_marker: 1001,
                    },
                },
                qb_application::completion::CompletionFilePlan {
                    relative_path: "dir/b.bin".into(),
                    size: 3072,
                    source_evidence: qb_application::storage::FileEvidence {
                        identity: qb_application::storage::FileIdentity {
                            volume_id: admission.working_volume_id,
                            file_id: 102,
                        },
                        size: 3072,
                        modified_marker: 1002,
                    },
                },
            ],
        }
    }

    #[test]
    fn completion_reservation_is_atomic_replayable_and_persists_file_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let admission = {
            let journal = Journal::open(&path).expect("journal");
            finished_admission(&journal, "completion-admission")
        };
        let preflight = completion_preflight(&admission, "completion-request");

        let operation_id = {
            let journal = Journal::open(&path).expect("reopen");
            let record = match journal
                .reserve_completion(&preflight)
                .expect("reserve completion")
            {
                CompletionReservation::New(record) => record,
                other => panic!("unexpected completion reservation: {other:?}"),
            };
            assert_eq!(record.state, CompletionState::Prepared);
            assert_eq!(record.registry_id, admission.registry_id);
            assert_eq!(record.files.len(), 2);
            assert_eq!(record.files[0].relative_path, "dir/a.bin");
            assert_eq!(
                record.files[0].strategy,
                CompletionHandoffStrategy::SameVolume
            );
            assert_eq!(record.files[0].state, CompletionFileState::Prepared);

            match journal
                .reserve_completion(&preflight)
                .expect("completion replay")
            {
                CompletionReservation::Replay(existing) => {
                    assert_eq!(existing.operation_id, record.operation_id);
                    assert_eq!(existing.files, record.files);
                }
                other => panic!("unexpected completion replay: {other:?}"),
            }

            let mut conflict = preflight.clone();
            conflict.registry_id = "different-registry".into();
            assert!(matches!(
                journal
                    .reserve_completion(&conflict)
                    .expect("request conflict"),
                CompletionReservation::Conflict { .. }
            ));

            let mut active = preflight.clone();
            active.request_id = RequestId::new("completion-active-conflict").expect("request id");
            assert!(matches!(
                journal
                    .reserve_completion(&active)
                    .expect("active conflict"),
                CompletionReservation::ActiveConflict { .. }
            ));

            record.operation_id
        };

        let reopened = Journal::open(&path).expect("reopen again");
        let record = reopened
            .get_completion(&operation_id)
            .expect("get completion")
            .expect("completion");
        assert_eq!(record.files.len(), 2);
        assert_eq!(record.total_bytes, 4096);

        let recoverable = reopened
            .list_recoverable_completions()
            .expect("recoverable completions");
        assert_eq!(recoverable.len(), 1);
        assert_eq!(recoverable[0].operation_id, operation_id);
    }

    #[test]
    fn completion_stop_transitions_are_durable_and_observation_driven() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let admission = {
            let journal = Journal::open(&path).expect("journal");
            finished_admission(&journal, "completion-stop-admission")
        };
        let preflight = completion_preflight(&admission, "completion-stop-request");

        let operation_id = {
            let journal = Journal::open(&path).expect("reopen");
            match journal
                .reserve_completion(&preflight)
                .expect("reserve completion")
            {
                CompletionReservation::New(record) => record.operation_id,
                other => panic!("unexpected completion reservation: {other:?}"),
            }
        };

        let journal = Journal::open(&path).expect("reopen transitions");
        let pending =
            CompletionJournal::mark_stop_pending(&journal, &operation_id).expect("stop pending");
        assert_eq!(pending.state, CompletionState::StopPending);

        let unknown =
            CompletionJournal::mark_unknown_stop(&journal, &operation_id, "QBIT_STOP_UNCERTAIN")
                .expect("unknown stop");
        assert_eq!(unknown.state, CompletionState::UnknownStop);
        assert_eq!(unknown.problem_code.as_deref(), Some("QBIT_STOP_UNCERTAIN"));

        let retry = CompletionJournal::retry_stop(&journal, &operation_id).expect("retry stop");
        assert_eq!(retry.state, CompletionState::Prepared);
        assert!(retry.problem_code.is_none());

        CompletionJournal::mark_stop_pending(&journal, &operation_id).expect("stop pending again");
        let stopped = CompletionJournal::mark_stopped(&journal, &operation_id).expect("stopped");
        assert_eq!(stopped.state, CompletionState::Stopped);

        drop(journal);
        let reopened = Journal::open(&path).expect("reopen stopped");
        let recovered = reopened
            .get_completion(&operation_id)
            .expect("get completion")
            .expect("completion");
        assert_eq!(recovered.state, CompletionState::Stopped);
        assert_eq!(recovered.files.len(), 2);
    }

    #[test]
    fn release_is_durable_and_transfers_capacity_without_losing_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");
        let admission = {
            let journal = Journal::open(&path).expect("journal");
            finished_admission(&journal, "release-admission")
        };

        let release_request = ReleaseRequest {
            request_id: RequestId::new("release-request").expect("request id"),
            registry_id: admission.registry_id.clone(),
        };
        let release_operation = {
            let journal = Journal::open(&path).expect("reopen");
            let record = match journal
                .reserve_release(&release_request)
                .expect("reserve release")
            {
                ReleaseReservation::New(record) => record,
                other => panic!("unexpected release reservation: {other:?}"),
            };
            assert_eq!(record.state, ReleaseState::Prepared);
            assert_eq!(record.admission_operation_id, admission.operation_id);
            assert_eq!(record.retained_bytes, admission.reserved_bytes);
            assert!(
                !journal
                    .get_admission(&admission.operation_id)
                    .expect("admission")
                    .expect("record")
                    .reservation_active
            );
            let reservations = journal.capacity_reservations(42).expect("capacity");
            assert_eq!(reservations.len(), 1);
            assert_eq!(reservations[0].responsible, "retained:candidate.torrent");
            assert_eq!(reservations[0].bytes, 4096);
            record.operation_id
        };

        let reopened = Journal::open(&path).expect("reopen again");
        let recoverable = reopened
            .list_recoverable_releases()
            .expect("recoverable releases");
        assert_eq!(recoverable.len(), 1);
        assert_eq!(recoverable[0].operation_id, release_operation);

        let stop_pending =
            ReleaseJournal::mark_stop_pending(&reopened, &release_operation).expect("stop pending");
        assert_eq!(stop_pending.state, ReleaseState::StopPending);
        let unknown =
            ReleaseJournal::mark_unknown_stop(&reopened, &release_operation, "QBIT_STOP_UNCERTAIN")
                .expect("unknown stop");
        assert_eq!(unknown.state, ReleaseState::UnknownStop);
        let retry = ReleaseJournal::retry_stop(&reopened, &release_operation).expect("retry stop");
        assert_eq!(retry.state, ReleaseState::Prepared);

        ReleaseJournal::mark_stop_pending(&reopened, &release_operation)
            .expect("stop pending again");
        let stopped = ReleaseJournal::mark_stopped(&reopened, &release_operation).expect("stopped");
        assert_eq!(stopped.state, ReleaseState::Stopped);
        reopened
            .mark_delete_pending(&release_operation)
            .expect("delete pending");
        let unknown_delete = reopened
            .mark_unknown_delete(&release_operation, "QBIT_DELETE_UNCERTAIN")
            .expect("unknown delete");
        assert_eq!(unknown_delete.state, ReleaseState::UnknownDelete);
        reopened
            .retry_delete(&release_operation)
            .expect("retry delete");
        reopened
            .mark_delete_pending(&release_operation)
            .expect("delete pending again");
        let finished = reopened
            .finish_release(&release_operation)
            .expect("finish release");
        assert_eq!(finished.state, ReleaseState::Finished);
        assert_eq!(
            finished.resolution,
            Some(ReleaseResolution::PreservedIncomplete)
        );
        assert!(reopened
            .list_recoverable_releases()
            .expect("recoverable")
            .is_empty());

        let registry = reopened
            .find_by_identity(&admission.identity)
            .expect("registry")
            .expect("record");
        assert_eq!(registry.state, RegistryState::Incoming);
        assert!(registry.operation_id.is_none());

        let reservations = reopened
            .capacity_reservations(42)
            .expect("retained capacity");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].responsible, "retained:candidate.torrent");

        let mut readmission = admission_request("readmission-after-release");
        readmission.identity = admission.identity.clone();
        match reopened
            .reserve_admission(&readmission)
            .expect("readmission")
        {
            AdmissionReservationResult::New(record) => {
                assert!(record.reservation_active);
                assert_eq!(record.registry_id, admission.registry_id);
            }
            other => panic!("unexpected readmission result: {other:?}"),
        }
        let reservations = reopened
            .capacity_reservations(42)
            .expect("capacity after readmission");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].bytes, 4096);
    }

    #[test]
    fn release_request_id_replays_and_conflicts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let admission = finished_admission(&journal, "release-conflict-admission");
        let request = ReleaseRequest {
            request_id: RequestId::new("release-idempotent").expect("request id"),
            registry_id: admission.registry_id.clone(),
        };
        let operation_id = match journal.reserve_release(&request).expect("release") {
            ReleaseReservation::New(record) => record.operation_id,
            other => panic!("unexpected release: {other:?}"),
        };
        match journal.reserve_release(&request).expect("replay") {
            ReleaseReservation::Replay(record) => assert_eq!(record.operation_id, operation_id),
            other => panic!("unexpected replay: {other:?}"),
        }

        let conflict = ReleaseRequest {
            request_id: request.request_id,
            registry_id: "different-registry".into(),
        };
        match journal.reserve_release(&conflict).expect("conflict") {
            ReleaseReservation::Conflict {
                operation_id: existing,
            } => {
                assert_eq!(existing, operation_id)
            }
            other => panic!("unexpected conflict: {other:?}"),
        }
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
            .reserve_request_inner(&request, &torrent_command(TorrentControlAction::Start))
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
    fn queue_target_revision_persists_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("state.sqlite");

        {
            let journal = Journal::open(&path).expect("journal");
            let command = MutationCommand::SetQueueTarget {
                target_client_count: 80,
            };
            let operation_id = match journal
                .reserve_request_inner(&request_id("queue-policy-1"), &command)
                .expect("reserve first queue target")
            {
                RequestReservation::New(record) => record.operation_id,
                other => panic!("unexpected reservation: {other:?}"),
            };
            journal
                .apply_queue_target_inner(&operation_id, 80)
                .expect("apply first queue target");

            let policy = journal.queue_target().expect("first policy");
            assert_eq!(policy.revision, 2);
            assert_eq!(policy.target_client_count, Some(80));
        }

        {
            let reopened = Journal::open(&path).expect("reopen");
            let policy = reopened.queue_target().expect("reopened policy");
            assert_eq!(policy.revision, 2);
            assert_eq!(policy.target_client_count, Some(80));

            let command = MutationCommand::SetQueueTarget {
                target_client_count: 64,
            };
            let operation_id = match reopened
                .reserve_request_inner(&request_id("queue-policy-2"), &command)
                .expect("reserve second queue target")
            {
                RequestReservation::New(record) => record.operation_id,
                other => panic!("unexpected reservation: {other:?}"),
            };
            reopened
                .apply_queue_target_inner(&operation_id, 64)
                .expect("apply second queue target");

            let policy = reopened.queue_target().expect("second policy");
            assert_eq!(policy.revision, 3);
            assert_eq!(policy.target_client_count, Some(64));
        }

        let reopened_again = Journal::open(&path).expect("reopen again");
        let policy = reopened_again.queue_target().expect("persisted policy");
        assert_eq!(policy.revision, 3);
        assert_eq!(policy.target_client_count, Some(64));
    }

    #[test]
    fn mutation_checkpoint_progress_is_monotonic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let journal = Journal::open(dir.path().join("state.sqlite")).expect("journal");
        let request = request_id("resume-1");

        let reservation = journal
            .reserve_request_inner(&request, &torrent_command(TorrentControlAction::Start))
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
            .reserve_request_inner(&request, &torrent_command(TorrentControlAction::Stop))
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
