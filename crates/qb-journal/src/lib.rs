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
    mutation::{
        MutationCommand, MutationDisposition, MutationJournal, MutationRecord, QueueTargetPolicy,
        RequestReservation, TorrentControlAction, FINGERPRINT_VERSION,
    },
    registry::{
        RegisterIncoming, RegisterIncomingResult, RegistryRecord, RegistryState, TorrentRegistry,
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

pub const SCHEMA_VERSION: u32 = 4;

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
                "SELECT operation_id, reserved_bytes
                 FROM admission_reservations
                 WHERE working_volume_id = ?1
                   AND reservation_state = 'active'
                 ORDER BY operation_id",
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
                    'admission_reservations'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("table count");
        assert_eq!(table_count, 7);
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
