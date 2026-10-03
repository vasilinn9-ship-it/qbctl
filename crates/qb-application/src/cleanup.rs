use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::{
    storage::{FileEvidence, IncomingDeleteOutcome, IncomingRedundantCopy, Storage},
    PortError,
};

pub const INCOMING_CLEANUP_FINGERPRINT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingCleanupIntent {
    pub canonical_path: String,
    pub canonical_evidence: FileEvidence,
    pub redundant_path: String,
    pub redundant_evidence: FileEvidence,
    pub source_sha256: [u8; 32],
}

impl From<&IncomingRedundantCopy> for IncomingCleanupIntent {
    fn from(copy: &IncomingRedundantCopy) -> Self {
        Self {
            canonical_path: copy.canonical_path.clone(),
            canonical_evidence: copy.canonical_evidence.clone(),
            redundant_path: copy.redundant_path.clone(),
            redundant_evidence: copy.source_evidence.clone(),
            source_sha256: copy.source_sha256,
        }
    }
}

impl IncomingCleanupIntent {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-incoming-cleanup-v1\0");
        update_text(&mut digest, &self.canonical_path);
        update_evidence(&mut digest, &self.canonical_evidence);
        update_text(&mut digest, &self.redundant_path);
        update_evidence(&mut digest, &self.redundant_evidence);
        digest.update(self.source_sha256);
        digest.finalize().into()
    }
}

fn update_text(digest: &mut Sha256, value: &str) {
    let length = u64::try_from(value.len()).expect("managed path length fits u64");
    digest.update(length.to_be_bytes());
    digest.update(value.as_bytes());
}

fn update_evidence(digest: &mut Sha256, evidence: &FileEvidence) {
    digest.update(evidence.identity.volume_id.to_be_bytes());
    digest.update(evidence.identity.file_id.to_be_bytes());
    digest.update(evidence.size.to_be_bytes());
    digest.update(evidence.modified_marker.to_be_bytes());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingCleanupState {
    Prepared,
    Deleted,
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingCleanupRecord {
    pub cleanup_id: String,
    pub intent: IncomingCleanupIntent,
    pub state: IncomingCleanupState,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingCleanupReservation {
    New(IncomingCleanupRecord),
    Replay(IncomingCleanupRecord),
}

pub trait IncomingCleanupJournal: Send + Sync {
    fn reserve_cleanup(
        &self,
        intent: &IncomingCleanupIntent,
    ) -> Result<IncomingCleanupReservation, PortError>;

    fn list_recoverable_cleanups(&self) -> Result<Vec<IncomingCleanupRecord>, PortError>;

    fn mark_cleanup_deleted(&self, cleanup_id: &str) -> Result<IncomingCleanupRecord, PortError>;

    fn mark_cleanup_blocked(
        &self,
        cleanup_id: &str,
        problem_code: &str,
    ) -> Result<IncomingCleanupRecord, PortError>;
}

pub struct IncomingCleanupService {
    journal: Arc<dyn IncomingCleanupJournal>,
    storage: Arc<dyn Storage>,
    max_metainfo_bytes: usize,
    lane: tokio::sync::Mutex<()>,
}

impl IncomingCleanupService {
    pub fn new(
        journal: Arc<dyn IncomingCleanupJournal>,
        storage: Arc<dyn Storage>,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            journal,
            storage,
            max_metainfo_bytes,
            lane: tokio::sync::Mutex::new(()),
        }
    }

    pub async fn execute(
        &self,
        copies: &[IncomingRedundantCopy],
    ) -> Result<Vec<IncomingCleanupRecord>, PortError> {
        let _guard = self.lane.lock().await;
        let mut records = Vec::with_capacity(copies.len());

        for copy in copies {
            let intent = IncomingCleanupIntent::from(copy);
            let record = match self.journal.reserve_cleanup(&intent)? {
                IncomingCleanupReservation::New(record)
                | IncomingCleanupReservation::Replay(record) => record,
            };
            records.push(self.advance(record)?);
        }

        Ok(records)
    }

    pub async fn recover_all(&self) -> Result<Vec<IncomingCleanupRecord>, PortError> {
        let _guard = self.lane.lock().await;
        let records = self.journal.list_recoverable_cleanups()?;
        records
            .into_iter()
            .map(|record| self.advance(record))
            .collect()
    }

    fn advance(&self, record: IncomingCleanupRecord) -> Result<IncomingCleanupRecord, PortError> {
        if record.state != IncomingCleanupState::Prepared {
            return Ok(record);
        }

        let Some(canonical_bytes) = self.canonical_exact_bytes(&record.intent)? else {
            return self
                .journal
                .mark_cleanup_blocked(&record.cleanup_id, "SOURCE_AMBIGUOUS");
        };

        match self.storage.delete_incoming_exact(
            &record.intent.redundant_path,
            &record.intent.redundant_evidence,
            &canonical_bytes,
            self.max_metainfo_bytes,
        )? {
            IncomingDeleteOutcome::Deleted | IncomingDeleteOutcome::Missing => {
                self.journal.mark_cleanup_deleted(&record.cleanup_id)
            }
            IncomingDeleteOutcome::Changed => self
                .journal
                .mark_cleanup_blocked(&record.cleanup_id, "SOURCE_AMBIGUOUS"),
        }
    }

    fn canonical_exact_bytes(
        &self,
        intent: &IncomingCleanupIntent,
    ) -> Result<Option<Vec<u8>>, PortError> {
        let snapshot = match self
            .storage
            .read_incoming(&intent.canonical_path, self.max_metainfo_bytes)
        {
            Ok(snapshot) => snapshot,
            Err(error)
                if matches!(
                    error.code,
                    "STORAGE_IO"
                        | "STORAGE_SOURCE_CHANGED"
                        | "STORAGE_REPARSE_POINT"
                        | "STORAGE_NOT_FILE"
                        | "STORAGE_SOURCE_TOO_LARGE"
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };

        let digest: [u8; 32] = Sha256::digest(&snapshot.bytes).into();
        if snapshot.evidence != intent.canonical_evidence || digest != intent.source_sha256 {
            return Ok(None);
        }
        Ok(Some(snapshot.bytes))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::storage::{FileIdentity, IncomingFileSnapshot, ManagedRoot, StorageVolumeStatus};

    use super::*;

    #[derive(Default)]
    struct FakeCleanupJournal {
        record: Mutex<Option<IncomingCleanupRecord>>,
    }

    impl IncomingCleanupJournal for FakeCleanupJournal {
        fn reserve_cleanup(
            &self,
            intent: &IncomingCleanupIntent,
        ) -> Result<IncomingCleanupReservation, PortError> {
            let mut record = self.record.lock().expect("journal mutex");
            if let Some(existing) = record.clone() {
                return Ok(IncomingCleanupReservation::Replay(existing));
            }
            let created = IncomingCleanupRecord {
                cleanup_id: "cleanup-1".into(),
                intent: intent.clone(),
                state: IncomingCleanupState::Prepared,
                problem_code: None,
                revision: 1,
            };
            *record = Some(created.clone());
            Ok(IncomingCleanupReservation::New(created))
        }

        fn list_recoverable_cleanups(&self) -> Result<Vec<IncomingCleanupRecord>, PortError> {
            Ok(self
                .record
                .lock()
                .expect("journal mutex")
                .as_ref()
                .filter(|record| record.state == IncomingCleanupState::Prepared)
                .cloned()
                .into_iter()
                .collect())
        }

        fn mark_cleanup_deleted(
            &self,
            _cleanup_id: &str,
        ) -> Result<IncomingCleanupRecord, PortError> {
            let mut state = self.record.lock().expect("journal mutex");
            let record = state.as_mut().expect("record");
            record.state = IncomingCleanupState::Deleted;
            record.problem_code = None;
            record.revision += 1;
            Ok(record.clone())
        }

        fn mark_cleanup_blocked(
            &self,
            _cleanup_id: &str,
            problem_code: &str,
        ) -> Result<IncomingCleanupRecord, PortError> {
            let mut state = self.record.lock().expect("journal mutex");
            let record = state.as_mut().expect("record");
            record.state = IncomingCleanupState::Blocked;
            record.problem_code = Some(problem_code.into());
            record.revision += 1;
            Ok(record.clone())
        }
    }

    struct FakeCleanupStorage {
        canonical: IncomingFileSnapshot,
        delete_outcome: IncomingDeleteOutcome,
        delete_calls: Mutex<usize>,
    }

    impl Storage for FakeCleanupStorage {
        fn volume_status(&self, _root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            Err(unused_storage_call())
        }

        fn root_path(&self, _root: ManagedRoot) -> Result<String, PortError> {
            Err(unused_storage_call())
        }

        fn matches_root_path(
            &self,
            _root: ManagedRoot,
            _observed: &str,
        ) -> Result<bool, PortError> {
            Err(unused_storage_call())
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Err(unused_storage_call())
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            if relative_path != self.canonical.relative_path {
                return Err(PortError::new("STORAGE_NOT_FOUND", relative_path));
            }
            Ok(self.canonical.clone())
        }

        fn delete_incoming_exact(
            &self,
            _relative_path: &str,
            _expected_evidence: &FileEvidence,
            expected_bytes: &[u8],
            _max_bytes: usize,
        ) -> Result<IncomingDeleteOutcome, PortError> {
            assert_eq!(expected_bytes, self.canonical.bytes);
            *self.delete_calls.lock().expect("delete mutex") += 1;
            Ok(self.delete_outcome)
        }
    }

    fn unused_storage_call() -> PortError {
        PortError::new(
            "INTERNAL_INVARIANT_VIOLATION",
            "unexpected fake Storage call",
        )
    }

    fn cleanup_fixture() -> (IncomingRedundantCopy, IncomingFileSnapshot) {
        let bytes = b"same-metainfo".to_vec();
        let canonical_evidence = FileEvidence {
            identity: FileIdentity {
                volume_id: 1,
                file_id: 10,
            },
            size: u64::try_from(bytes.len()).expect("fixture size"),
            modified_marker: 20,
        };
        let redundant_evidence = FileEvidence {
            identity: FileIdentity {
                volume_id: 1,
                file_id: 11,
            },
            size: u64::try_from(bytes.len()).expect("fixture size"),
            modified_marker: 21,
        };
        let source_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let identity =
            qb_domain::torrent::TorrentIdentity::new(Some([0x11; 20]), None).expect("identity");

        (
            IncomingRedundantCopy {
                canonical_path: "a.torrent".into(),
                canonical_evidence: canonical_evidence.clone(),
                redundant_path: "b.torrent".into(),
                source_evidence: redundant_evidence,
                source_sha256,
                torrent_identity: identity,
            },
            IncomingFileSnapshot {
                relative_path: "a.torrent".into(),
                evidence: canonical_evidence,
                bytes,
            },
        )
    }

    #[tokio::test]
    async fn cleanup_blocks_without_delete_if_canonical_changed() {
        let (copy, mut canonical) = cleanup_fixture();
        canonical.evidence.modified_marker += 1;
        let journal = Arc::new(FakeCleanupJournal::default());
        let storage = Arc::new(FakeCleanupStorage {
            canonical,
            delete_outcome: IncomingDeleteOutcome::Deleted,
            delete_calls: Mutex::new(0),
        });
        let service = IncomingCleanupService::new(journal, storage.clone(), 1024);

        let records = service.execute(&[copy]).await.expect("cleanup");

        assert_eq!(records[0].state, IncomingCleanupState::Blocked);
        assert_eq!(records[0].problem_code.as_deref(), Some("SOURCE_AMBIGUOUS"));
        assert_eq!(*storage.delete_calls.lock().expect("delete mutex"), 0);
    }

    #[tokio::test]
    async fn cleanup_blocks_when_redundant_changed_at_delete_boundary() {
        let (copy, canonical) = cleanup_fixture();
        let journal = Arc::new(FakeCleanupJournal::default());
        let storage = Arc::new(FakeCleanupStorage {
            canonical,
            delete_outcome: IncomingDeleteOutcome::Changed,
            delete_calls: Mutex::new(0),
        });
        let service = IncomingCleanupService::new(journal, storage.clone(), 1024);

        let records = service.execute(&[copy]).await.expect("cleanup");

        assert_eq!(records[0].state, IncomingCleanupState::Blocked);
        assert_eq!(records[0].problem_code.as_deref(), Some("SOURCE_AMBIGUOUS"));
        assert_eq!(*storage.delete_calls.lock().expect("delete mutex"), 1);
    }

    #[tokio::test]
    async fn cleanup_recovery_receipts_missing_file_after_delete_crash() {
        let (copy, canonical) = cleanup_fixture();
        let journal = Arc::new(FakeCleanupJournal::default());
        let intent = IncomingCleanupIntent::from(&copy);
        match journal.reserve_cleanup(&intent).expect("intent") {
            IncomingCleanupReservation::New(_) => {}
            other => panic!("unexpected reservation: {other:?}"),
        }

        let storage = Arc::new(FakeCleanupStorage {
            canonical,
            delete_outcome: IncomingDeleteOutcome::Missing,
            delete_calls: Mutex::new(0),
        });
        let service = IncomingCleanupService::new(journal, storage.clone(), 1024);

        let records = service.recover_all().await.expect("recover");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, IncomingCleanupState::Deleted);
        assert_eq!(*storage.delete_calls.lock().expect("delete mutex"), 1);
    }

    #[test]
    fn cleanup_fingerprint_covers_both_file_identities_and_bytes() {
        let base = IncomingCleanupIntent {
            canonical_path: "a.torrent".into(),
            canonical_evidence: FileEvidence {
                identity: crate::storage::FileIdentity {
                    volume_id: 1,
                    file_id: 2,
                },
                size: 3,
                modified_marker: 4,
            },
            redundant_path: "b.torrent".into(),
            redundant_evidence: FileEvidence {
                identity: crate::storage::FileIdentity {
                    volume_id: 1,
                    file_id: 5,
                },
                size: 3,
                modified_marker: 6,
            },
            source_sha256: [0x77; 32],
        };
        let mut changed = base.clone();
        changed.redundant_evidence.identity.file_id += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.canonical_evidence.modified_marker += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.source_sha256[0] ^= 0xff;
        assert_ne!(base.fingerprint(), changed.fingerprint());
    }
}
