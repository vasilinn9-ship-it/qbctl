use crate::{
    cleanup::{IncomingCleanupRecord, IncomingCleanupService},
    storage::{IncomingScan, IncomingScanService},
    PortError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingPass {
    pub scan: IncomingScan,
    pub cleanup: Vec<IncomingCleanupRecord>,
}

pub struct IncomingService {
    scan: IncomingScanService,
    cleanup: IncomingCleanupService,
    max_metainfo_bytes: usize,
}

impl IncomingService {
    pub fn new(
        scan: IncomingScanService,
        cleanup: IncomingCleanupService,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            scan,
            cleanup,
            max_metainfo_bytes,
        }
    }

    pub async fn run_once(&self) -> Result<IncomingPass, PortError> {
        let scan = self.scan.scan(self.max_metainfo_bytes)?;
        let cleanup = self.cleanup.execute(&scan.redundant_identical).await?;
        Ok(IncomingPass { scan, cleanup })
    }

    pub fn scan(&self) -> Result<IncomingScan, PortError> {
        self.scan.scan(self.max_metainfo_bytes)
    }

    pub async fn recover_cleanup(&self) -> Result<Vec<IncomingCleanupRecord>, PortError> {
        self.cleanup.recover_all().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use qb_domain::torrent::{ManifestFile, TorrentIdentity, TorrentManifest, TorrentMetainfo};

    use crate::{
        cleanup::{
            IncomingCleanupIntent, IncomingCleanupJournal, IncomingCleanupReservation,
            IncomingCleanupState,
        },
        registry::{RegisterIncoming, RegisterIncomingResult, RegistryRecord, TorrentRegistry},
        storage::{
            FileEvidence, FileIdentity, IncomingDeleteOutcome, IncomingFileSnapshot, ManagedRoot,
            Storage, StorageVolumeStatus,
        },
        torrent::MetainfoReader,
    };

    use super::*;

    struct FakeStorage {
        files: Mutex<BTreeMap<String, IncomingFileSnapshot>>,
    }

    impl Storage for FakeStorage {
        fn volume_status(&self, _root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            Err(unused())
        }

        fn root_path(&self, _root: ManagedRoot) -> Result<String, PortError> {
            Err(unused())
        }

        fn matches_root_path(
            &self,
            _root: ManagedRoot,
            _observed: &str,
        ) -> Result<bool, PortError> {
            Err(unused())
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Ok(self
                .files
                .lock()
                .expect("storage mutex")
                .keys()
                .cloned()
                .collect())
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            self.files
                .lock()
                .expect("storage mutex")
                .get(relative_path)
                .cloned()
                .ok_or_else(|| PortError::new("STORAGE_NOT_FOUND", relative_path))
        }

        fn delete_incoming_exact(
            &self,
            relative_path: &str,
            expected_evidence: &FileEvidence,
            expected_bytes: &[u8],
            _max_bytes: usize,
        ) -> Result<IncomingDeleteOutcome, PortError> {
            let mut files = self.files.lock().expect("storage mutex");
            let Some(snapshot) = files.get(relative_path) else {
                return Ok(IncomingDeleteOutcome::Missing);
            };
            if &snapshot.evidence != expected_evidence || snapshot.bytes != expected_bytes {
                return Ok(IncomingDeleteOutcome::Changed);
            }
            files.remove(relative_path);
            Ok(IncomingDeleteOutcome::Deleted)
        }
    }

    struct FakeMetainfoReader;

    impl MetainfoReader for FakeMetainfoReader {
        fn parse(&self, _bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
            Ok(TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                manifest: TorrentManifest::new(vec![ManifestFile {
                    path: "payload.bin".into(),
                    size: 1,
                }])
                .expect("manifest"),
            })
        }
    }

    struct EmptyRegistry;

    impl TorrentRegistry for EmptyRegistry {
        fn find_by_identity(
            &self,
            _identity: &TorrentIdentity,
        ) -> Result<Option<RegistryRecord>, PortError> {
            Ok(None)
        }

        fn register_incoming(
            &self,
            _candidate: &RegisterIncoming,
        ) -> Result<RegisterIncomingResult, PortError> {
            Err(unused())
        }
    }

    #[derive(Default)]
    struct FakeCleanupJournal {
        record: Mutex<Option<IncomingCleanupRecord>>,
    }

    impl IncomingCleanupJournal for FakeCleanupJournal {
        fn reserve_cleanup(
            &self,
            intent: &IncomingCleanupIntent,
        ) -> Result<IncomingCleanupReservation, PortError> {
            let mut state = self.record.lock().expect("journal mutex");
            if let Some(record) = state.as_ref() {
                return Ok(IncomingCleanupReservation::Replay(record.clone()));
            }
            let record = IncomingCleanupRecord {
                cleanup_id: "cleanup-1".into(),
                intent: intent.clone(),
                state: IncomingCleanupState::Prepared,
                problem_code: None,
                revision: 1,
            };
            *state = Some(record.clone());
            Ok(IncomingCleanupReservation::New(record))
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

    fn snapshot(path: &str, file_id: u64) -> IncomingFileSnapshot {
        let bytes = b"identical-metainfo".to_vec();
        IncomingFileSnapshot {
            relative_path: path.into(),
            evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 7,
                    file_id,
                },
                size: u64::try_from(bytes.len()).expect("fixture size"),
                modified_marker: u128::from(file_id),
            },
            bytes,
        }
    }

    fn unused() -> PortError {
        PortError::new(
            "INTERNAL_INVARIANT_VIOLATION",
            "unexpected fake dependency call",
        )
    }

    #[tokio::test]
    async fn one_shot_scan_executes_exact_duplicate_cleanup() {
        let storage = Arc::new(FakeStorage {
            files: Mutex::new(BTreeMap::from([
                ("a.torrent".into(), snapshot("a.torrent", 1)),
                ("b.torrent".into(), snapshot("b.torrent", 2)),
            ])),
        });
        let scan = IncomingScanService::new(
            storage.clone(),
            Arc::new(FakeMetainfoReader),
            Arc::new(EmptyRegistry),
        );
        let cleanup = IncomingCleanupService::new(
            Arc::new(FakeCleanupJournal::default()),
            storage.clone(),
            1024,
        );
        let service = IncomingService::new(scan, cleanup, 1024);

        let pass = service.run_once().await.expect("incoming pass");

        assert_eq!(pass.scan.eligible.len(), 1);
        assert_eq!(pass.scan.redundant_identical.len(), 1);
        assert_eq!(pass.cleanup.len(), 1);
        assert_eq!(pass.cleanup[0].state, IncomingCleanupState::Deleted);

        let files = storage.files.lock().expect("storage mutex");
        assert!(files.contains_key("a.torrent"));
        assert!(!files.contains_key("b.torrent"));
    }
}
