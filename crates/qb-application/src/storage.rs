use std::{collections::BTreeMap, sync::Arc};

use qb_domain::torrent::TorrentMetainfo;
use sha2::{Digest, Sha256};

use crate::{
    registry::{RegistryRecord, RegistryState, TorrentRegistry},
    torrent::MetainfoReader,
    PortError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileIdentity {
    pub volume_id: u64,
    pub file_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileEvidence {
    pub identity: FileIdentity,
    pub size: u64,
    pub modified_marker: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingFileSnapshot {
    pub relative_path: String,
    pub evidence: FileEvidence,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedRoot {
    Incoming,
    Archive,
    Working,
    Completed,
    Runtime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageVolumeStatus {
    pub root: ManagedRoot,
    pub volume_id: u64,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingDeleteOutcome {
    Deleted,
    Missing,
    Changed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SameVolumeMoveOutcome {
    Moved { destination: FileEvidence },
    SourceMissing,
    SourceChanged { observed: FileEvidence },
    DestinationExists { observed: FileEvidence },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VerifiedCopyOutcome {
    Verified {
        temp: FileEvidence,
        sha256: [u8; 32],
        created: bool,
    },
    SourceMissing,
    SourceChanged {
        observed: FileEvidence,
    },
    TempConflict {
        observed: FileEvidence,
        sha256: [u8; 32],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedDeleteOutcome {
    Deleted,
    Missing,
    Changed,
}

pub trait Storage: Send + Sync {
    fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError>;

    fn root_path(&self, root: ManagedRoot) -> Result<String, PortError>;

    fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError>;

    fn observe_file(
        &self,
        _root: ManagedRoot,
        _relative_path: &str,
    ) -> Result<Option<FileEvidence>, PortError> {
        Err(PortError::new(
            "STORAGE_OBSERVE_UNSUPPORTED",
            "managed file observation is not supported by this storage adapter",
        ))
    }

    fn move_same_volume_no_replace(
        &self,
        _source_root: ManagedRoot,
        _source_relative: &str,
        _destination_root: ManagedRoot,
        _destination_relative: &str,
        _expected_source: &FileEvidence,
    ) -> Result<SameVolumeMoveOutcome, PortError> {
        Err(PortError::new(
            "STORAGE_MOVE_UNSUPPORTED",
            "same-volume managed move is not supported by this storage adapter",
        ))
    }

    fn copy_to_temp_verified(
        &self,
        _source_root: ManagedRoot,
        _source_relative: &str,
        _destination_root: ManagedRoot,
        _temp_relative: &str,
        _expected_source: &FileEvidence,
    ) -> Result<VerifiedCopyOutcome, PortError> {
        Err(PortError::new(
            "STORAGE_COPY_UNSUPPORTED",
            "verified cross-volume copy is not supported by this storage adapter",
        ))
    }

    fn delete_managed_exact(
        &self,
        _root: ManagedRoot,
        _relative_path: &str,
        _expected_evidence: &FileEvidence,
        _expected_sha256: &[u8; 32],
    ) -> Result<ManagedDeleteOutcome, PortError> {
        Err(PortError::new(
            "STORAGE_DELETE_UNSUPPORTED",
            "exact managed deletion is not supported by this storage adapter",
        ))
    }

    fn list_incoming(&self) -> Result<Vec<String>, PortError>;

    fn read_incoming(
        &self,
        relative_path: &str,
        max_bytes: usize,
    ) -> Result<IncomingFileSnapshot, PortError>;

    fn delete_incoming_exact(
        &self,
        _relative_path: &str,
        _expected_evidence: &FileEvidence,
        _expected_bytes: &[u8],
        _max_bytes: usize,
    ) -> Result<IncomingDeleteOutcome, PortError> {
        Err(PortError::new(
            "STORAGE_DELETE_UNSUPPORTED",
            "exact Incoming deletion is not supported by this storage adapter",
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingCandidate {
    pub relative_path: String,
    pub source_evidence: FileEvidence,
    pub source_sha256: [u8; 32],
    pub metainfo: TorrentMetainfo,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingRejection {
    pub relative_path: String,
    pub problem_code: &'static str,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingRedundantCopy {
    pub canonical_path: String,
    pub canonical_evidence: FileEvidence,
    pub redundant_path: String,
    pub source_evidence: FileEvidence,
    pub source_sha256: [u8; 32],
    pub torrent_identity: qb_domain::torrent::TorrentIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingAlreadyProcessed {
    pub relative_path: String,
    pub registry_id: String,
    pub registry_state: RegistryState,
    pub registered_source_relative: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingScan {
    pub eligible: Vec<IncomingCandidate>,
    pub already_processed: Vec<IncomingAlreadyProcessed>,
    pub redundant_identical: Vec<IncomingRedundantCopy>,
    pub rejected: Vec<IncomingRejection>,
}

pub struct IncomingScanService {
    storage: Arc<dyn Storage>,
    metainfo: Arc<dyn MetainfoReader>,
    registry: Arc<dyn TorrentRegistry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRootStatus {
    pub root: ManagedRoot,
    pub path: String,
    pub volume_id: u64,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageStatusSnapshot {
    pub roots: Vec<ManagedRootStatus>,
    pub incoming: IncomingScan,
}

pub struct StorageStatusService {
    storage: Arc<dyn Storage>,
    scan: IncomingScanService,
    max_metainfo_bytes: usize,
}

impl StorageStatusService {
    pub fn new(
        storage: Arc<dyn Storage>,
        scan: IncomingScanService,
        max_metainfo_bytes: usize,
    ) -> Self {
        Self {
            storage,
            scan,
            max_metainfo_bytes,
        }
    }

    pub fn list(&self) -> Result<Vec<ManagedRootStatus>, PortError> {
        [
            ManagedRoot::Incoming,
            ManagedRoot::Archive,
            ManagedRoot::Working,
            ManagedRoot::Completed,
        ]
        .into_iter()
        .map(|root| {
            let path = self.storage.root_path(root)?;
            let volume = self.storage.volume_status(root)?;
            Ok(ManagedRootStatus {
                root,
                path,
                volume_id: volume.volume_id,
                free_bytes: volume.free_bytes,
                total_bytes: volume.total_bytes,
            })
        })
        .collect()
    }

    pub fn status(&self) -> Result<StorageStatusSnapshot, PortError> {
        Ok(StorageStatusSnapshot {
            roots: self.list()?,
            incoming: self.scan.scan(self.max_metainfo_bytes)?,
        })
    }
}

impl IncomingScanService {
    pub fn new(
        storage: Arc<dyn Storage>,
        metainfo: Arc<dyn MetainfoReader>,
        registry: Arc<dyn TorrentRegistry>,
    ) -> Self {
        Self {
            storage,
            metainfo,
            registry,
        }
    }

    pub fn scan(&self, max_metainfo_bytes: usize) -> Result<IncomingScan, PortError> {
        let paths = self.storage.list_incoming()?;
        let mut eligible = Vec::new();
        let mut rejected = Vec::new();

        for relative_path in paths {
            let snapshot = match self
                .storage
                .read_incoming(&relative_path, max_metainfo_bytes)
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    rejected.push(IncomingRejection {
                        relative_path,
                        problem_code: error.code,
                        message: error.message,
                    });
                    continue;
                }
            };

            let metainfo = match self.metainfo.parse(&snapshot.bytes) {
                Ok(metainfo) => metainfo,
                Err(error) => {
                    rejected.push(IncomingRejection {
                        relative_path: snapshot.relative_path,
                        problem_code: error.code,
                        message: error.message,
                    });
                    continue;
                }
            };

            let source_sha256 = source_digest(&snapshot.bytes);
            eligible.push(IncomingCandidate {
                relative_path: snapshot.relative_path,
                source_evidence: snapshot.evidence,
                source_sha256,
                metainfo,
            });
        }

        let (eligible, mut redundant_identical, duplicate_rejections) =
            self.reconcile_duplicates(eligible, max_metainfo_bytes);
        rejected.extend(duplicate_rejections);

        let mut fresh = Vec::new();
        let mut processed = Vec::new();
        let mut allowed_canonical_paths = BTreeMap::new();

        for candidate in eligible {
            match self.registry.find_by_identity(&candidate.metainfo.identity) {
                Ok(Some(record)) if record.source_metainfo_digest == candidate.source_sha256 => {
                    allowed_canonical_paths.insert(candidate.relative_path.clone(), ());
                    if record.state == RegistryState::Incoming && record.operation_id.is_none() {
                        fresh.push(candidate);
                    } else {
                        processed.push(classify_already_processed(candidate, record));
                    }
                }
                Ok(Some(_)) => {
                    rejected.push(IncomingRejection {
                        relative_path: candidate.relative_path,
                        problem_code: "IDENTITY_CONFLICT",
                        message: "registered torrent identity has different metainfo bytes".into(),
                    });
                }
                Ok(None) => {
                    allowed_canonical_paths.insert(candidate.relative_path.clone(), ());
                    fresh.push(candidate);
                }
                Err(error) if error.code == "IDENTITY_CONFLICT" => {
                    rejected.push(IncomingRejection {
                        relative_path: candidate.relative_path,
                        problem_code: error.code,
                        message: error.message,
                    });
                }
                Err(error) => return Err(error),
            }
        }

        redundant_identical
            .retain(|copy| allowed_canonical_paths.contains_key(&copy.canonical_path));
        processed.sort_by(|left, right| path_order(&left.relative_path, &right.relative_path));
        rejected.sort_by(|left, right| path_order(&left.relative_path, &right.relative_path));

        Ok(IncomingScan {
            eligible: fresh,
            already_processed: processed,
            redundant_identical,
            rejected,
        })
    }

    fn reconcile_duplicates(
        &self,
        candidates: Vec<IncomingCandidate>,
        max_metainfo_bytes: usize,
    ) -> (
        Vec<IncomingCandidate>,
        Vec<IncomingRedundantCopy>,
        Vec<IncomingRejection>,
    ) {
        let groups = identity_groups(&candidates);
        let mut eligible = Vec::new();
        let mut redundant = Vec::new();
        let mut rejected = Vec::new();

        for mut group in groups {
            group.sort_by(|left, right| {
                path_order(
                    &candidates[*left].relative_path,
                    &candidates[*right].relative_path,
                )
            });

            if group.len() == 1 {
                eligible.push(candidates[group[0]].clone());
                continue;
            }

            let canonical = &candidates[group[0]];
            let canonical_snapshot = match self
                .storage
                .read_incoming(&canonical.relative_path, max_metainfo_bytes)
            {
                Ok(snapshot) if snapshot_matches(canonical, &snapshot) => snapshot,
                Ok(_) => {
                    reject_group_as_ambiguous(
                        &mut rejected,
                        &candidates,
                        &group,
                        "canonical Incoming source changed after scan",
                    );
                    continue;
                }
                Err(error) => {
                    reject_group_as_ambiguous(
                        &mut rejected,
                        &candidates,
                        &group,
                        &format!(
                            "canonical Incoming source could not be re-read: {}",
                            error.code
                        ),
                    );
                    continue;
                }
            };

            let mut conflict = false;
            let mut ambiguous = None;
            for index in group.iter().copied().skip(1) {
                let candidate = &candidates[index];
                if candidate.source_sha256 != canonical.source_sha256 {
                    conflict = true;
                    break;
                }

                match self
                    .storage
                    .read_incoming(&candidate.relative_path, max_metainfo_bytes)
                {
                    Ok(snapshot)
                        if snapshot_matches(candidate, &snapshot)
                            && snapshot.bytes == canonical_snapshot.bytes => {}
                    Ok(snapshot) if !snapshot_matches(candidate, &snapshot) => {
                        ambiguous = Some(format!(
                            "Incoming source changed after scan: {}",
                            candidate.relative_path
                        ));
                        break;
                    }
                    Ok(_) => {
                        conflict = true;
                        break;
                    }
                    Err(error) => {
                        ambiguous = Some(format!(
                            "Incoming source could not be re-read: {} ({})",
                            candidate.relative_path, error.code
                        ));
                        break;
                    }
                }
            }

            if let Some(message) = ambiguous {
                reject_group_as_ambiguous(&mut rejected, &candidates, &group, &message);
                continue;
            }
            if conflict {
                for index in group {
                    rejected.push(IncomingRejection {
                        relative_path: candidates[index].relative_path.clone(),
                        problem_code: "IDENTITY_CONTENT_CONFLICT",
                        message: "same torrent identity is represented by different Incoming bytes"
                            .into(),
                    });
                }
                continue;
            }

            eligible.push(canonical.clone());
            for index in group.into_iter().skip(1) {
                let candidate = &candidates[index];
                redundant.push(IncomingRedundantCopy {
                    canonical_path: canonical.relative_path.clone(),
                    canonical_evidence: canonical.source_evidence.clone(),
                    redundant_path: candidate.relative_path.clone(),
                    source_evidence: candidate.source_evidence.clone(),
                    source_sha256: candidate.source_sha256,
                    torrent_identity: candidate.metainfo.identity.clone(),
                });
            }
        }

        eligible.sort_by(|left, right| path_order(&left.relative_path, &right.relative_path));
        redundant.sort_by(|left, right| path_order(&left.redundant_path, &right.redundant_path));
        rejected.sort_by(|left, right| path_order(&left.relative_path, &right.relative_path));
        (eligible, redundant, rejected)
    }
}

fn classify_already_processed(
    candidate: IncomingCandidate,
    record: RegistryRecord,
) -> IncomingAlreadyProcessed {
    IncomingAlreadyProcessed {
        relative_path: candidate.relative_path,
        registry_id: record.registry_id,
        registry_state: record.state,
        registered_source_relative: record.source_relative,
    }
}

fn source_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn snapshot_matches(candidate: &IncomingCandidate, snapshot: &IncomingFileSnapshot) -> bool {
    candidate.relative_path == snapshot.relative_path
        && candidate.source_evidence == snapshot.evidence
        && candidate.source_sha256 == source_digest(&snapshot.bytes)
}

fn reject_group_as_ambiguous(
    rejected: &mut Vec<IncomingRejection>,
    candidates: &[IncomingCandidate],
    group: &[usize],
    message: &str,
) {
    for index in group {
        rejected.push(IncomingRejection {
            relative_path: candidates[*index].relative_path.clone(),
            problem_code: "SOURCE_AMBIGUOUS",
            message: message.to_string(),
        });
    }
}

fn path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.to_ascii_lowercase()
        .cmp(&right.to_ascii_lowercase())
        .then_with(|| left.cmp(right))
}

fn identity_groups(candidates: &[IncomingCandidate]) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..candidates.len()).collect();

    for left in 0..candidates.len() {
        for right in (left + 1)..candidates.len() {
            if candidates[left]
                .metainfo
                .identity
                .shares_alias_with(&candidates[right].metainfo.identity)
            {
                union(&mut parent, left, right);
            }
        }
    }

    let mut groups = BTreeMap::<usize, Vec<usize>>::new();
    for index in 0..candidates.len() {
        let root = find(&mut parent, index);
        groups.entry(root).or_default().push(index);
    }
    groups.into_values().collect()
}

fn find(parent: &mut [usize], index: usize) -> usize {
    if parent[index] != index {
        parent[index] = find(parent, parent[index]);
    }
    parent[index]
}

fn union(parent: &mut [usize], left: usize, right: usize) {
    let left_root = find(parent, left);
    let right_root = find(parent, right);
    if left_root != right_root {
        parent[right_root] = left_root;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use qb_domain::torrent::{ManifestFile, TorrentIdentity, TorrentManifest, TorrentMetainfo};

    use super::*;

    struct FakeStorage {
        files: Vec<IncomingFileSnapshot>,
    }

    struct ChangingStorage {
        files: Vec<IncomingFileSnapshot>,
        changed_path: String,
        changed_reads: AtomicUsize,
    }

    impl Storage for ChangingStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            Ok(StorageVolumeStatus {
                root,
                volume_id: 7,
                free_bytes: 1_000_000,
                total_bytes: 2_000_000,
            })
        }

        fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
            Ok(match root {
                ManagedRoot::Incoming => r"C:\Managed\Incoming",
                ManagedRoot::Archive => r"C:\Managed\Archive",
                ManagedRoot::Working => r"C:\Managed\Working",
                ManagedRoot::Completed => r"C:\Managed\Completed",
                ManagedRoot::Runtime => r"C:\Managed\Runtime",
            }
            .into())
        }

        fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
            Ok(self.root_path(root)?.eq_ignore_ascii_case(observed))
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Ok(self
                .files
                .iter()
                .map(|file| file.relative_path.clone())
                .collect())
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            let mut snapshot = self
                .files
                .iter()
                .find(|file| file.relative_path == relative_path)
                .cloned()
                .ok_or_else(|| PortError::new("STORAGE_NOT_FOUND", relative_path))?;

            if relative_path == self.changed_path
                && self.changed_reads.fetch_add(1, Ordering::SeqCst) > 0
            {
                snapshot.evidence.modified_marker =
                    snapshot.evidence.modified_marker.saturating_add(1);
                snapshot.bytes.push(b'!');
            }
            Ok(snapshot)
        }
    }

    impl Storage for FakeStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            Ok(StorageVolumeStatus {
                root,
                volume_id: 7,
                free_bytes: 1_000_000,
                total_bytes: 2_000_000,
            })
        }

        fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
            Ok(match root {
                ManagedRoot::Incoming => r"C:\Managed\Incoming",
                ManagedRoot::Archive => r"C:\Managed\Archive",
                ManagedRoot::Working => r"C:\Managed\Working",
                ManagedRoot::Completed => r"C:\Managed\Completed",
                ManagedRoot::Runtime => r"C:\Managed\Runtime",
            }
            .into())
        }

        fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
            Ok(self.root_path(root)?.eq_ignore_ascii_case(observed))
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Ok(self
                .files
                .iter()
                .map(|file| file.relative_path.clone())
                .collect())
        }

        fn read_incoming(
            &self,
            relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            self.files
                .iter()
                .find(|file| file.relative_path == relative_path)
                .cloned()
                .ok_or_else(|| PortError::new("STORAGE_NOT_FOUND", relative_path))
        }
    }

    #[derive(Default)]
    struct FakeRegistry {
        records: Vec<RegistryRecord>,
        conflict_identity: Option<TorrentIdentity>,
    }

    impl TorrentRegistry for FakeRegistry {
        fn find_by_identity(
            &self,
            identity: &TorrentIdentity,
        ) -> Result<Option<RegistryRecord>, PortError> {
            if self
                .conflict_identity
                .as_ref()
                .is_some_and(|conflict| conflict.shares_alias_with(identity))
            {
                return Err(PortError::new(
                    "IDENTITY_CONFLICT",
                    "fixture registry identity conflict",
                ));
            }

            Ok(self
                .records
                .iter()
                .find(|record| record.identity.shares_alias_with(identity))
                .cloned())
        }

        fn register_incoming(
            &self,
            _candidate: &crate::registry::RegisterIncoming,
        ) -> Result<crate::registry::RegisterIncomingResult, PortError> {
            Err(PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                "read-only scan must not register",
            ))
        }
    }

    struct FakeMetainfoReader;

    impl MetainfoReader for FakeMetainfoReader {
        fn parse(&self, bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
            if bytes == b"bad" {
                return Err(PortError::new("METAINFO_INVALID", "bad fixture"));
            }

            let identity = match bytes {
                b"v1" => TorrentIdentity::new(Some([0x11; 20]), None),
                b"hybrid" => TorrentIdentity::new(Some([0x11; 20]), Some([0x22; 32])),
                b"v2" => TorrentIdentity::new(None, Some([0x22; 32])),
                b"unrelated" => TorrentIdentity::new(Some([0x33; 20]), None),
                _ => TorrentIdentity::new(Some([0x44; 20]), None),
            }
            .expect("fixture identity");

            Ok(TorrentMetainfo {
                identity,
                manifest: TorrentManifest::new(vec![ManifestFile {
                    path: "payload.bin".into(),
                    size: 4,
                }])
                .expect("fixture manifest"),
            })
        }
    }

    fn snapshot(path: &str, bytes: &[u8], file_id: u64) -> IncomingFileSnapshot {
        IncomingFileSnapshot {
            relative_path: path.into(),
            evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 7,
                    file_id,
                },
                size: u64::try_from(bytes.len()).expect("fixture size"),
                modified_marker: 9,
            },
            bytes: bytes.to_vec(),
        }
    }

    #[test]
    fn incoming_scan_selects_deterministic_canonical_for_exact_duplicates() {
        let storage = Arc::new(FakeStorage {
            files: vec![
                snapshot("z.torrent", b"same", 1),
                snapshot("A.torrent", b"same", 2),
            ],
        });
        let service = IncomingScanService::new(
            storage,
            Arc::new(FakeMetainfoReader),
            Arc::new(FakeRegistry::default()),
        );

        let scan = service.scan(1024).expect("scan");

        assert_eq!(scan.eligible.len(), 1);
        assert_eq!(scan.eligible[0].relative_path, "A.torrent");
        assert!(scan.already_processed.is_empty());
        assert_eq!(scan.redundant_identical.len(), 1);
        assert_eq!(scan.redundant_identical[0].canonical_path, "A.torrent");
        assert_eq!(scan.redundant_identical[0].redundant_path, "z.torrent");
        assert!(scan.rejected.is_empty());
    }

    #[test]
    fn incoming_scan_marks_duplicate_group_ambiguous_if_source_changes_on_reread() {
        let storage = Arc::new(ChangingStorage {
            files: vec![
                snapshot("a.torrent", b"same", 1),
                snapshot("b.torrent", b"same", 2),
            ],
            changed_path: "b.torrent".into(),
            changed_reads: AtomicUsize::new(0),
        });
        let service = IncomingScanService::new(
            storage,
            Arc::new(FakeMetainfoReader),
            Arc::new(FakeRegistry::default()),
        );

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert!(scan.already_processed.is_empty());
        assert!(scan.redundant_identical.is_empty());
        assert_eq!(scan.rejected.len(), 2);
        assert!(scan
            .rejected
            .iter()
            .all(|entry| entry.problem_code == "SOURCE_AMBIGUOUS"));
    }

    #[test]
    fn incoming_scan_blocks_same_identity_when_bytes_differ() {
        let storage = Arc::new(FakeStorage {
            files: vec![
                snapshot("a.torrent", b"first", 1),
                snapshot("b.torrent", b"second", 2),
            ],
        });
        let service = IncomingScanService::new(
            storage,
            Arc::new(FakeMetainfoReader),
            Arc::new(FakeRegistry::default()),
        );

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert!(scan.already_processed.is_empty());
        assert!(scan.redundant_identical.is_empty());
        assert_eq!(scan.rejected.len(), 2);
        assert!(scan
            .rejected
            .iter()
            .all(|entry| entry.problem_code == "IDENTITY_CONTENT_CONFLICT"));
    }

    #[test]
    fn incoming_scan_groups_v1_v2_aliases_transitively_through_hybrid_identity() {
        let storage = Arc::new(FakeStorage {
            files: vec![
                snapshot("a-v1.torrent", b"v1", 1),
                snapshot("b-hybrid.torrent", b"hybrid", 2),
                snapshot("c-v2.torrent", b"v2", 3),
                snapshot("d-other.torrent", b"unrelated", 4),
            ],
        });
        let service = IncomingScanService::new(
            storage,
            Arc::new(FakeMetainfoReader),
            Arc::new(FakeRegistry::default()),
        );

        let scan = service.scan(1024).expect("scan");

        assert_eq!(scan.eligible.len(), 1);
        assert_eq!(scan.eligible[0].relative_path, "d-other.torrent");
        assert!(scan.already_processed.is_empty());
        assert_eq!(scan.rejected.len(), 3);
        assert!(scan
            .rejected
            .iter()
            .all(|entry| entry.problem_code == "IDENTITY_CONTENT_CONFLICT"));
    }

    #[test]
    fn incoming_scan_classifies_registered_identity_as_already_processed() {
        let storage = Arc::new(FakeStorage {
            files: vec![snapshot("known.torrent", b"v1", 1)],
        });
        let identity = TorrentIdentity::new(Some([0x11; 20]), None).expect("identity");
        let registry = Arc::new(FakeRegistry {
            records: vec![RegistryRecord {
                registry_id: "registry-1".into(),
                identity,
                state: RegistryState::Finished,
                source_relative: "archive/known.torrent".into(),
                source_metainfo_digest: source_digest(b"v1"),
                operation_id: Some("operation-1".into()),
                archive_ref: Some("archive/known.torrent".into()),
                handoff_file_count: 1,
                handoff_receipt_count: 1,
            }],
            conflict_identity: None,
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader), registry);

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert_eq!(scan.already_processed.len(), 1);
        assert_eq!(scan.already_processed[0].relative_path, "known.torrent");
        assert_eq!(scan.already_processed[0].registry_id, "registry-1");
        assert_eq!(
            scan.already_processed[0].registry_state,
            RegistryState::Finished
        );
        assert!(scan.rejected.is_empty());
    }

    #[test]
    fn incoming_scan_retries_detached_incoming_registry_identity() {
        let storage = Arc::new(FakeStorage {
            files: vec![snapshot("retry.torrent", b"v1", 1)],
        });
        let registry = Arc::new(FakeRegistry {
            records: vec![RegistryRecord {
                registry_id: "registry-retry".into(),
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                state: RegistryState::Incoming,
                source_relative: "retry.torrent".into(),
                source_metainfo_digest: source_digest(b"v1"),
                operation_id: None,
                archive_ref: None,
                handoff_file_count: 0,
                handoff_receipt_count: 0,
            }],
            conflict_identity: None,
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader), registry);

        let scan = service.scan(1024).expect("scan");

        assert_eq!(scan.eligible.len(), 1);
        assert_eq!(scan.eligible[0].relative_path, "retry.torrent");
        assert!(scan.already_processed.is_empty());
        assert!(scan.rejected.is_empty());
    }

    #[test]
    fn incoming_scan_does_not_readd_registry_identity_with_active_operation() {
        let storage = Arc::new(FakeStorage {
            files: vec![snapshot("active.torrent", b"v1", 1)],
        });
        let registry = Arc::new(FakeRegistry {
            records: vec![RegistryRecord {
                registry_id: "registry-active".into(),
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                state: RegistryState::Incoming,
                source_relative: "active.torrent".into(),
                source_metainfo_digest: source_digest(b"v1"),
                operation_id: Some("operation-active".into()),
                archive_ref: None,
                handoff_file_count: 0,
                handoff_receipt_count: 0,
            }],
            conflict_identity: None,
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader), registry);

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert_eq!(scan.already_processed.len(), 1);
        assert_eq!(scan.already_processed[0].registry_id, "registry-active");
        assert!(scan.rejected.is_empty());
    }

    #[test]
    fn incoming_scan_rejects_registered_identity_with_different_metainfo_bytes() {
        let storage = Arc::new(FakeStorage {
            files: vec![snapshot("changed.torrent", b"v1", 1)],
        });
        let identity = TorrentIdentity::new(Some([0x11; 20]), None).expect("identity");
        let registry = Arc::new(FakeRegistry {
            records: vec![RegistryRecord {
                registry_id: "registry-1".into(),
                identity,
                state: RegistryState::Incoming,
                source_relative: "original.torrent".into(),
                source_metainfo_digest: [0x77; 32],
                operation_id: None,
                archive_ref: None,
                handoff_file_count: 0,
                handoff_receipt_count: 0,
            }],
            conflict_identity: None,
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader), registry);

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert!(scan.already_processed.is_empty());
        assert!(scan.redundant_identical.is_empty());
        assert_eq!(scan.rejected.len(), 1);
        assert_eq!(scan.rejected[0].problem_code, "IDENTITY_CONFLICT");
    }

    #[test]
    fn incoming_scan_reports_registry_identity_conflict_without_mutation() {
        let storage = Arc::new(FakeStorage {
            files: vec![snapshot("conflict.torrent", b"v1", 1)],
        });
        let registry = Arc::new(FakeRegistry {
            records: Vec::new(),
            conflict_identity: Some(
                TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
            ),
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader), registry);

        let scan = service.scan(1024).expect("scan");

        assert!(scan.eligible.is_empty());
        assert!(scan.already_processed.is_empty());
        assert!(scan.redundant_identical.is_empty());
        assert_eq!(scan.rejected.len(), 1);
        assert_eq!(scan.rejected[0].problem_code, "IDENTITY_CONFLICT");
    }

    #[test]
    fn incoming_scan_keeps_invalid_metainfo_as_per_candidate_rejection() {
        let storage = Arc::new(FakeStorage {
            files: vec![
                snapshot("a.torrent", b"good", 1),
                snapshot("b.torrent", b"bad", 2),
            ],
        });
        let service = IncomingScanService::new(
            storage,
            Arc::new(FakeMetainfoReader),
            Arc::new(FakeRegistry::default()),
        );

        let scan = service.scan(1024).expect("scan");

        assert_eq!(scan.eligible.len(), 1);
        assert_eq!(scan.eligible[0].relative_path, "a.torrent");
        assert_ne!(scan.eligible[0].source_sha256, [0; 32]);
        assert!(scan.already_processed.is_empty());
        assert!(scan.redundant_identical.is_empty());
        assert_eq!(scan.rejected.len(), 1);
        assert_eq!(scan.rejected[0].relative_path, "b.torrent");
        assert_eq!(scan.rejected[0].problem_code, "METAINFO_INVALID");
    }
}
