use std::sync::Arc;

use qb_domain::torrent::TorrentMetainfo;
use sha2::{Digest, Sha256};

use crate::{torrent::MetainfoReader, PortError};

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

pub trait Storage: Send + Sync {
    fn list_incoming(&self) -> Result<Vec<String>, PortError>;

    fn read_incoming(
        &self,
        relative_path: &str,
        max_bytes: usize,
    ) -> Result<IncomingFileSnapshot, PortError>;
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
pub struct IncomingScan {
    pub eligible: Vec<IncomingCandidate>,
    pub rejected: Vec<IncomingRejection>,
}

pub struct IncomingScanService {
    storage: Arc<dyn Storage>,
    metainfo: Arc<dyn MetainfoReader>,
}

impl IncomingScanService {
    pub fn new(storage: Arc<dyn Storage>, metainfo: Arc<dyn MetainfoReader>) -> Self {
        Self { storage, metainfo }
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

            let source_sha256 = Sha256::digest(&snapshot.bytes).into();
            eligible.push(IncomingCandidate {
                relative_path: snapshot.relative_path,
                source_evidence: snapshot.evidence,
                source_sha256,
                metainfo,
            });
        }

        Ok(IncomingScan { eligible, rejected })
    }
}

#[cfg(test)]
mod tests {
    use qb_domain::torrent::{
        ManifestFile, TorrentIdentity, TorrentManifest, TorrentMetainfo,
    };

    use super::*;

    struct FakeStorage {
        files: Vec<IncomingFileSnapshot>,
    }

    impl Storage for FakeStorage {
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

    struct FakeMetainfoReader;

    impl MetainfoReader for FakeMetainfoReader {
        fn parse(&self, bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
            if bytes == b"bad" {
                return Err(PortError::new("METAINFO_INVALID", "bad fixture"));
            }

            Ok(TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None)
                    .expect("fixture identity"),
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
    fn incoming_scan_keeps_invalid_metainfo_as_per_candidate_rejection() {
        let storage = Arc::new(FakeStorage {
            files: vec![
                snapshot("a.torrent", b"good", 1),
                snapshot("b.torrent", b"bad", 2),
            ],
        });
        let service = IncomingScanService::new(storage, Arc::new(FakeMetainfoReader));

        let scan = service.scan(1024).expect("scan");

        assert_eq!(scan.eligible.len(), 1);
        assert_eq!(scan.eligible[0].relative_path, "a.torrent");
        assert_ne!(scan.eligible[0].source_sha256, [0; 32]);
        assert_eq!(scan.rejected.len(), 1);
        assert_eq!(scan.rejected[0].relative_path, "b.torrent");
        assert_eq!(scan.rejected[0].problem_code, "METAINFO_INVALID");
    }
}
