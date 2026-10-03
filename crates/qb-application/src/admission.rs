use qb_domain::{torrent::TorrentIdentity, OperationId, RequestId};
use sha2::{Digest, Sha256};

use crate::{
    mutation::MutationDisposition,
    storage::{FileEvidence, IncomingScan, ManagedRoot, Storage},
    PortError,
};

pub const ADMISSION_FINGERPRINT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionReservationRequest {
    pub request_id: RequestId,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub reserved_bytes: u64,
    pub working_save_path: String,
}

impl AdmissionReservationRequest {
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"qbctl-admission-fingerprint-v1\0");
        digest.update(self.request_id.as_str().as_bytes());
        digest.update([0]);
        if let Some(v1) = self.identity.v1 {
            digest.update(b"v1");
            digest.update(v1);
        }
        digest.update([0]);
        if let Some(v2) = self.identity.v2 {
            digest.update(b"v2");
            digest.update(v2);
        }
        digest.update([0]);
        digest.update(self.source_relative.as_bytes());
        digest.update([0]);
        digest.update(self.source_evidence.identity.volume_id.to_be_bytes());
        digest.update(self.source_evidence.identity.file_id.to_be_bytes());
        digest.update(self.source_evidence.size.to_be_bytes());
        digest.update(self.source_evidence.modified_marker.to_be_bytes());
        digest.update(self.source_metainfo_digest);
        digest.update(self.working_volume_id.to_be_bytes());
        digest.update(self.reserved_bytes.to_be_bytes());
        digest.update(self.working_save_path.as_bytes());
        digest.finalize().into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionRecord {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub registry_id: String,
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_evidence: FileEvidence,
    pub source_metainfo_digest: [u8; 32],
    pub working_volume_id: u64,
    pub reserved_bytes: u64,
    pub working_save_path: String,
    pub checkpoint: String,
    pub disposition: MutationDisposition,
    pub pending_effect_kind: Option<String>,
    pub problem_code: Option<String>,
    pub revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdmissionReservationResult {
    New(AdmissionRecord),
    Replay(AdmissionRecord),
    Conflict { operation_id: OperationId },
}

pub trait AdmissionJournal: Send + Sync {
    fn reserve_admission(
        &self,
        request: &AdmissionReservationRequest,
    ) -> Result<AdmissionReservationResult, PortError>;

    fn get_admission(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<AdmissionRecord>, PortError>;

    fn list_recoverable_admissions(&self) -> Result<Vec<AdmissionRecord>, PortError>;

    fn capacity_reservations(
        &self,
        working_volume_id: u64,
    ) -> Result<Vec<CapacityReservation>, PortError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityReservation {
    pub responsible: String,
    pub volume_id: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityCandidate {
    pub key: String,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapacityDisposition {
    Accepted,
    DeferredInsufficientCapacity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityExplanation {
    pub volume_id: u64,
    pub free_bytes: u64,
    pub already_reserved_bytes: u64,
    pub candidate_bytes: u64,
    pub reserve_bytes: u64,
    pub required_bytes: u64,
    pub shortfall_bytes: u64,
    pub reservations: Vec<CapacityReservation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapacityDecision {
    pub candidate: CapacityCandidate,
    pub disposition: CapacityDisposition,
    pub explanation: CapacityExplanation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionCapacityPlan {
    pub decisions: Vec<CapacityDecision>,
    pub final_reserved_bytes: u64,
}

pub fn plan_incoming_capacity_from_storage(
    storage: &dyn Storage,
    scan: &IncomingScan,
    reserve_bytes: u64,
    existing_reservations: Vec<CapacityReservation>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let volume = storage.volume_status(ManagedRoot::Working)?;
    plan_incoming_capacity(
        scan,
        volume.volume_id,
        volume.free_bytes,
        reserve_bytes,
        existing_reservations,
    )
}

pub fn plan_incoming_capacity(
    scan: &IncomingScan,
    volume_id: u64,
    free_bytes: u64,
    reserve_bytes: u64,
    existing_reservations: Vec<CapacityReservation>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let candidates = scan
        .eligible
        .iter()
        .map(|candidate| CapacityCandidate {
            key: candidate.relative_path.clone(),
            bytes: candidate.metainfo.manifest.total_size,
        })
        .collect();
    plan_capacity(
        volume_id,
        free_bytes,
        reserve_bytes,
        existing_reservations,
        candidates,
    )
}

pub fn plan_capacity(
    volume_id: u64,
    free_bytes: u64,
    reserve_bytes: u64,
    reservations: Vec<CapacityReservation>,
    mut candidates: Vec<CapacityCandidate>,
) -> Result<AdmissionCapacityPlan, PortError> {
    let mut reservations: Vec<CapacityReservation> = reservations
        .into_iter()
        .filter(|reservation| reservation.volume_id == volume_id)
        .collect();
    reservations.sort_by(|left, right| capacity_key_order(&left.responsible, &right.responsible));
    candidates.sort_by(|left, right| capacity_key_order(&left.key, &right.key));

    let mut reserved_bytes = checked_sum(
        reservations.iter().map(|reservation| reservation.bytes),
        "existing capacity reservations",
    )?;
    let mut decisions = Vec::with_capacity(candidates.len());

    for candidate in candidates {
        let required_bytes = reserved_bytes
            .checked_add(candidate.bytes)
            .and_then(|value| value.checked_add(reserve_bytes))
            .ok_or_else(|| {
                PortError::new(
                    "INTERNAL_INVARIANT_VIOLATION",
                    "admission capacity calculation overflow",
                )
            })?;
        let shortfall_bytes = required_bytes.saturating_sub(free_bytes);
        let disposition = if shortfall_bytes == 0 {
            CapacityDisposition::Accepted
        } else {
            CapacityDisposition::DeferredInsufficientCapacity
        };

        decisions.push(CapacityDecision {
            candidate: candidate.clone(),
            disposition,
            explanation: CapacityExplanation {
                volume_id,
                free_bytes,
                already_reserved_bytes: reserved_bytes,
                candidate_bytes: candidate.bytes,
                reserve_bytes,
                required_bytes,
                shortfall_bytes,
                reservations: reservations.clone(),
            },
        });

        if disposition == CapacityDisposition::Accepted {
            reserved_bytes = reserved_bytes.checked_add(candidate.bytes).ok_or_else(|| {
                PortError::new(
                    "INTERNAL_INVARIANT_VIOLATION",
                    "admission reservation total overflow",
                )
            })?;
            reservations.push(CapacityReservation {
                responsible: candidate.key,
                volume_id,
                bytes: candidate.bytes,
            });
        }
    }

    Ok(AdmissionCapacityPlan {
        decisions,
        final_reserved_bytes: reserved_bytes,
    })
}

fn checked_sum(values: impl IntoIterator<Item = u64>, context: &str) -> Result<u64, PortError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total.checked_add(value).ok_or_else(|| {
            PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                format!("{context} overflow"),
            )
        })
    })
}

fn capacity_key_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.to_ascii_lowercase()
        .cmp(&right.to_ascii_lowercase())
        .then_with(|| left.cmp(right))
}

#[cfg(test)]
mod tests {
    use qb_domain::torrent::{ManifestFile, TorrentIdentity, TorrentManifest, TorrentMetainfo};

    use crate::storage::{
        FileEvidence, FileIdentity, IncomingCandidate, IncomingFileSnapshot, IncomingScan,
        StorageVolumeStatus,
    };

    use super::*;

    struct CapacityStorage;

    impl Storage for CapacityStorage {
        fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
            assert_eq!(root, ManagedRoot::Working);
            Ok(StorageVolumeStatus {
                root,
                volume_id: 7,
                free_bytes: 120,
                total_bytes: 1_000,
            })
        }

        fn list_incoming(&self) -> Result<Vec<String>, PortError> {
            Err(PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                "capacity planning must not rescan Incoming",
            ))
        }

        fn read_incoming(
            &self,
            _relative_path: &str,
            _max_bytes: usize,
        ) -> Result<IncomingFileSnapshot, PortError> {
            Err(PortError::new(
                "INTERNAL_INVARIANT_VIOLATION",
                "capacity planning must not reread Incoming",
            ))
        }
    }

    #[test]
    fn admission_fingerprint_covers_source_and_capacity_identity() {
        let base = AdmissionReservationRequest {
            request_id: RequestId::new("admission-1").expect("request id"),
            identity: TorrentIdentity::new(Some([0x11; 20]), Some([0x22; 32]))
                .expect("identity"),
            source_relative: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: crate::storage::FileIdentity {
                    volume_id: 7,
                    file_id: 9,
                },
                size: 123,
                modified_marker: 456,
            },
            source_metainfo_digest: [0x33; 32],
            working_volume_id: 42,
            reserved_bytes: 1_000,
            working_save_path: r"C:\Managed\Working".into(),
        };
        let mut changed = base.clone();
        changed.source_evidence.modified_marker += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.reserved_bytes += 1;
        assert_ne!(base.fingerprint(), changed.fingerprint());

        changed = base.clone();
        changed.working_save_path.push_str("-other");
        assert_ne!(base.fingerprint(), changed.fingerprint());

        assert_eq!(base.fingerprint(), base.fingerprint());
    }

    #[test]
    fn incoming_capacity_reads_fresh_working_volume_status() {
        let candidate = IncomingCandidate {
            relative_path: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 1,
                    file_id: 2,
                },
                size: 3,
                modified_marker: 4,
            },
            source_sha256: [0x55; 32],
            metainfo: TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                manifest: TorrentManifest::new(vec![ManifestFile {
                    path: "payload.bin".into(),
                    size: 90,
                }])
                .expect("manifest"),
            },
        };
        let scan = IncomingScan {
            eligible: vec![candidate],
            already_processed: Vec::new(),
            redundant_identical: Vec::new(),
            rejected: Vec::new(),
        };

        let plan = plan_incoming_capacity_from_storage(&CapacityStorage, &scan, 10, Vec::new())
            .expect("capacity plan");

        assert_eq!(plan.decisions[0].explanation.volume_id, 7);
        assert_eq!(plan.decisions[0].explanation.free_bytes, 120);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 100);
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
    }

    #[test]
    fn defers_large_candidate_and_continues_with_later_independent_candidate() {
        let plan = plan_capacity(
            7,
            100,
            10,
            vec![CapacityReservation {
                responsible: "existing-operation".into(),
                volume_id: 7,
                bytes: 20,
            }],
            vec![
                CapacityCandidate {
                    key: "a-large.torrent".into(),
                    bytes: 80,
                },
                CapacityCandidate {
                    key: "b-small.torrent".into(),
                    bytes: 30,
                },
            ],
        )
        .expect("plan");

        assert_eq!(plan.decisions.len(), 2);
        assert_eq!(
            plan.decisions[0].disposition,
            CapacityDisposition::DeferredInsufficientCapacity
        );
        assert_eq!(plan.decisions[0].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 110);
        assert_eq!(plan.decisions[0].explanation.shortfall_bytes, 10);

        assert_eq!(plan.decisions[1].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.decisions[1].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.decisions[1].explanation.required_bytes, 60);
        assert_eq!(plan.decisions[1].explanation.shortfall_bytes, 0);
        assert_eq!(plan.final_reserved_bytes, 50);
    }

    #[test]
    fn accepted_candidate_reserves_budget_for_following_candidates() {
        let plan = plan_capacity(
            7,
            100,
            10,
            Vec::new(),
            vec![
                CapacityCandidate {
                    key: "b-second.torrent".into(),
                    bytes: 60,
                },
                CapacityCandidate {
                    key: "a-first.torrent".into(),
                    bytes: 30,
                },
            ],
        )
        .expect("plan");

        assert_eq!(plan.decisions[0].candidate.key, "a-first.torrent");
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.decisions[1].candidate.key, "b-second.torrent");
        assert_eq!(plan.decisions[1].explanation.already_reserved_bytes, 30);
        assert_eq!(plan.decisions[1].disposition, CapacityDisposition::Accepted);
        assert_eq!(plan.final_reserved_bytes, 90);
    }

    #[test]
    fn plan_exposes_reservations_responsible_for_reserved_amount() {
        let plan = plan_capacity(
            7,
            100,
            5,
            vec![
                CapacityReservation {
                    responsible: "operation-z".into(),
                    volume_id: 7,
                    bytes: 7,
                },
                CapacityReservation {
                    responsible: "operation-a".into(),
                    volume_id: 7,
                    bytes: 8,
                },
            ],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 20,
            }],
        )
        .expect("plan");

        let explanation = &plan.decisions[0].explanation;
        assert_eq!(explanation.already_reserved_bytes, 15);
        assert_eq!(
            explanation
                .reservations
                .iter()
                .map(|reservation| reservation.responsible.as_str())
                .collect::<Vec<_>>(),
            vec!["operation-a", "operation-z"]
        );
    }

    #[test]
    fn incoming_plan_uses_manifest_total_size_and_only_eligible_candidates() {
        let candidate = IncomingCandidate {
            relative_path: "candidate.torrent".into(),
            source_evidence: FileEvidence {
                identity: FileIdentity {
                    volume_id: 1,
                    file_id: 2,
                },
                size: 3,
                modified_marker: 4,
            },
            source_sha256: [0x55; 32],
            metainfo: TorrentMetainfo {
                identity: TorrentIdentity::new(Some([0x11; 20]), None).expect("identity"),
                manifest: TorrentManifest::new(vec![
                    ManifestFile {
                        path: "a.bin".into(),
                        size: 40,
                    },
                    ManifestFile {
                        path: "b.bin".into(),
                        size: 50,
                    },
                ])
                .expect("manifest"),
            },
        };
        let scan = IncomingScan {
            eligible: vec![candidate],
            already_processed: Vec::new(),
            redundant_identical: Vec::new(),
            rejected: Vec::new(),
        };

        let plan = plan_incoming_capacity(&scan, 7, 120, 10, Vec::new()).expect("plan");

        assert_eq!(plan.decisions.len(), 1);
        assert_eq!(plan.decisions[0].candidate.bytes, 90);
        assert_eq!(plan.decisions[0].explanation.required_bytes, 100);
        assert_eq!(plan.decisions[0].disposition, CapacityDisposition::Accepted);
    }

    #[test]
    fn capacity_plan_ignores_reservations_on_other_volumes() {
        let plan = plan_capacity(
            7,
            100,
            10,
            vec![
                CapacityReservation {
                    responsible: "same-volume".into(),
                    volume_id: 7,
                    bytes: 20,
                },
                CapacityReservation {
                    responsible: "other-volume".into(),
                    volume_id: 9,
                    bytes: 70,
                },
            ],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 30,
            }],
        )
        .expect("plan");

        assert_eq!(plan.decisions[0].explanation.volume_id, 7);
        assert_eq!(plan.decisions[0].explanation.already_reserved_bytes, 20);
        assert_eq!(plan.final_reserved_bytes, 50);
        assert_eq!(plan.decisions[0].explanation.reservations.len(), 1);
        assert_eq!(
            plan.decisions[0].explanation.reservations[0].responsible,
            "same-volume"
        );
    }

    #[test]
    fn capacity_overflow_fails_closed() {
        let error = plan_capacity(
            7,
            u64::MAX,
            1,
            vec![CapacityReservation {
                responsible: "existing".into(),
                volume_id: 7,
                bytes: u64::MAX,
            }],
            vec![CapacityCandidate {
                key: "candidate.torrent".into(),
                bytes: 1,
            }],
        )
        .expect_err("overflow must fail");

        assert_eq!(error.code, "INTERNAL_INVARIANT_VIOLATION");
    }
}
