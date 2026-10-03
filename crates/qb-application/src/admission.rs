use crate::{storage::IncomingScan, PortError};

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

    use crate::storage::{FileEvidence, FileIdentity, IncomingCandidate, IncomingScan};

    use super::*;

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
