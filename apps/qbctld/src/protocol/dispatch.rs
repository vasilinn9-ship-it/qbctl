use qb_application::{
    completion::{
        CompletionExecution, CompletionExecutionStatus, CompletionJournal, CompletionService,
    },
    mutation::{
        MutationCommand, MutationExecution, MutationExecutionResult, MutationExecutionStatus,
        MutationService, TorrentControlAction,
    },
    storage::StorageStatusService,
    system::SystemService,
    torrent::TorrentService,
    PortError,
};
use qb_domain::{torrent::TorrentId, OperationId, RequestId};
use qb_proto::{
    v1::{
        request, response, CapabilitiesResponse, DoctorCheck, DoctorResponse, MutationCertainty,
        MutationResultResponse, NextAction, OperationGetResponse, OperationListResponse,
        OperationRecoverResponse, Problem, ProblemCategory, QueueTargetResponse, Request, Response,
        RetryGuidance, Status, TorrentDiagnoseResponse, TorrentGetResponse,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

#[derive(Clone, Copy, Default)]
pub struct OperationServices<'a> {
    pub mutations: Option<&'a MutationService>,
    pub completion: Option<&'a CompletionService>,
    pub completion_journal: Option<&'a dyn CompletionJournal>,
}

pub async fn dispatch(
    request: Request,
    system: &SystemService,
    storage: Option<&StorageStatusService>,
    torrents: Option<&TorrentService>,
    operations: OperationServices<'_>,
    mutation_admission_enabled: bool,
    qbit_startup_problem: Option<&str>,
) -> Response {
    let sequence = request.sequence;
    let request_id = request.request_id.clone();
    let mutations = operations.mutations;
    let completion = operations.completion;
    let completion_journal = operations.completion_journal;

    match request.command {
        Some(request::Command::Capabilities(_)) => success(
            sequence,
            request_id,
            response::Payload::Capabilities(CapabilitiesResponse {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                daemon_version: env!("CARGO_PKG_VERSION").to_string(),
                capabilities: super::capabilities(),
            }),
        ),
        Some(request::Command::Status(_)) => match system.status() {
            Ok(status) => success(
                sequence,
                request_id,
                response::Payload::SystemStatus(super::encode::system_status(status)),
            ),
            Err(error) => port_error(sequence, request_id, error),
        },
        Some(request::Command::StorageList(_)) => {
            let Some(service) = storage else {
                return state_problem(
                    sequence,
                    request_id,
                    "STORAGE_NOT_CONFIGURED",
                    "managed storage is not configured",
                );
            };
            match service.list() {
                Ok(roots) => success(
                    sequence,
                    request_id,
                    response::Payload::StorageList(super::encode::storage_list(roots)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::StorageStatus(_)) => {
            let Some(service) = storage else {
                return state_problem(
                    sequence,
                    request_id,
                    "STORAGE_NOT_CONFIGURED",
                    "managed storage is not configured",
                );
            };
            match service.status() {
                Ok(status) => success(
                    sequence,
                    request_id,
                    response::Payload::StorageStatus(super::encode::storage_status(status)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::OperationList(_)) => {
            let Some(journal) = completion_journal else {
                return internal_unavailable(
                    sequence,
                    request_id,
                    "completion operation journal unavailable",
                );
            };
            match journal.list_completions() {
                Ok(records) => success(
                    sequence,
                    request_id,
                    response::Payload::OperationList(OperationListResponse {
                        operations: records
                            .iter()
                            .map(super::encode::operation_summary)
                            .collect(),
                    }),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::OperationGet(command)) => {
            let Some(journal) = completion_journal else {
                return internal_unavailable(
                    sequence,
                    request_id,
                    "completion operation journal unavailable",
                );
            };
            let operation_id = match OperationId::new(command.operation_id) {
                Ok(value) => value,
                Err(error) => return invalid_request(sequence, request_id, &error.to_string()),
            };
            match journal.get_completion(&operation_id) {
                Ok(Some(record)) => success(
                    sequence,
                    request_id,
                    response::Payload::OperationGet(OperationGetResponse {
                        operation: Some(super::encode::operation_view(record)),
                    }),
                ),
                Ok(None) => state_problem(
                    sequence,
                    request_id,
                    "OPERATION_NOT_FOUND",
                    "completion operation was not found",
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::OperationRecover(command)) => {
            let Some(service) = completion else {
                return internal_unavailable(
                    sequence,
                    request_id,
                    "completion operation service unavailable",
                );
            };
            let recovery_request_id = match require_request_id(request_id.as_deref()) {
                Ok(value) => value,
                Err(error) => return invalid_request(sequence, request_id, &error),
            };
            let operation_id = match OperationId::new(command.operation_id) {
                Ok(value) => value,
                Err(error) => return invalid_request(sequence, request_id, &error.to_string()),
            };
            match service
                .recover_operation(&operation_id, &recovery_request_id)
                .await
            {
                Ok(Some(execution)) => {
                    completion_execution_response(sequence, request_id, execution)
                }
                Ok(None) => state_problem(
                    sequence,
                    request_id,
                    "OPERATION_NOT_FOUND",
                    "completion operation was not found",
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::Doctor(_)) => success(
            sequence,
            request_id,
            response::Payload::Doctor(
                doctor_response(system, torrents, qbit_startup_problem).await,
            ),
        ),
        Some(request::Command::TorrentList(_)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            match service.list().await {
                Ok(values) => success(
                    sequence,
                    request_id,
                    response::Payload::TorrentList(super::encode::torrent_list(values)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::TorrentGet(command)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            let id = match TorrentId::new(&command.torrent_id) {
                Ok(id) => id,
                Err(error) => {
                    return invalid_request(sequence, request_id, &error.to_string());
                }
            };

            match service.get(&id).await {
                Ok(Some(value)) => success(
                    sequence,
                    request_id,
                    response::Payload::TorrentGet(TorrentGetResponse {
                        torrent: Some(super::encode::torrent_summary(value)),
                    }),
                ),
                Ok(None) => state_problem(
                    sequence,
                    request_id,
                    "TORRENT_NOT_FOUND",
                    "torrent was not found",
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::TorrentDiagnose(command)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            let id = match TorrentId::new(&command.torrent_id) {
                Ok(id) => id,
                Err(error) => {
                    return invalid_request(sequence, request_id, &error.to_string());
                }
            };

            let torrent = match service.get(&id).await {
                Ok(Some(value)) => value,
                Ok(None) => {
                    return state_problem(
                        sequence,
                        request_id,
                        "TORRENT_NOT_FOUND",
                        "torrent was not found",
                    );
                }
                Err(error) => return port_error(sequence, request_id, error),
            };
            let trackers = match service.trackers(&id).await {
                Ok(values) => values,
                Err(error) => return port_error(sequence, request_id, error),
            };

            success(
                sequence,
                request_id,
                response::Payload::TorrentDiagnose(TorrentDiagnoseResponse {
                    torrent: Some(super::encode::torrent_summary(torrent)),
                    trackers: trackers
                        .into_iter()
                        .map(super::encode::tracker_evidence)
                        .collect(),
                }),
            )
        }
        Some(request::Command::QueueGet(_)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            match service.queue_settings().await {
                Ok(value) => success(
                    sequence,
                    request_id,
                    response::Payload::QueueSettings(super::encode::queue_settings(value)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::QueueTargetGet(_)) => {
            let Some(service) = mutations else {
                return internal_unavailable(sequence, request_id, "mutation service unavailable");
            };
            match service.queue_target() {
                Ok(target_client_count) => success(
                    sequence,
                    request_id,
                    response::Payload::QueueTarget(QueueTargetResponse {
                        target_client_count: target_client_count.target_client_count,
                        revision: target_client_count.revision,
                    }),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::TransferLimitsGet(_)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            match service.transfer_info().await {
                Ok(value) => success(
                    sequence,
                    request_id,
                    response::Payload::TransferLimits(super::encode::transfer_limits(value)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::QbitProbe(_)) => {
            let Some(service) = torrents else {
                return qbit_unavailable(sequence, request_id, qbit_startup_problem);
            };
            match service.probe().await {
                Ok(value) => success(
                    sequence,
                    request_id,
                    response::Payload::QbitProbe(super::encode::qbit_probe(value)),
                ),
                Err(error) => port_error(sequence, request_id, error),
            }
        }
        Some(request::Command::TorrentPause(command)) => {
            let id = match TorrentId::new(&command.torrent_id) {
                Ok(id) => id,
                Err(error) => {
                    return invalid_request(sequence, request_id, &error.to_string());
                }
            };
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::TorrentControl {
                    torrent_id: id,
                    action: TorrentControlAction::Stop,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        Some(request::Command::TorrentResume(command)) => {
            let id = match TorrentId::new(&command.torrent_id) {
                Ok(id) => id,
                Err(error) => {
                    return invalid_request(sequence, request_id, &error.to_string());
                }
            };
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::TorrentControl {
                    torrent_id: id,
                    action: TorrentControlAction::Start,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        Some(request::Command::QueueTargetSet(command)) => {
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::SetQueueTarget {
                    target_client_count: command.target_client_count,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        Some(request::Command::QueueDownloadsSet(command)) => {
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::SetActiveDownloads {
                    max_active_downloads: command.max_active_downloads,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        Some(request::Command::TransferDownloadLimitSet(command)) => {
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::SetDownloadLimit {
                    bytes_per_sec: command.bytes_per_sec,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        Some(request::Command::TransferUploadLimitSet(command)) => {
            dispatch_mutation(
                sequence,
                request_id,
                MutationCommand::SetUploadLimit {
                    bytes_per_sec: command.bytes_per_sec,
                },
                mutations,
                mutation_admission_enabled,
            )
            .await
        }
        None => invalid_request(sequence, request_id, "command is required"),
    }
}

async fn doctor_response(
    system: &SystemService,
    torrents: Option<&TorrentService>,
    qbit_startup_problem: Option<&str>,
) -> DoctorResponse {
    let mut report = super::encode::doctor(system.doctor());

    let Some(torrents) = torrents else {
        report.checks.push(DoctorCheck {
            name: "qbit".into(),
            ok: false,
            message: qbit_startup_problem
                .unwrap_or("qBittorrent is unavailable")
                .into(),
        });
        return report;
    };

    match torrents.probe().await {
        Ok(probe) => report.checks.push(DoctorCheck {
            name: "qbit.api".into(),
            ok: probe.mutation_ready,
            message: format!(
                "application={} webapi={} mutation_ready={}",
                probe.application_version, probe.webapi_version, probe.mutation_ready
            ),
        }),
        Err(error) => {
            report.checks.push(DoctorCheck {
                name: "qbit.api".into(),
                ok: false,
                message: format!("{}: {}", error.code, error.message),
            });
            return report;
        }
    }

    match torrents.transfer_info().await {
        Ok(info) => report.checks.push(DoctorCheck {
            name: "qbit.transfer".into(),
            ok: true,
            message: format!(
                "connection={} dht_nodes={} download_rate_bps={} upload_rate_bps={} download_limit_bps={} upload_limit_bps={}",
                connection_status_name(info.connection_status),
                info.dht_nodes,
                info.download_rate_bps,
                info.upload_rate_bps,
                info.download_limit_bps,
                info.upload_limit_bps
            ),
        }),
        Err(error) => report.checks.push(DoctorCheck {
            name: "qbit.transfer".into(),
            ok: false,
            message: format!("{}: {}", error.code, error.message),
        }),
    }

    match torrents.network_preferences().await {
        Ok(network) => report.checks.push(DoctorCheck {
            name: "qbit.network".into(),
            ok: true,
            message: format!(
                "listen_port={} upnp={} dht={} pex={} lsd={} interface={} address={} max_connections={} max_connections_per_torrent={}",
                network.listen_port,
                network.upnp,
                network.dht,
                network.pex,
                network.lsd,
                compact_evidence(&network.current_network_interface),
                compact_evidence(&network.current_interface_address),
                network.max_connections,
                network.max_connections_per_torrent
            ),
        }),
        Err(error) => report.checks.push(DoctorCheck {
            name: "qbit.network".into(),
            ok: false,
            message: format!("{}: {}", error.code, error.message),
        }),
    }

    match torrents.queue_settings().await {
        Ok(queue) => report.checks.push(DoctorCheck {
            name: "qbit.queue".into(),
            ok: true,
            message: format!(
                "enabled={} max_active_downloads={} max_active_torrents={} dont_count_slow_torrents={}",
                queue.queueing_enabled,
                queue.max_active_downloads,
                queue.max_active_torrents,
                queue.dont_count_slow_torrents
            ),
        }),
        Err(error) => report.checks.push(DoctorCheck {
            name: "qbit.queue".into(),
            ok: false,
            message: format!("{}: {}", error.code, error.message),
        }),
    }

    report
}

fn connection_status_name(value: qb_application::torrent::ConnectionStatus) -> &'static str {
    use qb_application::torrent::ConnectionStatus;
    match value {
        ConnectionStatus::Connected => "connected",
        ConnectionStatus::Firewalled => "firewalled",
        ConnectionStatus::Disconnected => "disconnected",
        ConnectionStatus::Unknown => "unknown",
    }
}

fn compact_evidence(value: &str) -> String {
    let value: String = value
        .chars()
        .filter(|character| !character.is_control())
        .take(128)
        .collect();
    if value.is_empty() {
        "<unspecified>".into()
    } else {
        value
    }
}

async fn dispatch_mutation(
    sequence: u64,
    request_id: Option<String>,
    command: MutationCommand,
    mutations: Option<&MutationService>,
    mutation_admission_enabled: bool,
) -> Response {
    if !mutation_admission_enabled && !matches!(command, MutationCommand::SetQueueTarget { .. }) {
        return mutation_blocked(
            sequence,
            request_id,
            "MUTATION_ADMISSION_DISABLED",
            "daemon is not ready to admit qBittorrent mutations",
        );
    }

    let Some(service) = mutations else {
        return internal_unavailable(sequence, request_id, "mutation service unavailable");
    };
    let request_id = match require_request_id(request_id.as_deref()) {
        Ok(value) => value,
        Err(message) => return invalid_request(sequence, request_id, &message),
    };

    match service.execute(&request_id, command).await {
        Ok(MutationExecutionResult::Execution(execution)) => {
            mutation_execution_response(sequence, Some(request_id.into_inner()), *execution)
        }
        Ok(MutationExecutionResult::Conflict { operation_id }) => request_conflict(
            sequence,
            Some(request_id.into_inner()),
            operation_id.to_string(),
        ),
        Err(error) => port_error(sequence, Some(request_id.into_inner()), error),
    }
}

fn completion_execution_response(
    sequence: u64,
    request_id: Option<String>,
    execution: CompletionExecution,
) -> Response {
    let operation_id = Some(execution.record.operation_id.to_string());
    let status = execution.status;
    let payload = Some(response::Payload::OperationRecover(
        OperationRecoverResponse {
            operation: Some(super::encode::operation_view(execution.record)),
            execution_status: super::encode::completion_execution_status_name(status).into(),
            replayed: execution.replayed,
        },
    ));

    match status {
        CompletionExecutionStatus::Finished => Response {
            sequence,
            status: Status::Ok as i32,
            request_id,
            operation_id,
            job_id: None,
            problems: Vec::new(),
            next_actions: Vec::new(),
            payload,
        },
        CompletionExecutionStatus::Blocked => mutation_problem(
            sequence,
            request_id,
            operation_id,
            execution.problem.or_else(|| {
                Some(PortError::new(
                    "COMPLETION_BLOCKED",
                    "completion recovery is blocked by durable evidence",
                ))
            }),
            MutationProblemSpec {
                status: Status::Blocked,
                certainty: MutationCertainty::MayHaveApplied,
                retry: RetryGuidance::RetryAfterStateChange,
            },
            payload,
        ),
        CompletionExecutionStatus::Failed => mutation_problem(
            sequence,
            request_id,
            operation_id,
            execution.problem.or_else(|| {
                Some(PortError::new(
                    "COMPLETION_FAILED",
                    "completion recovery failed",
                ))
            }),
            MutationProblemSpec {
                status: Status::Error,
                certainty: MutationCertainty::MayHaveApplied,
                retry: RetryGuidance::ObserveOrRecover,
            },
            payload,
        ),
        CompletionExecutionStatus::Stopped
        | CompletionExecutionStatus::ArchivePending
        | CompletionExecutionStatus::PayloadPending
        | CompletionExecutionStatus::RemoveRecordPending
        | CompletionExecutionStatus::UnknownStop
        | CompletionExecutionStatus::UnknownArchive
        | CompletionExecutionStatus::UnknownArchiveSourceDelete
        | CompletionExecutionStatus::UnknownMove
        | CompletionExecutionStatus::UnknownSourceDelete
        | CompletionExecutionStatus::UnknownRemoveRecord => mutation_problem(
            sequence,
            request_id,
            operation_id.clone(),
            execution.problem.or_else(|| {
                Some(PortError::new(
                    "COMPLETION_RECOVERY_PENDING",
                    "completion recovery remains pending at a durable effect boundary",
                ))
            }),
            MutationProblemSpec {
                status: Status::Unknown,
                certainty: MutationCertainty::MayHaveApplied,
                retry: RetryGuidance::ObserveOrRecover,
            },
            payload,
        )
        .with_next_action("recover_operation", operation_id),
    }
}

fn require_request_id(value: Option<&str>) -> Result<RequestId, String> {
    let value = value.ok_or_else(|| "request_id is required for mutations".to_string())?;
    RequestId::new(value.to_string()).map_err(|error| error.to_string())
}

fn mutation_execution_response(
    sequence: u64,
    request_id: Option<String>,
    execution: MutationExecution,
) -> Response {
    let operation_id = Some(execution.record.operation_id.to_string());
    let payload = Some(response::Payload::MutationResult(MutationResultResponse {
        replayed: execution.replayed,
        checkpoint: execution.record.checkpoint.clone(),
    }));

    match execution.status {
        MutationExecutionStatus::Finished => Response {
            sequence,
            status: Status::Ok as i32,
            request_id,
            operation_id,
            job_id: None,
            problems: Vec::new(),
            next_actions: Vec::new(),
            payload,
        },
        MutationExecutionStatus::Blocked => mutation_problem(
            sequence,
            request_id,
            operation_id,
            execution.problem,
            MutationProblemSpec {
                status: Status::Blocked,
                certainty: MutationCertainty::ConfirmedNotApplied,
                retry: RetryGuidance::RetryAfterStateChange,
            },
            payload,
        ),
        MutationExecutionStatus::Unknown => mutation_problem(
            sequence,
            request_id,
            operation_id.clone(),
            execution.problem,
            MutationProblemSpec {
                status: Status::Unknown,
                certainty: MutationCertainty::MayHaveApplied,
                retry: RetryGuidance::ObserveOrRecover,
            },
            payload,
        )
        .with_next_action("recover_operation", operation_id),
        MutationExecutionStatus::Failed => mutation_problem(
            sequence,
            request_id,
            operation_id,
            execution.problem,
            MutationProblemSpec {
                status: Status::Error,
                certainty: MutationCertainty::ConfirmedNotApplied,
                retry: RetryGuidance::NewRequestRequired,
            },
            payload,
        ),
    }
}

trait ResponseNextAction {
    fn with_next_action(self, kind: &str, target_id: Option<String>) -> Self;
}

impl ResponseNextAction for Response {
    fn with_next_action(mut self, kind: &str, target_id: Option<String>) -> Self {
        self.next_actions.push(NextAction {
            kind: kind.into(),
            target_id,
        });
        self
    }
}

struct MutationProblemSpec {
    status: Status,
    certainty: MutationCertainty,
    retry: RetryGuidance,
}

fn mutation_problem(
    sequence: u64,
    request_id: Option<String>,
    operation_id: Option<String>,
    problem: Option<PortError>,
    spec: MutationProblemSpec,
    payload: Option<response::Payload>,
) -> Response {
    let problem = problem.unwrap_or_else(|| PortError::new("MUTATION_FAILED", "mutation failed"));
    Response {
        sequence,
        status: spec.status as i32,
        request_id,
        operation_id,
        job_id: None,
        problems: vec![Problem {
            code: problem.code.into(),
            category: problem_category(problem.code) as i32,
            retry_guidance: spec.retry as i32,
            mutation_certainty: spec.certainty as i32,
            message_key: problem.message,
            details: Vec::new(),
        }],
        next_actions: Vec::new(),
        payload,
    }
}

fn request_conflict(sequence: u64, request_id: Option<String>, operation_id: String) -> Response {
    Response {
        sequence,
        status: Status::Error as i32,
        request_id,
        operation_id: Some(operation_id),
        job_id: None,
        problems: vec![Problem {
            code: "REQUEST_ID_CONFLICT".into(),
            category: ProblemCategory::State as i32,
            retry_guidance: RetryGuidance::NewRequestRequired as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: "request_id is already bound to a different semantic command".into(),
            details: Vec::new(),
        }],
        next_actions: Vec::new(),
        payload: None,
    }
}

fn mutation_blocked(
    sequence: u64,
    request_id: Option<String>,
    code: &str,
    message: &str,
) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Blocked,
        Problem {
            code: code.into(),
            category: ProblemCategory::State as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::ConfirmedNotApplied as i32,
            message_key: message.into(),
            details: Vec::new(),
        },
    )
}

fn success(sequence: u64, request_id: Option<String>, payload: response::Payload) -> Response {
    Response {
        sequence,
        status: Status::Ok as i32,
        request_id,
        operation_id: None,
        job_id: None,
        problems: Vec::new(),
        next_actions: Vec::new(),
        payload: Some(payload),
    }
}

fn invalid_request(sequence: u64, request_id: Option<String>, message: &str) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: "INVALID_ARGUMENT".into(),
            category: ProblemCategory::Usage as i32,
            retry_guidance: RetryGuidance::DoNotRetry as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: message.into(),
            details: Vec::new(),
        },
    )
}

fn state_problem(sequence: u64, request_id: Option<String>, code: &str, message: &str) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: code.into(),
            category: ProblemCategory::State as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: message.into(),
            details: Vec::new(),
        },
    )
}

fn qbit_unavailable(sequence: u64, request_id: Option<String>, problem: Option<&str>) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: "QBIT_UNAVAILABLE".into(),
            category: ProblemCategory::Availability as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: problem
                .unwrap_or("qBittorrent service is unavailable")
                .into(),
            details: Vec::new(),
        },
    )
}

fn internal_unavailable(sequence: u64, request_id: Option<String>, message: &str) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: "INTERNAL_UNAVAILABLE".into(),
            category: ProblemCategory::Internal as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: message.into(),
            details: Vec::new(),
        },
    )
}

fn port_error(sequence: u64, request_id: Option<String>, error: PortError) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: error.code.into(),
            category: problem_category(error.code) as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: error.message,
            details: Vec::new(),
        },
    )
}

fn problem_category(code: &str) -> ProblemCategory {
    match code {
        "QBIT_UNAVAILABLE" | "QBIT_AUTH_FAILED" | "JOURNAL_UNAVAILABLE" => {
            ProblemCategory::Availability
        }
        "INVALID_ARGUMENT" => ProblemCategory::Usage,
        "QBIT_API_UNSUPPORTED"
        | "QBIT_QUEUEING_DISABLED"
        | "TORRENT_NOT_FOUND"
        | "TORRENT_STATE_UNKNOWN"
        | "REQUEST_ID_CONFLICT"
        | "QBIT_MUTATION_REJECTED"
        | "QBIT_POSTCONDITION_UNCONFIRMED" => ProblemCategory::State,
        _ => ProblemCategory::Internal,
    }
}

fn problem_response(
    sequence: u64,
    request_id: Option<String>,
    status: Status,
    problem: Problem,
) -> Response {
    Response {
        sequence,
        status: status as i32,
        request_id,
        operation_id: None,
        job_id: None,
        problems: vec![problem],
        next_actions: Vec::new(),
        payload: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qb_application::{
        system::{DaemonPhase, RuntimeHealthPort, RuntimeSnapshot, SystemService},
        JournalHealthPort, PortError,
    };
    use qb_proto::v1::{request, response, PauseTorrentRequest, Request, Status, StatusRequest};

    use super::{dispatch, OperationServices};

    struct FakeJournal;

    impl JournalHealthPort for FakeJournal {
        fn schema_version(&self) -> Result<u32, PortError> {
            Ok(3)
        }

        fn quick_check(&self) -> Result<(), PortError> {
            Ok(())
        }

        fn recovery_blockers(
            &self,
        ) -> Result<Vec<qb_application::RecoveryBlocker>, PortError> {
            Ok(Vec::new())
        }
    }

    struct FakeRuntime;

    impl RuntimeHealthPort for FakeRuntime {
        fn snapshot(&self) -> RuntimeSnapshot {
            RuntimeSnapshot {
                phase: DaemonPhase::Ready,
                instance_id: "protocol-test".into(),
                mutation_admission_enabled: false,
            }
        }

        fn runtime_root_check(&self) -> Result<String, PortError> {
            Ok("test-runtime".into())
        }
    }

    fn system() -> SystemService {
        SystemService::new(Arc::new(FakeJournal), Arc::new(FakeRuntime))
    }

    #[tokio::test]
    async fn status_mapping_does_not_require_ipc() {
        let response = dispatch(
            Request {
                sequence: 42,
                request_id: None,
                command: Some(request::Command::Status(StatusRequest {})),
            },
            &system(),
            None,
            None,
            OperationServices::default(),
            false,
            None,
        )
        .await;

        assert_eq!(response.sequence, 42);
        assert_eq!(response.status, Status::Ok as i32);

        match response.payload {
            Some(response::Payload::SystemStatus(status)) => {
                assert_eq!(status.instance_id, "protocol-test");
                assert_eq!(status.schema_version, 3);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn qbit_mutation_is_blocked_when_runtime_is_not_ready() {
        let response = dispatch(
            Request {
                sequence: 8,
                request_id: Some("blocked-while-recovering".into()),
                command: Some(request::Command::TorrentPause(PauseTorrentRequest {
                    torrent_id: "abcdef0123456789abcdef0123456789abcdef01".into(),
                })),
            },
            &system(),
            None,
            None,
            OperationServices::default(),
            false,
            None,
        )
        .await;

        assert_eq!(response.status, Status::Blocked as i32);
        assert_eq!(response.problems.len(), 1);
        assert_eq!(response.problems[0].code, "MUTATION_ADMISSION_DISABLED");
        assert!(response.operation_id.is_none());
        assert!(response.payload.is_none());
    }

    #[tokio::test]
    async fn missing_command_is_rejected_before_application_dispatch() {
        let response = dispatch(
            Request {
                sequence: 7,
                request_id: None,
                command: None,
            },
            &system(),
            None,
            None,
            OperationServices::default(),
            false,
            None,
        )
        .await;

        assert_eq!(response.status, Status::Error as i32);
        assert_eq!(response.problems[0].code, "INVALID_ARGUMENT");
        assert!(response.payload.is_none());
    }
}
