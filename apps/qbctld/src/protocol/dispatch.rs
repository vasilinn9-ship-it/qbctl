use qb_application::{
    mutation::{
        MutationCommand, MutationExecution, MutationExecutionResult, MutationExecutionStatus,
        MutationService, TorrentControlAction,
    },
    system::SystemService,
    torrent::TorrentService,
    PortError,
};
use qb_domain::{torrent::TorrentId, RequestId};
use qb_proto::{
    v1::{
        request, response, CapabilitiesResponse, MutationCertainty, MutationResultResponse,
        NextAction, Problem, ProblemCategory, QueueTargetResponse, Request, Response, RetryGuidance,
        Status, TorrentGetResponse,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

pub async fn dispatch(
    request: Request,
    system: &SystemService,
    torrents: Option<&TorrentService>,
    mutations: Option<&MutationService>,
    mutation_admission_enabled: bool,
    qbit_startup_problem: Option<&str>,
) -> Response {
    let sequence = request.sequence;
    let request_id = request.request_id.clone();

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
        Some(request::Command::Doctor(_)) => success(
            sequence,
            request_id,
            response::Payload::Doctor(super::encode::doctor(system.doctor())),
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
                        target_client_count,
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
        Ok(MutationExecutionResult::Conflict { operation_id }) => {
            request_conflict(sequence, Some(request_id.into_inner()), operation_id.to_string())
        }
        Err(error) => port_error(sequence, Some(request_id.into_inner()), error),
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
            Status::Blocked,
            execution.problem,
            MutationCertainty::ConfirmedNotApplied,
            RetryGuidance::RetryAfterStateChange,
            payload,
        ),
        MutationExecutionStatus::Unknown => mutation_problem(
            sequence,
            request_id,
            operation_id.clone(),
            Status::Unknown,
            execution.problem,
            MutationCertainty::MayHaveApplied,
            RetryGuidance::ObserveOrRecover,
            payload,
        )
        .with_next_action("recover_operation", operation_id),
        MutationExecutionStatus::Failed => mutation_problem(
            sequence,
            request_id,
            operation_id,
            Status::Error,
            execution.problem,
            MutationCertainty::ConfirmedNotApplied,
            RetryGuidance::NewRequestRequired,
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

fn mutation_problem(
    sequence: u64,
    request_id: Option<String>,
    operation_id: Option<String>,
    status: Status,
    problem: Option<PortError>,
    certainty: MutationCertainty,
    retry: RetryGuidance,
    payload: Option<response::Payload>,
) -> Response {
    let problem = problem.unwrap_or_else(|| PortError::new("MUTATION_FAILED", "mutation failed"));
    Response {
        sequence,
        status: status as i32,
        request_id,
        operation_id,
        job_id: None,
        problems: vec![Problem {
            code: problem.code.into(),
            category: problem_category(problem.code) as i32,
            retry_guidance: retry as i32,
            mutation_certainty: certainty as i32,
            message_key: problem.message,
            details: Vec::new(),
        }],
        next_actions: Vec::new(),
        payload,
    }
}

fn request_conflict(
    sequence: u64,
    request_id: Option<String>,
    operation_id: String,
) -> Response {
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
    use qb_proto::v1::{request, response, Request, Status, StatusRequest};

    use super::dispatch;

    struct FakeJournal;

    impl JournalHealthPort for FakeJournal {
        fn schema_version(&self) -> Result<u32, PortError> {
            Ok(3)
        }

        fn quick_check(&self) -> Result<(), PortError> {
            Ok(())
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
            false,
            None,
        )
        .await;

        assert_eq!(response.status, Status::Error as i32);
        assert_eq!(response.problems[0].code, "INVALID_ARGUMENT");
        assert!(response.payload.is_none());
    }
}
