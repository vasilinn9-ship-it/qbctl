use qb_application::{
    system::SystemService,
    torrent::TorrentService,
    PortError,
};
use qb_domain::torrent::TorrentId;
use qb_proto::{
    v1::{
        request, response, CapabilitiesResponse, MutationCertainty, Problem, ProblemCategory,
        Request, Response, RetryGuidance, Status, TorrentGetResponse,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

pub async fn dispatch(
    request: Request,
    system: &SystemService,
    torrents: Option<&TorrentService>,
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
        None => invalid_request(sequence, request_id, "command is required"),
    }
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

fn state_problem(
    sequence: u64,
    request_id: Option<String>,
    code: &str,
    message: &str,
) -> Response {
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

fn qbit_unavailable(
    sequence: u64,
    request_id: Option<String>,
    problem: Option<&str>,
) -> Response {
    problem_response(
        sequence,
        request_id,
        Status::Error,
        Problem {
            code: "QBIT_UNAVAILABLE".into(),
            category: ProblemCategory::Availability as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: problem.unwrap_or("qBittorrent service is unavailable").into(),
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
            category: ProblemCategory::Availability as i32,
            retry_guidance: RetryGuidance::RetryAfterStateChange as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: error.message,
            details: Vec::new(),
        },
    )
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
        )
        .await;

        assert_eq!(response.status, Status::Error as i32);
        assert_eq!(response.problems[0].code, "INVALID_ARGUMENT");
        assert!(response.payload.is_none());
    }
}
