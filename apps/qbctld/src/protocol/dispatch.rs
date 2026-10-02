use qb_application::{system::SystemService, PortError};
use qb_proto::{
    v1::{
        request, response, CapabilitiesResponse, MutationCertainty, Problem, ProblemCategory,
        Request, Response, RetryGuidance, Status,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

pub fn dispatch(request: Request, system: &SystemService) -> Response {
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
