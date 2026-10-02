use std::{
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context as _, Result};
use qb_ipc::{IpcError, ServerConnection, ServerListener};
use qb_journal::Journal;
use qb_proto::{
    v1::{
        request, response, CapabilitiesResponse, ClientHello, DaemonState, DoctorCheck,
        DoctorResponse, MutationCertainty, Problem, ProblemCategory, Request, Response,
        RetryGuidance, ServerHello, Status, StatusResponse,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use qb_win::{InstanceGuard, RuntimeMode, RuntimeRoot};
use tokio::{sync::RwLock, task::JoinSet};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;

#[derive(Clone, Copy, Debug)]
enum RuntimeState {
    Ready,
    Draining,
}

impl RuntimeState {
    fn wire(self) -> DaemonState {
        match self {
            Self::Ready => DaemonState::Ready,
            Self::Draining => DaemonState::Draining,
        }
    }
}

struct Context {
    instance_id: String,
    journal: Arc<Journal>,
    runtime_root: PathBuf,
    state: RwLock<RuntimeState>,
}

pub async fn run(runtime_override: Option<PathBuf>) -> Result<()> {
    let runtime_root = match runtime_override {
        Some(path) => RuntimeRoot::at(path),
        None => RuntimeRoot::discover(RuntimeMode::User).context("discover runtime root")?,
    };
    runtime_root.ensure().context("create runtime root")?;

    let _instance_guard = InstanceGuard::acquire(&runtime_root).context("acquire daemon ownership")?;
    let config = Config::load(runtime_root.path()).context("load config")?;
    let journal = Arc::new(
        Journal::open(runtime_root.path().join("state.sqlite")).context("open journal")?,
    );

    let context = Arc::new(Context {
        instance_id: Uuid::new_v4().to_string(),
        journal,
        runtime_root: runtime_root.path().to_path_buf(),
        state: RwLock::new(RuntimeState::Ready),
    });

    let mut listener = ServerListener::bind(config.pipe.clone()).context("bind named pipe")?;
    let mut tasks = JoinSet::new();

    info!(
        pipe = %config.pipe,
        runtime_root = %context.runtime_root.display(),
        instance_id = %context.instance_id,
        "qbctld ready"
    );

    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal.context("listen for Ctrl+C")?;
                *context.state.write().await = RuntimeState::Draining;
                info!("shutdown requested");
                break;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok(connection) => {
                        let context = Arc::clone(&context);
                        tasks.spawn(async move {
                            if let Err(error) = handle_connection(connection, context).await {
                                warn!(error = %error, "client connection ended with error");
                            }
                        });
                    }
                    Err(error) => return Err(error).context("accept named pipe client"),
                }
            }
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined {
                    error!(error = %error, "client task panicked");
                }
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            error!(error = %error, "client task panicked during shutdown");
        }
    }

    Ok(())
}

async fn handle_connection(
    mut connection: ServerConnection,
    context: Arc<Context>,
) -> Result<(), IpcError> {
    let hello: ClientHello = connection.recv().await?;

    let server_hello = ServerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: capabilities(),
        instance_id: context.instance_id.clone(),
    };
    connection.send(&server_hello).await?;

    if hello.protocol_major != PROTOCOL_MAJOR {
        return Ok(());
    }

    loop {
        let request: Request = match connection.recv().await {
            Ok(request) => request,
            Err(IpcError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = dispatch(request, &context).await;
        connection.send(&response).await?;
    }
}

async fn dispatch(request: Request, context: &Context) -> Response {
    let sequence = request.sequence;
    let request_id = request.request_id.clone();

    let payload = match request.command {
        Some(request::Command::Capabilities(_)) => {
            Some(response::Payload::Capabilities(CapabilitiesResponse {
                protocol_major: PROTOCOL_MAJOR,
                protocol_minor: PROTOCOL_MINOR,
                daemon_version: env!("CARGO_PKG_VERSION").to_string(),
                capabilities: capabilities(),
            }))
        }
        Some(request::Command::Status(_)) => {
            let state = *context.state.read().await;
            let schema_version = context.journal.schema_version().unwrap_or_default();
            Some(response::Payload::SystemStatus(StatusResponse {
                daemon_state: state.wire() as i32,
                instance_id: context.instance_id.clone(),
                schema_version,
                mutation_admission_enabled: false,
            }))
        }
        Some(request::Command::Doctor(_)) => {
            let journal = match context.journal.quick_check() {
                Ok(()) => DoctorCheck {
                    name: "journal".into(),
                    ok: true,
                    message: "SQLite quick_check: ok".into(),
                },
                Err(error) => DoctorCheck {
                    name: "journal".into(),
                    ok: false,
                    message: error.to_string(),
                },
            };
            let runtime = DoctorCheck {
                name: "runtime_root".into(),
                ok: context.runtime_root.is_dir(),
                message: context.runtime_root.display().to_string(),
            };
            Some(response::Payload::Doctor(DoctorResponse {
                checks: vec![journal, runtime],
            }))
        }
        None => {
            return invalid_request(sequence, request_id, "command is required");
        }
    };

    Response {
        sequence,
        status: Status::Ok as i32,
        request_id,
        operation_id: None,
        job_id: None,
        problems: Vec::new(),
        next_actions: Vec::new(),
        payload,
    }
}

fn invalid_request(sequence: u64, request_id: Option<String>, message: &str) -> Response {
    Response {
        sequence,
        status: Status::Error as i32,
        request_id,
        operation_id: None,
        job_id: None,
        problems: vec![Problem {
            code: "INVALID_ARGUMENT".into(),
            category: ProblemCategory::Usage as i32,
            retry_guidance: RetryGuidance::DoNotRetry as i32,
            mutation_certainty: MutationCertainty::NoMutation as i32,
            message_key: message.into(),
            details: Vec::new(),
        }],
        next_actions: Vec::new(),
        payload: None,
    }
}

fn capabilities() -> Vec<String> {
    vec![
        "ipc.protobuf.v1".into(),
        "journal.sqlite.v1".into(),
        "status.v1".into(),
        "doctor.v1".into(),
    ]
}
