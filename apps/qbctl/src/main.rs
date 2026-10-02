use std::{
    io::{self, Write},
    process::ExitCode,
};

use clap::{Parser, Subcommand, ValueEnum};
use prost::Message;
use qb_ipc::{ClientConnection, IpcError, DEFAULT_PIPE};
use qb_proto::{
    v1::{
        request, response, CapabilitiesRequest, ClientHello, DaemonState, DoctorRequest, Request,
        Response, ServerHello, Status, StatusRequest,
    },
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputMode {
    Human,
    Proto,
    Fields,
}

#[derive(Debug, Parser)]
#[command(name = "qbctl", version, about = "Agent-friendly qbctld client")]
struct Cli {
    #[arg(long, value_enum, default_value_t = OutputMode::Human, global = true)]
    output: OutputMode,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Capabilities,
    Status,
    Doctor,
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Status,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("{0}")]
    Ipc(#[from] IpcError),
    #[error("protocol major mismatch: client={client}, daemon={daemon}")]
    ProtocolMismatch { client: u32, daemon: u32 },
    #[error("output error: {0}")]
    Output(#[from] io::Error),
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let output = cli.output;

    match execute(cli).await {
        Ok(response) => match render(&response, output) {
            Ok(()) => exit_for_response(&response),
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(8)
            }
        },
        Err(CliError::ProtocolMismatch { client, daemon }) => {
            eprintln!("protocol major mismatch: client={client}, daemon={daemon}");
            ExitCode::from(7)
        }
        Err(CliError::Ipc(error)) => {
            eprintln!("{error}");
            ExitCode::from(6)
        }
        Err(CliError::Output(error)) => {
            eprintln!("{error}");
            ExitCode::from(8)
        }
    }
}

async fn execute(cli: Cli) -> Result<Response, CliError> {
    let pipe = std::env::var("QBCTL_PIPE").unwrap_or_else(|_| DEFAULT_PIPE.to_string());
    let mut connection = ClientConnection::connect(&pipe).await?;

    connection
        .send(&ClientHello {
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: vec!["status.v1".into()],
        })
        .await?;

    let hello: ServerHello = connection.recv().await?;
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(CliError::ProtocolMismatch {
            client: PROTOCOL_MAJOR,
            daemon: hello.protocol_major,
        });
    }

    let command = match cli.command {
        Command::Capabilities => request::Command::Capabilities(CapabilitiesRequest {}),
        Command::Status | Command::Daemon {
            command: DaemonCommand::Status,
        } => request::Command::Status(StatusRequest {}),
        Command::Doctor => request::Command::Doctor(DoctorRequest {}),
    };

    connection
        .send(&Request {
            sequence: 1,
            request_id: None,
            command: Some(command),
        })
        .await?;

    Ok(connection.recv().await?)
}

fn render(response: &Response, mode: OutputMode) -> Result<(), CliError> {
    match mode {
        OutputMode::Human => render_human(response),
        OutputMode::Fields => render_fields(response),
        OutputMode::Proto => {
            let mut stdout = io::stdout().lock();
            stdout.write_all(&response.encode_to_vec())?;
            stdout.flush()?;
            Ok(())
        }
    }
}

fn render_human(response: &Response) -> Result<(), CliError> {
    match response.payload.as_ref() {
        Some(response::Payload::Capabilities(value)) => {
            println!(
                "daemon {} · protocol {}.{}",
                value.daemon_version, value.protocol_major, value.protocol_minor
            );
            for capability in &value.capabilities {
                println!("  {capability}");
            }
        }
        Some(response::Payload::SystemStatus(value)) => {
            let state = DaemonState::try_from(value.daemon_state)
                .unwrap_or(DaemonState::Unspecified);
            println!(
                "{} · instance {} · schema {} · mutations {}",
                state.as_str_name(),
                value.instance_id,
                value.schema_version,
                if value.mutation_admission_enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            );
        }
        Some(response::Payload::Doctor(value)) => {
            for check in &value.checks {
                println!(
                    "{} {} — {}",
                    if check.ok { "OK" } else { "FAIL" },
                    check.name,
                    check.message
                );
            }
        }
        None => {}
    }

    for problem in &response.problems {
        eprintln!("{}: {}", problem.code, problem.message_key);
    }

    Ok(())
}

fn render_fields(response: &Response) -> Result<(), CliError> {
    println!("status={}", status_name(response.status));

    match response.payload.as_ref() {
        Some(response::Payload::Capabilities(value)) => {
            println!("protocol_major={}", value.protocol_major);
            println!("protocol_minor={}", value.protocol_minor);
            println!("daemon_version={}", value.daemon_version);
            println!("capability_count={}", value.capabilities.len());
        }
        Some(response::Payload::SystemStatus(value)) => {
            println!("daemon_state={}", daemon_state_name(value.daemon_state));
            println!("instance_id={}", value.instance_id);
            println!("schema_version={}", value.schema_version);
            println!(
                "mutation_admission_enabled={}",
                value.mutation_admission_enabled
            );
        }
        Some(response::Payload::Doctor(value)) => {
            println!("check_count={}", value.checks.len());
            println!("checks_ok={}", value.checks.iter().all(|check| check.ok));
        }
        None => {}
    }

    Ok(())
}

fn status_name(value: i32) -> &'static str {
    Status::try_from(value)
        .unwrap_or(Status::Error)
        .as_str_name()
}

fn daemon_state_name(value: i32) -> &'static str {
    DaemonState::try_from(value)
        .unwrap_or(DaemonState::Unspecified)
        .as_str_name()
}

fn exit_for_response(response: &Response) -> ExitCode {
    match Status::try_from(response.status).unwrap_or(Status::Error) {
        Status::Ok => ExitCode::SUCCESS,
        Status::Pending | Status::Partial => ExitCode::from(4),
        Status::Blocked => ExitCode::from(3),
        Status::Unknown => ExitCode::from(5),
        Status::Unspecified | Status::Error => ExitCode::from(8),
    }
}
