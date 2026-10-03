use std::{
    io::{self, Write},
    process::ExitCode,
};

use clap::{Parser, Subcommand, ValueEnum};
use prost::Message;
use qb_ipc::{ClientConnection, IpcError, DEFAULT_PIPE};
use qb_proto::{
    v1::{
        request, response, CapabilitiesRequest, ClientHello, DaemonState, DoctorRequest,
        QbitProbeRequest, QueueGetRequest, Request, Response, ServerHello, Status, StatusRequest,
        TorrentGetRequest, TorrentListRequest, TorrentStateView, TransferLimitsGetRequest,
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
    Qbit {
        #[command(subcommand)]
        command: QbitCommand,
    },
    Torrent {
        #[command(subcommand)]
        command: TorrentCommand,
    },
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    Transfer {
        #[command(subcommand)]
        command: TransferCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Status,
}

#[derive(Debug, Subcommand)]
enum QbitCommand {
    Status,
}

#[derive(Debug, Subcommand)]
enum TorrentCommand {
    List,
    Get { torrent_id: String },
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    Get,
}

#[derive(Debug, Subcommand)]
enum TransferCommand {
    Limits {
        #[command(subcommand)]
        command: TransferLimitsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum TransferLimitsCommand {
    Get,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("{0}")]
    Ipc(#[from] IpcError),
    #[error("protocol major mismatch: client={client}, daemon={daemon}")]
    ProtocolMismatch { client: u32, daemon: u32 },
    #[error("invalid protocol payload: {0}")]
    Decode(#[from] prost::DecodeError),
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
        Err(CliError::Decode(error)) => {
            eprintln!("invalid protocol payload: {error}");
            ExitCode::from(7)
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

    let client_hello = ClientHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: vec!["status.v1".into(), "torrent.read.v1".into()],
    };
    connection.send_frame(client_hello.encode_to_vec()).await?;

    let hello = ServerHello::decode(connection.recv_frame().await?)?;
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(CliError::ProtocolMismatch {
            client: PROTOCOL_MAJOR,
            daemon: hello.protocol_major,
        });
    }

    let command = match cli.command {
        Command::Capabilities => request::Command::Capabilities(CapabilitiesRequest {}),
        Command::Status
        | Command::Daemon {
            command: DaemonCommand::Status,
        } => request::Command::Status(StatusRequest {}),
        Command::Doctor => request::Command::Doctor(DoctorRequest {}),
        Command::Qbit {
            command: QbitCommand::Status,
        } => request::Command::QbitProbe(QbitProbeRequest {}),
        Command::Torrent {
            command: TorrentCommand::List,
        } => request::Command::TorrentList(TorrentListRequest {}),
        Command::Torrent {
            command: TorrentCommand::Get { torrent_id },
        } => request::Command::TorrentGet(TorrentGetRequest { torrent_id }),
        Command::Queue {
            command: QueueCommand::Get,
        } => request::Command::QueueGet(QueueGetRequest {}),
        Command::Transfer {
            command:
                TransferCommand::Limits {
                    command: TransferLimitsCommand::Get,
                },
        } => request::Command::TransferLimitsGet(TransferLimitsGetRequest {}),
    };

    let request = Request {
        sequence: 1,
        request_id: None,
        command: Some(command),
    };
    connection.send_frame(request.encode_to_vec()).await?;

    Ok(Response::decode(connection.recv_frame().await?)?)
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
            let state =
                DaemonState::try_from(value.daemon_state).unwrap_or(DaemonState::Unspecified);
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
        Some(response::Payload::QbitProbe(value)) => {
            println!(
                "qBittorrent {} · WebAPI {} · mutations {}",
                value.application_version,
                value.webapi_version,
                if value.mutation_ready {
                    "supported"
                } else {
                    "disabled"
                }
            );
        }
        Some(response::Payload::TorrentList(value)) => {
            println!("{} torrent(s)", value.torrents.len());
            for torrent in &value.torrents {
                println!(
                    "{} {} {:>6.2}% {}",
                    torrent.id,
                    torrent_state_name(torrent.state),
                    f64::from(torrent.progress_ppm) / 10_000.0,
                    torrent.name
                );
            }
        }
        Some(response::Payload::TorrentGet(value)) => {
            if let Some(torrent) = value.torrent.as_ref() {
                println!("{} {}", torrent.id, torrent.name);
                println!("  state: {}", torrent_state_name(torrent.state));
                println!(
                    "  progress: {:.2}% · remaining: {} bytes",
                    f64::from(torrent.progress_ppm) / 10_000.0,
                    torrent.remaining_bytes
                );
                println!(
                    "  rates: down {} B/s · up {} B/s",
                    torrent.download_rate_bps, torrent.upload_rate_bps
                );
                println!(
                    "  peers: {}/{} · seeds: {}/{}",
                    torrent.peers_connected,
                    torrent.peers_known,
                    torrent.seeds_connected,
                    torrent.seeds_known
                );
            }
        }
        Some(response::Payload::QueueSettings(value)) => {
            println!(
                "queueing {} · active downloads {} · active torrents {} · slow torrents {}",
                if value.queueing_enabled {
                    "enabled"
                } else {
                    "disabled"
                },
                value.max_active_downloads,
                value.max_active_torrents,
                if value.dont_count_slow_torrents {
                    "not counted"
                } else {
                    "counted"
                }
            );
        }
        Some(response::Payload::TransferLimits(value)) => {
            println!(
                "download limit {} B/s · upload limit {} B/s · current {} / {} B/s",
                value.download_limit_bps,
                value.upload_limit_bps,
                value.observed_download_rate_bps,
                value.observed_upload_rate_bps
            );
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
        Some(response::Payload::QbitProbe(value)) => {
            println!("application_version={}", value.application_version);
            println!("webapi_version={}", value.webapi_version);
            println!("mutation_ready={}", value.mutation_ready);
        }
        Some(response::Payload::TorrentList(value)) => {
            println!("torrent_count={}", value.torrents.len());
            for (index, torrent) in value.torrents.iter().enumerate() {
                print_torrent_fields(index, torrent);
            }
        }
        Some(response::Payload::TorrentGet(value)) => {
            println!("torrent_present={}", value.torrent.is_some());
            if let Some(torrent) = value.torrent.as_ref() {
                print_torrent_fields(0, torrent);
            }
        }
        Some(response::Payload::QueueSettings(value)) => {
            println!("queueing_enabled={}", value.queueing_enabled);
            println!("max_active_downloads={}", value.max_active_downloads);
            println!("max_active_torrents={}", value.max_active_torrents);
            println!(
                "dont_count_slow_torrents={}",
                value.dont_count_slow_torrents
            );
        }
        Some(response::Payload::TransferLimits(value)) => {
            println!("download_limit_bps={}", value.download_limit_bps);
            println!("upload_limit_bps={}", value.upload_limit_bps);
            println!(
                "observed_download_rate_bps={}",
                value.observed_download_rate_bps
            );
            println!(
                "observed_upload_rate_bps={}",
                value.observed_upload_rate_bps
            );
        }
        None => {}
    }

    Ok(())
}

fn print_torrent_fields(index: usize, torrent: &qb_proto::v1::TorrentSummary) {
    println!("torrent.{index}.id={}", torrent.id);
    println!("torrent.{index}.name={}", sanitize_field(&torrent.name));
    println!("torrent.{index}.state={}", torrent_state_name(torrent.state));
    println!("torrent.{index}.total_bytes={}", torrent.total_bytes);
    println!(
        "torrent.{index}.remaining_bytes={}",
        torrent.remaining_bytes
    );
    println!(
        "torrent.{index}.download_rate_bps={}",
        torrent.download_rate_bps
    );
    println!(
        "torrent.{index}.upload_rate_bps={}",
        torrent.upload_rate_bps
    );
    println!("torrent.{index}.progress_ppm={}", torrent.progress_ppm);
    println!(
        "torrent.{index}.peers_connected={}",
        torrent.peers_connected
    );
    println!("torrent.{index}.peers_known={}", torrent.peers_known);
    println!(
        "torrent.{index}.seeds_connected={}",
        torrent.seeds_connected
    );
    println!("torrent.{index}.seeds_known={}", torrent.seeds_known);
}

fn sanitize_field(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, '\r' | '\n' | '='))
        .collect()
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

fn torrent_state_name(value: i32) -> &'static str {
    TorrentStateView::try_from(value)
        .unwrap_or(TorrentStateView::Unknown)
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
