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
        ManagedRootView, OperationGetRequest, OperationListRequest, OperationRecoverRequest,
        OperationView, PauseTorrentRequest, ProblemCategory, QbitProbeRequest, QueueGetRequest,
        QueueTargetGetRequest, Request, Response, ResumeTorrentRequest, ServerHello,
        SetActiveDownloadsRequest, SetDownloadLimitRequest, SetQueueTargetRequest,
        SetUploadLimitRequest, Status, StatusRequest, StorageListRequest, StorageStatusRequest,
        TorrentDiagnoseRequest, TorrentGetRequest, TorrentListRequest, TorrentStateView,
        TrackerStatusView, TransferLimitsGetRequest,
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
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    Operation {
        #[command(subcommand)]
        command: OperationCommand,
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
    Get {
        torrent_id: String,
    },
    Diagnose {
        torrent_id: String,
    },
    Pause {
        torrent_id: String,
        #[arg(long)]
        request_id: String,
    },
    Resume {
        torrent_id: String,
        #[arg(long)]
        request_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    Get,
    Target {
        #[command(subcommand)]
        command: QueueTargetCommand,
    },
    Downloads {
        #[command(subcommand)]
        command: QueueDownloadsCommand,
    },
}

#[derive(Debug, Subcommand)]
enum QueueTargetCommand {
    Get,
    Set {
        count: u32,
        #[arg(long)]
        request_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum QueueDownloadsCommand {
    Get,
    Set {
        count: u32,
        #[arg(long)]
        request_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum TransferCommand {
    Limits {
        #[command(subcommand)]
        command: TransferLimitsCommand,
    },
    DownloadLimit {
        #[command(subcommand)]
        command: TransferLimitCommand,
    },
    UploadLimit {
        #[command(subcommand)]
        command: TransferLimitCommand,
    },
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    List,
    Status,
}

#[derive(Debug, Subcommand)]
enum OperationCommand {
    List,
    Get { operation_id: String },
    Recover { operation_id: String },
}

#[derive(Debug, Subcommand)]
enum TransferLimitsCommand {
    Get,
}

#[derive(Debug, Subcommand)]
enum TransferLimitCommand {
    Get,
    Set {
        bytes_per_sec: u64,
        #[arg(long)]
        request_id: String,
    },
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
        capabilities: vec![
            "status.v1".into(),
            "torrent.read.v1".into(),
            "torrent.control.v1".into(),
            "request-idempotency.v1".into(),
            "storage.read.v1".into(),
            "operation.recovery.v1".into(),
        ],
    };
    connection.send_frame(client_hello.encode_to_vec()).await?;

    let hello = ServerHello::decode(connection.recv_frame().await?)?;
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(CliError::ProtocolMismatch {
            client: PROTOCOL_MAJOR,
            daemon: hello.protocol_major,
        });
    }

    let (command, request_id) = command_request(cli.command);

    let request = Request {
        sequence: 1,
        request_id,
        command: Some(command),
    };
    connection.send_frame(request.encode_to_vec()).await?;

    Ok(Response::decode(connection.recv_frame().await?)?)
}

fn command_request(command: Command) -> (request::Command, Option<String>) {
    match command {
        Command::Capabilities => (request::Command::Capabilities(CapabilitiesRequest {}), None),
        Command::Status
        | Command::Daemon {
            command: DaemonCommand::Status,
        } => (request::Command::Status(StatusRequest {}), None),
        Command::Doctor => (request::Command::Doctor(DoctorRequest {}), None),
        Command::Storage {
            command: StorageCommand::List,
        } => (request::Command::StorageList(StorageListRequest {}), None),
        Command::Storage {
            command: StorageCommand::Status,
        } => (
            request::Command::StorageStatus(StorageStatusRequest {}),
            None,
        ),
        Command::Operation {
            command: OperationCommand::List,
        } => (request::Command::OperationList(OperationListRequest {}), None),
        Command::Operation {
            command: OperationCommand::Get { operation_id },
        } => (
            request::Command::OperationGet(OperationGetRequest { operation_id }),
            None,
        ),
        Command::Operation {
            command: OperationCommand::Recover { operation_id },
        } => (
            request::Command::OperationRecover(OperationRecoverRequest { operation_id }),
            None,
        ),
        Command::Qbit {
            command: QbitCommand::Status,
        } => (request::Command::QbitProbe(QbitProbeRequest {}), None),
        Command::Torrent {
            command: TorrentCommand::List,
        } => (request::Command::TorrentList(TorrentListRequest {}), None),
        Command::Torrent {
            command: TorrentCommand::Get { torrent_id },
        } => (
            request::Command::TorrentGet(TorrentGetRequest { torrent_id }),
            None,
        ),
        Command::Torrent {
            command: TorrentCommand::Diagnose { torrent_id },
        } => (
            request::Command::TorrentDiagnose(TorrentDiagnoseRequest { torrent_id }),
            None,
        ),
        Command::Torrent {
            command:
                TorrentCommand::Pause {
                    torrent_id,
                    request_id,
                },
        } => (
            request::Command::TorrentPause(PauseTorrentRequest { torrent_id }),
            Some(request_id),
        ),
        Command::Torrent {
            command:
                TorrentCommand::Resume {
                    torrent_id,
                    request_id,
                },
        } => (
            request::Command::TorrentResume(ResumeTorrentRequest { torrent_id }),
            Some(request_id),
        ),
        Command::Queue {
            command: QueueCommand::Get,
        }
        | Command::Queue {
            command:
                QueueCommand::Downloads {
                    command: QueueDownloadsCommand::Get,
                },
        } => (request::Command::QueueGet(QueueGetRequest {}), None),
        Command::Queue {
            command:
                QueueCommand::Target {
                    command: QueueTargetCommand::Get,
                },
        } => (
            request::Command::QueueTargetGet(QueueTargetGetRequest {}),
            None,
        ),
        Command::Queue {
            command:
                QueueCommand::Target {
                    command: QueueTargetCommand::Set { count, request_id },
                },
        } => (
            request::Command::QueueTargetSet(SetQueueTargetRequest {
                target_client_count: count,
            }),
            Some(request_id),
        ),
        Command::Queue {
            command:
                QueueCommand::Downloads {
                    command: QueueDownloadsCommand::Set { count, request_id },
                },
        } => (
            request::Command::QueueDownloadsSet(SetActiveDownloadsRequest {
                max_active_downloads: count,
            }),
            Some(request_id),
        ),
        Command::Transfer {
            command:
                TransferCommand::Limits {
                    command: TransferLimitsCommand::Get,
                },
        }
        | Command::Transfer {
            command:
                TransferCommand::DownloadLimit {
                    command: TransferLimitCommand::Get,
                },
        }
        | Command::Transfer {
            command:
                TransferCommand::UploadLimit {
                    command: TransferLimitCommand::Get,
                },
        } => (
            request::Command::TransferLimitsGet(TransferLimitsGetRequest {}),
            None,
        ),
        Command::Transfer {
            command:
                TransferCommand::DownloadLimit {
                    command:
                        TransferLimitCommand::Set {
                            bytes_per_sec,
                            request_id,
                        },
                },
        } => (
            request::Command::TransferDownloadLimitSet(SetDownloadLimitRequest { bytes_per_sec }),
            Some(request_id),
        ),
        Command::Transfer {
            command:
                TransferCommand::UploadLimit {
                    command:
                        TransferLimitCommand::Set {
                            bytes_per_sec,
                            request_id,
                        },
                },
        } => (
            request::Command::TransferUploadLimitSet(SetUploadLimitRequest { bytes_per_sec }),
            Some(request_id),
        ),
    }
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
        Some(response::Payload::StorageList(value)) => {
            println!("{} managed storage root(s)", value.roots.len());
            for root in &value.roots {
                println!(
                    "{} · {} · free {} / {} bytes · volume {}",
                    managed_root_name(root.root),
                    root.path,
                    root.free_bytes,
                    root.total_bytes,
                    root.volume_id
                );
            }
        }
        Some(response::Payload::StorageStatus(value)) => {
            println!(
                "Incoming: {} eligible · {} already processed · {} redundant exact · {} rejected",
                value.eligible_count,
                value.already_processed_count,
                value.redundant_identical_count,
                value.rejected_count
            );
            for root in &value.roots {
                println!(
                    "  {}: {} · free {} / {} bytes · volume {}",
                    managed_root_name(root.root),
                    root.path,
                    root.free_bytes,
                    root.total_bytes,
                    root.volume_id
                );
            }
            for entry in &value.incoming {
                let mut suffix = String::new();
                if let Some(registry_id) = entry.registry_id.as_deref() {
                    suffix.push_str(&format!(" · registry {registry_id}"));
                }
                if let Some(problem_code) = entry.problem_code.as_deref() {
                    suffix.push_str(&format!(" · {problem_code}"));
                }
                if let Some(detail) = entry.detail.as_deref() {
                    suffix.push_str(&format!(" · {detail}"));
                }
                println!(
                    "  {} · {}{}",
                    entry.classification, entry.relative_path, suffix
                );
            }
        }
        Some(response::Payload::OperationList(value)) => {
            println!("{} completion operation(s)", value.operations.len());
            for operation in &value.operations {
                println!(
                    "{} · {} · registry {} · files {}/{} · archive receipt {}",
                    operation.operation_id,
                    operation.state,
                    operation.registry_id,
                    operation.files_moved_and_receipted,
                    operation.files_total,
                    if operation.archive_receipted { "yes" } else { "no" }
                );
            }
        }
        Some(response::Payload::OperationGet(value)) => {
            if let Some(operation) = value.operation.as_ref() {
                print_operation_human(operation);
            }
        }
        Some(response::Payload::OperationRecover(value)) => {
            println!(
                "recovery {} · replayed {}",
                value.execution_status, value.replayed
            );
            if let Some(operation) = value.operation.as_ref() {
                print_operation_human(operation);
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
        Some(response::Payload::TorrentDiagnose(value)) => {
            if let Some(torrent) = value.torrent.as_ref() {
                println!("{} {}", torrent.id, torrent.name);
                println!(
                    "  state: {} · rate: {} B/s · peers: {}/{} · seeds: {}/{}",
                    torrent_state_name(torrent.state),
                    torrent.download_rate_bps,
                    torrent.peers_connected,
                    torrent.peers_known,
                    torrent.seeds_connected,
                    torrent.seeds_known
                );
            }
            println!("  trackers: {}", value.trackers.len());
            for tracker in &value.trackers {
                println!(
                    "    {} · {} · peers {} · seeds {} · leeches {} · {}",
                    tracker.identity,
                    tracker_status_name(tracker.status),
                    tracker.peers,
                    tracker.seeds,
                    tracker.leeches,
                    tracker.message
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
        Some(response::Payload::QueueTarget(value)) => match value.target_client_count {
            Some(target) => println!(
                "target clients {target} · policy revision {}",
                value.revision
            ),
            None => println!(
                "target clients not configured · policy revision {}",
                value.revision
            ),
        },
        Some(response::Payload::TransferLimits(value)) => {
            println!(
                "download limit {} B/s · upload limit {} B/s · current {} / {} B/s",
                value.download_limit_bps,
                value.upload_limit_bps,
                value.observed_download_rate_bps,
                value.observed_upload_rate_bps
            );
        }
        Some(response::Payload::MutationResult(value)) => {
            println!(
                "operation {} · checkpoint {} · replayed {}",
                response.operation_id.as_deref().unwrap_or("<missing>"),
                value.checkpoint,
                value.replayed
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
    if let Some(request_id) = response.request_id.as_deref() {
        println!("request_id={}", sanitize_field(request_id));
    }
    if let Some(operation_id) = response.operation_id.as_deref() {
        println!("operation_id={}", sanitize_field(operation_id));
    }

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
        Some(response::Payload::StorageList(value)) => {
            println!("storage_root_count={}", value.roots.len());
            for (index, root) in value.roots.iter().enumerate() {
                print_storage_root_fields(index, root);
            }
        }
        Some(response::Payload::StorageStatus(value)) => {
            println!("storage_root_count={}", value.roots.len());
            println!("incoming_eligible_count={}", value.eligible_count);
            println!(
                "incoming_already_processed_count={}",
                value.already_processed_count
            );
            println!(
                "incoming_redundant_identical_count={}",
                value.redundant_identical_count
            );
            println!("incoming_rejected_count={}", value.rejected_count);
            for (index, root) in value.roots.iter().enumerate() {
                print_storage_root_fields(index, root);
            }
            for (index, entry) in value.incoming.iter().enumerate() {
                println!(
                    "incoming.{index}.relative_path={}",
                    sanitize_field(&entry.relative_path)
                );
                println!(
                    "incoming.{index}.classification={}",
                    sanitize_field(&entry.classification)
                );
                println!(
                    "incoming.{index}.registry_id={}",
                    sanitize_field(entry.registry_id.as_deref().unwrap_or(""))
                );
                println!(
                    "incoming.{index}.problem_code={}",
                    sanitize_field(entry.problem_code.as_deref().unwrap_or(""))
                );
                println!(
                    "incoming.{index}.detail={}",
                    sanitize_field(entry.detail.as_deref().unwrap_or(""))
                );
            }
        }
        Some(response::Payload::OperationList(value)) => {
            println!("operation_count={}", value.operations.len());
            for (index, operation) in value.operations.iter().enumerate() {
                print_operation_summary_fields(index, operation);
            }
        }
        Some(response::Payload::OperationGet(value)) => {
            println!("operation_present={}", value.operation.is_some());
            if let Some(operation) = value.operation.as_ref() {
                print_operation_view_fields(operation);
            }
        }
        Some(response::Payload::OperationRecover(value)) => {
            println!("execution_status={}", sanitize_field(&value.execution_status));
            println!("replayed={}", value.replayed);
            println!("operation_present={}", value.operation.is_some());
            if let Some(operation) = value.operation.as_ref() {
                print_operation_view_fields(operation);
            }
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
        Some(response::Payload::TorrentDiagnose(value)) => {
            println!("torrent_present={}", value.torrent.is_some());
            if let Some(torrent) = value.torrent.as_ref() {
                print_torrent_fields(0, torrent);
            }
            println!("tracker_count={}", value.trackers.len());
            for (index, tracker) in value.trackers.iter().enumerate() {
                println!(
                    "tracker.{index}.identity={}",
                    sanitize_field(&tracker.identity)
                );
                println!(
                    "tracker.{index}.status={}",
                    tracker_status_name(tracker.status)
                );
                println!("tracker.{index}.peers={}", tracker.peers);
                println!("tracker.{index}.seeds={}", tracker.seeds);
                println!("tracker.{index}.leeches={}", tracker.leeches);
                println!(
                    "tracker.{index}.message={}",
                    sanitize_field(&tracker.message)
                );
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
        Some(response::Payload::QueueTarget(value)) => {
            println!("policy_revision={}", value.revision);
            if let Some(target) = value.target_client_count {
                println!("target_client_count={target}");
            } else {
                println!("target_client_count=");
            }
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
        Some(response::Payload::MutationResult(value)) => {
            println!("replayed={}", value.replayed);
            println!("checkpoint={}", sanitize_field(&value.checkpoint));
        }
        None => {}
    }

    for (index, problem) in response.problems.iter().enumerate() {
        println!("problem.{index}.code={}", problem.code);
        println!(
            "problem.{index}.mutation_certainty={}",
            problem.mutation_certainty
        );
        println!("problem.{index}.retry_guidance={}", problem.retry_guidance);
    }

    Ok(())
}

fn print_storage_root_fields(index: usize, root: &qb_proto::v1::StorageRootView) {
    println!("storage_root.{index}.role={}", managed_root_name(root.root));
    println!("storage_root.{index}.path={}", sanitize_field(&root.path));
    println!("storage_root.{index}.volume_id={}", root.volume_id);
    println!("storage_root.{index}.free_bytes={}", root.free_bytes);
    println!("storage_root.{index}.total_bytes={}", root.total_bytes);
}

fn print_torrent_fields(index: usize, torrent: &qb_proto::v1::TorrentSummary) {
    println!("torrent.{index}.id={}", torrent.id);
    println!("torrent.{index}.name={}", sanitize_field(&torrent.name));
    println!(
        "torrent.{index}.state={}",
        torrent_state_name(torrent.state)
    );
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

fn print_operation_human(operation: &OperationView) {
    let Some(summary) = operation.summary.as_ref() else {
        println!("operation payload is missing its summary");
        return;
    };
    println!(
        "{} · {} · registry {} · torrent {}",
        summary.operation_id, summary.state, summary.registry_id, summary.torrent_id
    );
    println!(
        "  source: {} · archive receipt: {} · files moved+receipted: {}/{} · content audit: {}",
        operation.source_relative,
        if summary.archive_receipted { "yes" } else { "no" },
        summary.files_moved_and_receipted,
        summary.files_total,
        if summary.post_handoff_content_audited {
            "performed"
        } else {
            "not performed"
        }
    );
    if let Some(problem_code) = summary.problem_code.as_deref() {
        println!("  problem: {problem_code}");
    }
    for file in &operation.files {
        println!(
            "  file {} · {} · {} bytes · {} · {} · destination receipt {}",
            file.index,
            file.relative_path,
            file.size,
            file.strategy,
            file.state,
            if file.destination_receipted { "yes" } else { "no" }
        );
    }
}

fn print_operation_summary_fields(index: usize, operation: &qb_proto::v1::OperationSummary) {
    println!(
        "operation.{index}.operation_id={}",
        sanitize_field(&operation.operation_id)
    );
    println!(
        "operation.{index}.request_id={}",
        sanitize_field(&operation.request_id)
    );
    println!("operation.{index}.kind={}", sanitize_field(&operation.kind));
    println!("operation.{index}.state={}", sanitize_field(&operation.state));
    println!(
        "operation.{index}.registry_id={}",
        sanitize_field(&operation.registry_id)
    );
    println!(
        "operation.{index}.torrent_id={}",
        sanitize_field(&operation.torrent_id)
    );
    println!(
        "operation.{index}.problem_code={}",
        sanitize_field(operation.problem_code.as_deref().unwrap_or(""))
    );
    println!("operation.{index}.revision={}", operation.revision);
    println!("operation.{index}.files_total={}", operation.files_total);
    println!(
        "operation.{index}.files_moved_and_receipted={}",
        operation.files_moved_and_receipted
    );
    println!(
        "operation.{index}.archive_receipted={}",
        operation.archive_receipted
    );
    println!(
        "operation.{index}.post_handoff_content_audited={}",
        operation.post_handoff_content_audited
    );
}

fn print_operation_view_fields(operation: &OperationView) {
    if let Some(summary) = operation.summary.as_ref() {
        print_operation_summary_fields(0, summary);
    }
    println!(
        "operation.source_relative={}",
        sanitize_field(&operation.source_relative)
    );
    println!("operation.file_count={}", operation.files.len());
    for file in &operation.files {
        println!(
            "operation.file.{}.relative_path={}",
            file.index,
            sanitize_field(&file.relative_path)
        );
        println!("operation.file.{}.size={}", file.index, file.size);
        println!(
            "operation.file.{}.strategy={}",
            file.index,
            sanitize_field(&file.strategy)
        );
        println!(
            "operation.file.{}.state={}",
            file.index,
            sanitize_field(&file.state)
        );
        println!(
            "operation.file.{}.destination_receipted={}",
            file.index, file.destination_receipted
        );
        println!(
            "operation.file.{}.problem_code={}",
            file.index,
            sanitize_field(file.problem_code.as_deref().unwrap_or(""))
        );
        println!("operation.file.{}.revision={}", file.index, file.revision);
    }
}

fn managed_root_name(value: i32) -> &'static str {
    ManagedRootView::try_from(value)
        .unwrap_or(ManagedRootView::Unspecified)
        .as_str_name()
}

fn tracker_status_name(value: i32) -> &'static str {
    TrackerStatusView::try_from(value)
        .unwrap_or(TrackerStatusView::Unknown)
        .as_str_name()
}

fn exit_for_response(response: &Response) -> ExitCode {
    match Status::try_from(response.status).unwrap_or(Status::Error) {
        Status::Ok => ExitCode::SUCCESS,
        Status::Pending | Status::Partial => ExitCode::from(4),
        Status::Blocked => ExitCode::from(3),
        Status::Unknown => ExitCode::from(5),
        Status::Unspecified => ExitCode::from(8),
        Status::Error => exit_for_error(response),
    }
}

fn exit_for_error(response: &Response) -> ExitCode {
    let category = response
        .problems
        .first()
        .and_then(|problem| ProblemCategory::try_from(problem.category).ok())
        .unwrap_or(ProblemCategory::Internal);

    match category {
        ProblemCategory::Usage => ExitCode::from(2),
        ProblemCategory::State => ExitCode::from(3),
        ProblemCategory::Availability => ExitCode::from(6),
        ProblemCategory::Protocol => ExitCode::from(7),
        ProblemCategory::Unspecified | ProblemCategory::Internal => ExitCode::from(8),
    }
}
