use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc,
};

use qb_application::{
    mutation::{
        MutationCommand, MutationExecutionResult, MutationExecutionStatus, MutationJournal,
        MutationService, TorrentControlAction,
    },
    torrent::{
        ConnectionStatus, EffectAttempt, EffectFuture, NetworkPreferences, PortFuture, QbitProbe,
        QueueSettings, TorrentClient, TorrentView, TrackerEvidence, TransferInfo,
    },
    PortError,
};
use qb_domain::{
    torrent::{TorrentId, TorrentState},
    RequestId,
};
use qb_journal::Journal;

const DOWNLOADING: u8 = 0;
const STOPPED: u8 = 1;
const UNKNOWN: u8 = 2;

struct FakeTorrentClient {
    state: AtomicU8,
    uncertain_stop: AtomicBool,
    stop_calls: AtomicUsize,
}

impl FakeTorrentClient {
    fn new(state: u8, uncertain_stop: bool) -> Self {
        Self {
            state: AtomicU8::new(state),
            uncertain_stop: AtomicBool::new(uncertain_stop),
            stop_calls: AtomicUsize::new(0),
        }
    }

    fn set_state(&self, state: u8) {
        self.state.store(state, Ordering::SeqCst);
    }

    fn torrent_state(&self) -> TorrentState {
        match self.state.load(Ordering::SeqCst) {
            DOWNLOADING => TorrentState::Downloading,
            STOPPED => TorrentState::Stopped,
            _ => TorrentState::Unknown,
        }
    }
}

impl TorrentClient for FakeTorrentClient {
    fn probe(&self) -> PortFuture<'_, QbitProbe> {
        Box::pin(async {
            Ok(QbitProbe {
                application_version: "v5.2.0".into(),
                webapi_version: "2.16.2".into(),
                mutation_ready: true,
            })
        })
    }

    fn list(&self) -> PortFuture<'_, Vec<TorrentView>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn get<'a>(&'a self, id: &'a TorrentId) -> PortFuture<'a, Option<TorrentView>> {
        Box::pin(async move {
            Ok(Some(TorrentView {
                id: id.clone(),
                name: "fixture".into(),
                state: self.torrent_state(),
                total_bytes: 100,
                remaining_bytes: 50,
                download_rate_bps: 0,
                upload_rate_bps: 0,
                progress_ppm: 500_000,
                availability: Some(1.0),
                peers_connected: 0,
                peers_known: 0,
                seeds_connected: 0,
                seeds_known: 0,
            }))
        })
    }

    fn transfer_info(&self) -> PortFuture<'_, TransferInfo> {
        Box::pin(async {
            Ok(TransferInfo {
                download_rate_bps: 0,
                upload_rate_bps: 0,
                download_limit_bps: 0,
                upload_limit_bps: 0,
                dht_nodes: 0,
                connection_status: ConnectionStatus::Connected,
            })
        })
    }

    fn queue_settings(&self) -> PortFuture<'_, QueueSettings> {
        Box::pin(async {
            Ok(QueueSettings {
                queueing_enabled: true,
                max_active_downloads: 10,
                max_active_torrents: 20,
                dont_count_slow_torrents: false,
            })
        })
    }

    fn network_preferences(&self) -> PortFuture<'_, NetworkPreferences> {
        Box::pin(async {
            Ok(NetworkPreferences {
                listen_port: 6881,
                upnp: false,
                dht: true,
                pex: true,
                lsd: true,
                current_network_interface: String::new(),
                current_interface_address: String::new(),
                max_connections: 500,
                max_connections_per_torrent: 100,
            })
        })
    }

    fn trackers<'a>(&'a self, _id: &'a TorrentId) -> PortFuture<'a, Vec<TrackerEvidence>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stop<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
        Box::pin(async move {
            self.stop_calls.fetch_add(1, Ordering::SeqCst);
            self.state.store(STOPPED, Ordering::SeqCst);
            if self.uncertain_stop.load(Ordering::SeqCst) {
                EffectAttempt::Uncertain(PortError::new(
                    "QBIT_MUTATION_UNCERTAIN",
                    "fixture dropped response",
                ))
            } else {
                EffectAttempt::Accepted
            }
        })
    }

    fn start<'a>(&'a self, _id: &'a TorrentId) -> EffectFuture<'a> {
        Box::pin(async move {
            self.state.store(DOWNLOADING, Ordering::SeqCst);
            EffectAttempt::Accepted
        })
    }

    fn set_active_downloads(&self, _value: u32) -> EffectFuture<'_> {
        Box::pin(async { EffectAttempt::Accepted })
    }

    fn set_download_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
        Box::pin(async { EffectAttempt::Accepted })
    }

    fn set_upload_limit(&self, _bytes_per_sec: u64) -> EffectFuture<'_> {
        Box::pin(async { EffectAttempt::Accepted })
    }
}

fn stop_command() -> MutationCommand {
    MutationCommand::TorrentControl {
        torrent_id: TorrentId::new("abcdef0123456789abcdef0123456789abcdef01")
            .expect("torrent id"),
        action: TorrentControlAction::Stop,
    }
}

fn service(
    path: &std::path::Path,
    client: Arc<FakeTorrentClient>,
) -> (MutationService, Arc<Journal>) {
    let journal = Arc::new(Journal::open(path).expect("journal"));
    let journal_port: Arc<dyn MutationJournal> = journal.clone();
    let client_port: Arc<dyn TorrentClient> = client;
    (
        MutationService::new(journal_port, Some(client_port)),
        journal,
    )
}

#[tokio::test]
async fn uncertain_effect_replay_observes_without_duplicate_stop() {
    let dir = tempfile::tempdir().expect("tempdir");
    let client = Arc::new(FakeTorrentClient::new(DOWNLOADING, true));
    let (service, _journal) = service(&dir.path().join("state.sqlite"), Arc::clone(&client));
    let request_id = RequestId::new("uncertain-stop").expect("request id");
    let command = stop_command();

    let first = service
        .execute(&request_id, command.clone())
        .await
        .expect("first execute");
    let first = match first {
        MutationExecutionResult::Execution(value) => value,
        other => panic!("unexpected result: {other:?}"),
    };
    assert_eq!(first.status, MutationExecutionStatus::Unknown);
    let operation_id = first.record.operation_id.clone();
    assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);

    let replay = service
        .execute(&request_id, command.clone())
        .await
        .expect("replay");
    let replay = match replay {
        MutationExecutionResult::Execution(value) => value,
        other => panic!("unexpected replay: {other:?}"),
    };
    assert_eq!(replay.status, MutationExecutionStatus::Finished);
    assert!(replay.replayed);
    assert_eq!(replay.record.operation_id, operation_id);
    assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);

    let conflict = service
        .execute(
            &request_id,
            MutationCommand::TorrentControl {
                torrent_id: match command {
                    MutationCommand::TorrentControl { torrent_id, .. } => torrent_id,
                    _ => unreachable!(),
                },
                action: TorrentControlAction::Start,
            },
        )
        .await
        .expect("conflict");
    assert!(matches!(
        conflict,
        MutationExecutionResult::Conflict {
            operation_id: existing
        } if existing == operation_id
    ));
}

#[tokio::test]
async fn blocked_preflight_is_not_automatic_restart_work() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("state.sqlite");
    let client = Arc::new(FakeTorrentClient::new(UNKNOWN, false));
    let (service, journal) = service(&path, Arc::clone(&client));
    let request_id = RequestId::new("blocked-stop").expect("request id");
    let command = stop_command();

    let first = service
        .execute(&request_id, command.clone())
        .await
        .expect("blocked execute");
    let first = match first {
        MutationExecutionResult::Execution(value) => value,
        other => panic!("unexpected result: {other:?}"),
    };
    assert_eq!(first.status, MutationExecutionStatus::Blocked);
    assert_eq!(client.stop_calls.load(Ordering::SeqCst), 0);

    let recoverable = MutationJournal::list_recoverable(journal.as_ref()).expect("recoverable");
    assert!(recoverable.is_empty());

    client.set_state(DOWNLOADING);
    let replay = service
        .execute(&request_id, command)
        .await
        .expect("explicit retry");
    let replay = match replay {
        MutationExecutionResult::Execution(value) => value,
        other => panic!("unexpected replay: {other:?}"),
    };
    assert_eq!(replay.status, MutationExecutionStatus::Finished);
    assert_eq!(client.stop_calls.load(Ordering::SeqCst), 1);
}
