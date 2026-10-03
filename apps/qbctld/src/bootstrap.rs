use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result};
use qb_application::{
    cleanup::{IncomingCleanupJournal, IncomingCleanupService},
    incoming::IncomingService,
    mutation::{MutationJournal, MutationService},
    registry::TorrentRegistry,
    storage::{IncomingScanService, Storage},
    system::{RuntimeHealthPort, SystemService},
    torrent::{MetainfoReader, TorrentClient, TorrentService},
    JournalHealthPort,
};
use qb_journal::Journal;
use qb_metainfo::{LocalMetainfoReader, MAX_METAINFO_BYTES};
use qb_qbit::{QbitClient, QbitCredentials};
use qb_win::{
    credentials::read_generic,
    storage::{ManagedRootLayout, ManagedStorage},
    InstanceGuard, RuntimeMode, RuntimeRoot,
};
use uuid::Uuid;

use crate::{
    config::{Config, QbitConfig, StorageConfig},
    runtime::RuntimeContext,
};

pub struct Bootstrap {
    pub config: Config,
    pub runtime: Arc<RuntimeContext>,
    pub system: Arc<SystemService>,
    pub torrents: Option<Arc<TorrentService>>,
    pub mutations: Arc<MutationService>,
    pub incoming: Option<Arc<IncomingService>>,
    pub qbit_startup_problem: Option<String>,
    _instance_guard: InstanceGuard,
}

pub fn build(runtime_override: Option<PathBuf>) -> Result<Bootstrap> {
    let runtime_root = match runtime_override {
        Some(path) => RuntimeRoot::at(path),
        None => RuntimeRoot::discover(RuntimeMode::User).context("discover runtime root")?,
    };
    runtime_root.ensure().context("create runtime root")?;

    let instance_guard =
        InstanceGuard::acquire(&runtime_root).context("acquire daemon ownership")?;
    let config = Config::load(runtime_root.path()).context("load config")?;

    let journal =
        Arc::new(Journal::open(runtime_root.path().join("state.sqlite")).context("open journal")?);
    let journal_health: Arc<dyn JournalHealthPort> = journal.clone();
    let mutation_journal: Arc<dyn MutationJournal> = journal.clone();
    let registry: Arc<dyn TorrentRegistry> = journal.clone();
    let cleanup_journal: Arc<dyn IncomingCleanupJournal> = journal.clone();

    let runtime = Arc::new(RuntimeContext::new(
        Uuid::new_v4().to_string(),
        runtime_root.path().to_path_buf(),
    ));
    let runtime_port: Arc<dyn RuntimeHealthPort> = runtime.clone();
    let system = Arc::new(SystemService::new(journal_health, runtime_port));

    let incoming = config
        .storage
        .as_ref()
        .map(|storage| {
            build_incoming_service(
                storage,
                runtime_root.path(),
                registry.clone(),
                cleanup_journal.clone(),
            )
        })
        .transpose()?;

    let (torrents, mutation_client, qbit_startup_problem) = match config.qbittorrent.as_ref() {
        Some(qbit) => match build_qbit_client(qbit) {
            Ok(client) => {
                let torrent_port: Arc<dyn TorrentClient> = client.clone();
                let mutation_port: Arc<dyn TorrentClient> = client;
                (
                    Some(Arc::new(TorrentService::new(torrent_port))),
                    Some(mutation_port),
                    None,
                )
            }
            Err(error) => (None, None, Some(error.to_string())),
        },
        None => (
            None,
            None,
            Some("qBittorrent is not configured for the Rust daemon".into()),
        ),
    };
    let mutations = Arc::new(MutationService::new(mutation_journal, mutation_client));

    Ok(Bootstrap {
        config,
        runtime,
        system,
        torrents,
        mutations,
        incoming,
        qbit_startup_problem,
        _instance_guard: instance_guard,
    })
}

fn build_incoming_service(
    config: &StorageConfig,
    runtime_root: &Path,
    registry: Arc<dyn TorrentRegistry>,
    cleanup_journal: Arc<dyn IncomingCleanupJournal>,
) -> Result<Arc<IncomingService>> {
    let roots = ManagedRootLayout {
        incoming: config.incoming.clone(),
        archive: config.archive.clone(),
        working: config.working.clone(),
        completed: config.completed.clone(),
        runtime: runtime_root.to_path_buf(),
    }
    .validate()
    .context("validate managed storage roots")?;

    let storage: Arc<dyn Storage> = Arc::new(ManagedStorage::new(roots));
    let metainfo: Arc<dyn MetainfoReader> = Arc::new(LocalMetainfoReader);
    let scan = IncomingScanService::new(storage.clone(), metainfo, registry);
    let cleanup = IncomingCleanupService::new(cleanup_journal, storage, MAX_METAINFO_BYTES);

    Ok(Arc::new(IncomingService::new(
        scan,
        cleanup,
        MAX_METAINFO_BYTES,
    )))
}

fn build_qbit_client(config: &QbitConfig) -> Result<Arc<QbitClient>> {
    let secret = match std::env::var("QBCTL_QBIT_PASSWORD") {
        Ok(secret) => secret,
        Err(_) => {
            read_generic(&config.credential)
                .with_context(|| format!("read credential '{}'", config.credential))?
                .secret
        }
    };

    let client = QbitClient::new(
        &config.url,
        QbitCredentials::new(&config.username, secret),
        Duration::from_secs(config.request_timeout_seconds),
    )
    .context("build qBittorrent client")?;

    Ok(Arc::new(client))
}
