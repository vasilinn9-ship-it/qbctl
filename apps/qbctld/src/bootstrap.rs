use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use qb_application::{
    mutation::{MutationJournal, MutationService},
    system::{RuntimeHealthPort, SystemService},
    torrent::{TorrentClient, TorrentService},
    JournalHealthPort,
};
use qb_journal::Journal;
use qb_qbit::{QbitClient, QbitCredentials};
use qb_win::{credentials::read_generic, InstanceGuard, RuntimeMode, RuntimeRoot};
use uuid::Uuid;

use crate::{
    config::{Config, QbitConfig},
    runtime::RuntimeContext,
};

pub struct Bootstrap {
    pub config: Config,
    pub runtime: Arc<RuntimeContext>,
    pub system: Arc<SystemService>,
    pub torrents: Option<Arc<TorrentService>>,
    pub mutations: Option<Arc<MutationService>>,
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

    let journal = Arc::new(
        Journal::open(runtime_root.path().join("state.sqlite")).context("open journal")?,
    );
    let journal_health: Arc<dyn JournalHealthPort> = journal.clone();
    let mutation_journal: Arc<dyn MutationJournal> = journal;

    let runtime = Arc::new(RuntimeContext::new(
        Uuid::new_v4().to_string(),
        runtime_root.path().to_path_buf(),
    ));
    let runtime_port: Arc<dyn RuntimeHealthPort> = runtime.clone();
    let system = Arc::new(SystemService::new(journal_health, runtime_port));

    let (torrents, mutations, qbit_startup_problem) = match config.qbittorrent.as_ref() {
        Some(qbit) => match build_qbit_services(qbit, mutation_journal) {
            Ok((torrents, mutations)) => (Some(torrents), Some(mutations), None),
            Err(error) => (None, None, Some(error.to_string())),
        },
        None => (
            None,
            None,
            Some("qBittorrent is not configured for the Rust daemon".into()),
        ),
    };

    Ok(Bootstrap {
        config,
        runtime,
        system,
        torrents,
        mutations,
        qbit_startup_problem,
        _instance_guard: instance_guard,
    })
}

fn build_qbit_services(
    config: &QbitConfig,
    journal: Arc<dyn MutationJournal>,
) -> Result<(Arc<TorrentService>, Arc<MutationService>)> {
    let secret = match std::env::var("QBCTL_QBIT_PASSWORD") {
        Ok(secret) => secret,
        Err(_) => {
            read_generic(&config.credential)
                .with_context(|| format!("read credential '{}'", config.credential))?
                .secret
        }
    };

    let client = Arc::new(
        QbitClient::new(
            &config.url,
            QbitCredentials::new(&config.username, secret),
            Duration::from_secs(config.request_timeout_seconds),
        )
        .context("build qBittorrent client")?,
    );

    let torrent_port: Arc<dyn TorrentClient> = client.clone();
    let mutation_port: Arc<dyn TorrentClient> = client;
    let torrents = Arc::new(TorrentService::new(torrent_port));
    let mutations = Arc::new(MutationService::new(journal, mutation_port));

    Ok((torrents, mutations))
}
