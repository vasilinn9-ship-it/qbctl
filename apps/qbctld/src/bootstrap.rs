use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use qb_application::{
    system::{RuntimeHealthPort, SystemService},
    torrent::{TorrentClient, TorrentService},
    JournalHealthPort,
};
use qb_journal::Journal;
use qb_qbit::{QbitClient, QbitCredentials};
use qb_win::{
    credentials::read_generic,
    InstanceGuard, RuntimeMode, RuntimeRoot,
};
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

    let journal: Arc<dyn JournalHealthPort> =
        Arc::new(Journal::open(runtime_root.path().join("state.sqlite")).context("open journal")?);

    let runtime = Arc::new(RuntimeContext::new(
        Uuid::new_v4().to_string(),
        runtime_root.path().to_path_buf(),
    ));
    let runtime_port: Arc<dyn RuntimeHealthPort> = runtime.clone();
    let system = Arc::new(SystemService::new(journal, runtime_port));

    let (torrents, qbit_startup_problem) = match config.qbittorrent.as_ref() {
        Some(qbit) => match build_torrent_service(qbit) {
            Ok(service) => (Some(service), None),
            Err(error) => (None, Some(error.to_string())),
        },
        None => (
            None,
            Some("qBittorrent is not configured for the Rust daemon".into()),
        ),
    };

    Ok(Bootstrap {
        config,
        runtime,
        system,
        torrents,
        qbit_startup_problem,
        _instance_guard: instance_guard,
    })
}

fn build_torrent_service(config: &QbitConfig) -> Result<Arc<TorrentService>> {
    let secret = match std::env::var("QBCTL_QBIT_PASSWORD") {
        Ok(secret) => secret,
        Err(_) => read_generic(&config.credential)
            .with_context(|| format!("read credential '{}'", config.credential))?
            .secret,
    };

    let client = QbitClient::new(
        &config.url,
        QbitCredentials::new(&config.username, secret),
        Duration::from_secs(config.request_timeout_seconds),
    )
    .context("build qBittorrent client")?;

    let port: Arc<dyn TorrentClient> = Arc::new(client);
    Ok(Arc::new(TorrentService::new(port)))
}
