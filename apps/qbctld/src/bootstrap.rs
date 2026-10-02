use std::{path::PathBuf, sync::Arc};

use anyhow::{Context as _, Result};
use qb_application::{
    system::{RuntimeHealthPort, SystemService},
    JournalHealthPort,
};
use qb_journal::Journal;
use qb_win::{InstanceGuard, RuntimeMode, RuntimeRoot};
use uuid::Uuid;

use crate::{config::Config, runtime::RuntimeContext};

pub struct Bootstrap {
    pub config: Config,
    pub runtime: Arc<RuntimeContext>,
    pub system: Arc<SystemService>,
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

    Ok(Bootstrap {
        config,
        runtime,
        system,
        _instance_guard: instance_guard,
    })
}
