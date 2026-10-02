use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use qb_ipc::DEFAULT_PIPE;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct Config {
    pub pipe: String,
    pub qbittorrent: Option<QbitConfig>,
}

#[derive(Clone, Debug)]
pub struct QbitConfig {
    pub url: String,
    pub username: String,
    pub credential: String,
    pub request_timeout_seconds: u64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    revision: Option<u32>,
    ipc: Option<IpcConfig>,
    qbittorrent: Option<FileQbitConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcConfig {
    pipe: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileQbitConfig {
    url: String,
    username: String,
    credential: String,
    #[serde(default = "default_qbit_timeout")]
    request_timeout_seconds: u64,
}

const fn default_qbit_timeout() -> u64 {
    4
}

impl Config {
    pub fn load(runtime_root: &Path) -> Result<Self> {
        let path = runtime_root.join("config.toml");
        let file = if path.exists() {
            let raw =
                fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
            let file: FileConfig =
                toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;

            if file.revision.unwrap_or(1) != 1 {
                bail!("unsupported config revision");
            }
            file
        } else {
            FileConfig::default()
        };

        let mut config = Self {
            pipe: file
                .ipc
                .and_then(|ipc| ipc.pipe)
                .unwrap_or_else(|| DEFAULT_PIPE.to_string()),
            qbittorrent: file.qbittorrent.map(|qbit| QbitConfig {
                url: qbit.url,
                username: qbit.username,
                credential: qbit.credential,
                request_timeout_seconds: qbit.request_timeout_seconds,
            }),
        };

        if let Ok(pipe) = std::env::var("QBCTL_PIPE") {
            config.pipe = pipe;
        }

        if !config.pipe.starts_with(r"\\.\pipe\") {
            bail!("IPC pipe must be a local Windows named pipe");
        }

        if let Some(qbit) = config.qbittorrent.as_mut() {
            if let Ok(url) = std::env::var("QBCTL_QBIT_URL") {
                qbit.url = url;
            }
            if let Ok(username) = std::env::var("QBCTL_QBIT_USERNAME") {
                qbit.username = username;
            }

            if qbit.username.trim().is_empty() {
                bail!("qBittorrent username must not be empty");
            }
            if qbit.credential.trim().is_empty() && std::env::var_os("QBCTL_QBIT_PASSWORD").is_none()
            {
                bail!("qBittorrent credential reference must not be empty");
            }
            if !(1..=30).contains(&qbit.request_timeout_seconds) {
                bail!("qBittorrent request_timeout_seconds must be in 1..=30");
            }
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_top_level_fields() {
        let error = toml::from_str::<FileConfig>(
            r#"
revision = 1
[api]
url = "http://127.0.0.1:8080"
"#,
        )
        .expect_err("legacy Python config must not be silently accepted");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_unknown_ipc_fields() {
        let error = toml::from_str::<FileConfig>(
            r#"
revision = 1
[ipc]
pipe = "\\\\.\\pipe\\qbctl"
unexpected = true
"#,
        )
        .expect_err("unknown IPC field must fail");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn parses_qbit_settings_without_secret() {
        let config = toml::from_str::<FileConfig>(
            r#"
revision = 1
[qbittorrent]
url = "http://127.0.0.1:8080"
username = "admin"
credential = "qbctl/qbittorrent"
request_timeout_seconds = 4
"#,
        )
        .expect("qB config");

        let qbit = config.qbittorrent.expect("qB section");
        assert_eq!(qbit.username, "admin");
        assert_eq!(qbit.credential, "qbctl/qbittorrent");
    }
}
