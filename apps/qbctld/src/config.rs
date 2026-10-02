use std::{fs, path::Path};

use anyhow::{bail, Context, Result};
use qb_ipc::DEFAULT_PIPE;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct Config {
    pub pipe: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    revision: Option<u32>,
    ipc: Option<IpcConfig>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IpcConfig {
    pipe: Option<String>,
}

impl Config {
    pub fn load(runtime_root: &Path) -> Result<Self> {
        let path = runtime_root.join("config.toml");
        let mut config = if path.exists() {
            let raw =
                fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
            let file: FileConfig =
                toml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;

            if file.revision.unwrap_or(1) != 1 {
                bail!("unsupported config revision");
            }

            Self {
                pipe: file
                    .ipc
                    .and_then(|ipc| ipc.pipe)
                    .unwrap_or_else(|| DEFAULT_PIPE.to_string()),
            }
        } else {
            Self {
                pipe: DEFAULT_PIPE.to_string(),
            }
        };

        if let Ok(pipe) = std::env::var("QBCTL_PIPE") {
            config.pipe = pipe;
        }

        if !config.pipe.starts_with(r"\\.\pipe\") {
            bail!("IPC pipe must be a local Windows named pipe");
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
}
