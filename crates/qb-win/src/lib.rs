use std::{
    env,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeMode {
    User,
    Service,
}

#[derive(Clone, Debug)]
pub struct RuntimeRoot(PathBuf);

impl RuntimeRoot {
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    pub fn discover(mode: RuntimeMode) -> Result<Self, PlatformError> {
        if let Some(override_path) = env::var_os("QBCTL_RUNTIME_DIR") {
            return Ok(Self(PathBuf::from(override_path)));
        }

        let variable = match mode {
            RuntimeMode::User => "LOCALAPPDATA",
            RuntimeMode::Service => "ProgramData",
        };

        let base = env::var_os(variable).ok_or(PlatformError::RuntimeRootUnavailable(variable))?;
        Ok(Self(PathBuf::from(base).join("qbctl")))
    }

    pub fn ensure(&self) -> Result<(), PlatformError> {
        fs::create_dir_all(&self.0)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Error)]
pub enum PlatformError {
    #[error("environment variable {0} is unavailable")]
    RuntimeRootUnavailable(&'static str),
    #[error("another qbctld instance already owns this runtime root")]
    AlreadyRunning,
    #[error("platform I/O error: {0}")]
    Io(#[from] io::Error),
}

pub struct InstanceGuard {
    file: File,
}

impl InstanceGuard {
    pub fn acquire(root: &RuntimeRoot) -> Result<Self, PlatformError> {
        root.ensure()?;
        let path = root.path().join("daemon.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Err(PlatformError::AlreadyRunning)
            }
            Err(error) => Err(PlatformError::Io(error)),
        }
    }
}

impl Drop for InstanceGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_root_can_be_explicit() {
        let root = RuntimeRoot::at("example");
        assert_eq!(root.path(), Path::new("example"));
    }
}
