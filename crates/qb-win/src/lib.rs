pub mod credentials;
pub mod storage;

use std::{
    env,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

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
    _file: File,
}

impl InstanceGuard {
    pub fn acquire(root: &RuntimeRoot) -> Result<Self, PlatformError> {
        root.ensure()?;
        let path = root.path().join("daemon.lock");

        acquire_instance_file(&path).map(|file| Self { _file: file })
    }
}

#[cfg(windows)]
fn acquire_instance_file(path: &Path) -> Result<File, PlatformError> {
    use std::os::windows::fs::OpenOptionsExt;

    // Windows CreateFile sharing violation. We intentionally hold a handle
    // opened with dwShareMode=0 for the lifetime of the daemon.
    const ERROR_SHARING_VIOLATION: i32 = 32;

    match OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .share_mode(0)
        .open(path)
    {
        Ok(file) => Ok(file),
        Err(error) if error.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => {
            Err(PlatformError::AlreadyRunning)
        }
        Err(error) => Err(PlatformError::Io(error)),
    }
}

#[cfg(not(windows))]
fn acquire_instance_file(path: &Path) -> Result<File, PlatformError> {
    use fs2::FileExt;

    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;

    match file.try_lock_exclusive() {
        Ok(()) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            Err(PlatformError::AlreadyRunning)
        }
        Err(error) => Err(PlatformError::Io(error)),
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
