use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::{Component, Path, PathBuf},
};

use qb_application::{
    storage::{
        FileEvidence, FileIdentity, IncomingFileSnapshot, ManagedRoot, Storage, StorageVolumeStatus,
    },
    PortError,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagedRootRole {
    Incoming,
    Archive,
    Working,
    Completed,
    Runtime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRelativePath(PathBuf);

impl ManagedRelativePath {
    pub fn parse(value: &str) -> Result<Self, StorageError> {
        if value.is_empty() {
            return Err(StorageError::RelativePathEmpty);
        }
        if value.starts_with('/') || value.starts_with('\\') || looks_drive_relative(value) {
            return Err(StorageError::RelativePathAbsolute(value.to_string()));
        }

        let mut normalized = PathBuf::new();
        for component in value.split(['/', '\\']) {
            validate_relative_component(component)?;
            normalized.push(component);
        }

        Ok(Self(normalized))
    }

    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub struct ManagedRootLayout {
    pub incoming: PathBuf,
    pub archive: PathBuf,
    pub working: PathBuf,
    pub completed: PathBuf,
    pub runtime: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedManagedRootLayout {
    pub incoming: PathBuf,
    pub archive: PathBuf,
    pub working: PathBuf,
    pub completed: PathBuf,
    pub runtime: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VolumeObservation {
    pub role: ManagedRootRole,
    pub volume_root: PathBuf,
    pub serial_number: u32,
    pub free_bytes: u64,
    pub total_bytes: u64,
}

impl ManagedRootLayout {
    pub fn validate(&self) -> Result<ValidatedManagedRootLayout, StorageError> {
        let roots = [
            (
                ManagedRootRole::Incoming,
                validate_root(ManagedRootRole::Incoming, &self.incoming)?,
            ),
            (
                ManagedRootRole::Archive,
                validate_root(ManagedRootRole::Archive, &self.archive)?,
            ),
            (
                ManagedRootRole::Working,
                validate_root(ManagedRootRole::Working, &self.working)?,
            ),
            (
                ManagedRootRole::Completed,
                validate_root(ManagedRootRole::Completed, &self.completed)?,
            ),
            (
                ManagedRootRole::Runtime,
                validate_root(ManagedRootRole::Runtime, &self.runtime)?,
            ),
        ];

        for left in 0..roots.len() {
            for right in (left + 1)..roots.len() {
                let (left_role, left_path) = &roots[left];
                let (right_role, right_path) = &roots[right];
                if roots_overlap(left_path, right_path) {
                    return Err(StorageError::RootOverlap {
                        first: *left_role,
                        second: *right_role,
                    });
                }
            }
        }

        Ok(ValidatedManagedRootLayout {
            incoming: roots[0].1.clone(),
            archive: roots[1].1.clone(),
            working: roots[2].1.clone(),
            completed: roots[3].1.clone(),
            runtime: roots[4].1.clone(),
        })
    }
}

impl ValidatedManagedRootLayout {
    pub fn path(&self, role: ManagedRootRole) -> &Path {
        match role {
            ManagedRootRole::Incoming => &self.incoming,
            ManagedRootRole::Archive => &self.archive,
            ManagedRootRole::Working => &self.working,
            ManagedRootRole::Completed => &self.completed,
            ManagedRootRole::Runtime => &self.runtime,
        }
    }

    pub fn observe_volume(&self, role: ManagedRootRole) -> Result<VolumeObservation, StorageError> {
        observe_volume(role, self.path(role))
    }
}

#[derive(Clone, Debug)]
pub struct ManagedStorage {
    roots: ValidatedManagedRootLayout,
}

impl ManagedStorage {
    pub fn new(roots: ValidatedManagedRootLayout) -> Self {
        Self { roots }
    }

    pub fn roots(&self) -> &ValidatedManagedRootLayout {
        &self.roots
    }

    fn revalidate_root(&self, role: ManagedRootRole) -> Result<(), PortError> {
        let configured = self.roots.path(role);
        let refreshed = validate_root(role, configured).map_err(map_storage_port_error)?;
        if !paths_equal(&refreshed, configured) {
            return Err(PortError::new(
                "STORAGE_ROOT_CHANGED",
                format!("{role:?} root identity changed after validation"),
            ));
        }
        Ok(())
    }
}

impl Storage for ManagedStorage {
    fn volume_status(&self, root: ManagedRoot) -> Result<StorageVolumeStatus, PortError> {
        let role = match root {
            ManagedRoot::Incoming => ManagedRootRole::Incoming,
            ManagedRoot::Archive => ManagedRootRole::Archive,
            ManagedRoot::Working => ManagedRootRole::Working,
            ManagedRoot::Completed => ManagedRootRole::Completed,
            ManagedRoot::Runtime => ManagedRootRole::Runtime,
        };
        self.revalidate_root(role)?;
        let volume = self
            .roots
            .observe_volume(role)
            .map_err(map_storage_port_error)?;
        Ok(StorageVolumeStatus {
            root,
            volume_id: u64::from(volume.serial_number),
            free_bytes: volume.free_bytes,
            total_bytes: volume.total_bytes,
        })
    }

    fn list_incoming(&self) -> Result<Vec<String>, PortError> {
        self.revalidate_root(ManagedRootRole::Incoming)?;

        let entries = fs::read_dir(&self.roots.incoming)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        let mut paths = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
            let name = entry.file_name().into_string().map_err(|_| {
                PortError::new(
                    "STORAGE_PATH_INVALID",
                    "Incoming contains a non-Unicode entry name",
                )
            })?;
            if !is_torrent_name(&name) {
                continue;
            }
            ManagedRelativePath::parse(&name).map_err(|_| {
                PortError::new(
                    "STORAGE_PATH_INVALID",
                    format!("unsafe Incoming entry name: {name}"),
                )
            })?;
            paths.push(name);
        }

        paths.sort_by(|left, right| {
            left.to_ascii_lowercase()
                .cmp(&right.to_ascii_lowercase())
                .then_with(|| left.cmp(right))
        });
        Ok(paths)
    }

    fn read_incoming(
        &self,
        relative_path: &str,
        max_bytes: usize,
    ) -> Result<IncomingFileSnapshot, PortError> {
        if max_bytes == 0 {
            return Err(PortError::new(
                "STORAGE_LIMIT_INVALID",
                "Incoming read limit must be greater than zero",
            ));
        }

        self.revalidate_root(ManagedRootRole::Incoming)?;

        let managed = ManagedRelativePath::parse(relative_path).map_err(|_| {
            PortError::new(
                "STORAGE_PATH_INVALID",
                format!("unsafe Incoming path: {relative_path}"),
            )
        })?;
        if managed.as_path().components().count() != 1 || !is_torrent_name(relative_path) {
            return Err(PortError::new(
                "STORAGE_PATH_INVALID",
                "Incoming metainfo must be a top-level .torrent file",
            ));
        }

        let path = self.roots.incoming.join(managed.as_path());
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        if is_reparse_metadata(&metadata) {
            return Err(PortError::new(
                "STORAGE_REPARSE_POINT",
                format!("Incoming entry is a reparse point: {relative_path}"),
            ));
        }

        let mut file = open_snapshot_file(&path)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        let before = file_evidence(&file, relative_path)?;

        let read_limit = max_bytes.checked_add(1).ok_or_else(|| {
            PortError::new("STORAGE_LIMIT_INVALID", "Incoming read limit overflow")
        })?;
        let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
        (&mut file)
            .take(u64::try_from(read_limit).unwrap_or(u64::MAX))
            .read_to_end(&mut bytes)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        if bytes.len() > max_bytes {
            return Err(PortError::new(
                "STORAGE_SOURCE_TOO_LARGE",
                format!("Incoming metainfo exceeds {max_bytes} bytes: {relative_path}"),
            ));
        }

        let after = file_evidence(&file, relative_path)?;
        if before != after {
            return Err(PortError::new(
                "STORAGE_SOURCE_CHANGED",
                format!("Incoming file changed while it was read: {relative_path}"),
            ));
        }
        if before.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
            return Err(PortError::new(
                "STORAGE_SOURCE_CHANGED",
                format!("Incoming file size changed while it was read: {relative_path}"),
            ));
        }

        Ok(IncomingFileSnapshot {
            relative_path: relative_path.to_string(),
            evidence: before,
            bytes,
        })
    }
}

fn is_torrent_name(value: &str) -> bool {
    value
        .rsplit_once('.')
        .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("torrent"))
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("managed relative path is empty")]
    RelativePathEmpty,
    #[error("managed relative path must not be absolute or drive-relative: {0}")]
    RelativePathAbsolute(String),
    #[error("managed relative path contains an invalid component: {0}")]
    InvalidRelativeComponent(String),
    #[error("managed relative path uses a reserved Windows device name: {0}")]
    ReservedDeviceName(String),
    #[error("{role:?} root must be an absolute path")]
    RootNotAbsolute { role: ManagedRootRole },
    #[error("{role:?} root must not contain '.' or '..' components")]
    RootContainsTraversal { role: ManagedRootRole },
    #[error("{role:?} root does not resolve to a directory: {path}")]
    RootNotDirectory {
        role: ManagedRootRole,
        path: PathBuf,
    },
    #[error("{role:?} root traverses a symlink/reparse point: {path}")]
    RootReparsePoint {
        role: ManagedRootRole,
        path: PathBuf,
    },
    #[error("{role:?} root is not on a local fixed Windows volume: {path}")]
    RootNotLocalFixedVolume {
        role: ManagedRootRole,
        path: PathBuf,
    },
    #[error("managed roots overlap: {first:?} and {second:?}")]
    RootOverlap {
        first: ManagedRootRole,
        second: ManagedRootRole,
    },
    #[error("Windows volume observation is unavailable on this platform")]
    VolumeObservationUnsupported,
    #[error("{role:?} root I/O error at {path}: {source}")]
    RootIo {
        role: ManagedRootRole,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

fn validate_relative_component(component: &str) -> Result<(), StorageError> {
    if component.is_empty() || matches!(component, "." | "..") {
        return Err(StorageError::InvalidRelativeComponent(
            component.to_string(),
        ));
    }
    if component.ends_with(' ') || component.ends_with('.') {
        return Err(StorageError::InvalidRelativeComponent(
            component.to_string(),
        ));
    }
    if component.encode_utf16().count() > 255
        || component
            .chars()
            .any(|character| character <= '\u{1f}' || r#"<>:"/\\|?*"#.contains(character))
    {
        return Err(StorageError::InvalidRelativeComponent(
            component.to_string(),
        ));
    }
    if is_reserved_device_name(component) {
        return Err(StorageError::ReservedDeviceName(component.to_string()));
    }
    Ok(())
}

fn looks_drive_relative(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

fn is_reserved_device_name(component: &str) -> bool {
    let base = component.split('.').next().unwrap_or(component);
    let upper = base.to_uppercase();
    matches!(
        upper.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$" | "CLOCK$"
    ) || reserved_numbered_device(&upper, "COM")
        || reserved_numbered_device(&upper, "LPT")
}

fn reserved_numbered_device(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(|suffix| {
        matches!(
            suffix,
            "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
        )
    })
}

fn validate_root(role: ManagedRootRole, path: &Path) -> Result<PathBuf, StorageError> {
    if !path.is_absolute() {
        return Err(StorageError::RootNotAbsolute { role });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(StorageError::RootContainsTraversal { role });
    }

    reject_reparse_components(role, path)?;
    let canonical = fs::canonicalize(path).map_err(|source| StorageError::RootIo {
        role,
        path: path.to_path_buf(),
        source,
    })?;
    reject_reparse_components(role, &canonical)?;

    let metadata = fs::metadata(&canonical).map_err(|source| StorageError::RootIo {
        role,
        path: canonical.clone(),
        source,
    })?;
    if !metadata.is_dir() {
        return Err(StorageError::RootNotDirectory {
            role,
            path: canonical,
        });
    }

    ensure_local_fixed_volume(role, &canonical)?;
    Ok(canonical)
}

fn reject_reparse_components(role: ManagedRootRole, path: &Path) -> Result<(), StorageError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_) | Component::RootDir) {
            continue;
        }

        let metadata = fs::symlink_metadata(&current).map_err(|source| StorageError::RootIo {
            role,
            path: current.clone(),
            source,
        })?;
        if is_reparse_metadata(&metadata) {
            return Err(StorageError::RootReparsePoint {
                role,
                path: current,
            });
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse_metadata(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;

    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn is_reparse_metadata(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn ensure_local_fixed_volume(role: ManagedRootRole, path: &Path) -> Result<(), StorageError> {
    use windows_sys::Win32::Storage::FileSystem::GetDriveTypeW;

    // GetDriveTypeW: DRIVE_FIXED is the documented value 3.
    const DRIVE_TYPE_FIXED: u32 = 3;

    let (_, wide) = fixed_drive_root(role, path)?;
    let drive_type = unsafe { GetDriveTypeW(wide.as_ptr()) };
    if drive_type != DRIVE_TYPE_FIXED {
        return Err(StorageError::RootNotLocalFixedVolume {
            role,
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(windows)]
fn fixed_drive_root(
    role: ManagedRootRole,
    path: &Path,
) -> Result<(PathBuf, [u16; 4]), StorageError> {
    use std::path::Prefix;

    let letter = match path.components().next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => letter,
            _ => {
                return Err(StorageError::RootNotLocalFixedVolume {
                    role,
                    path: path.to_path_buf(),
                });
            }
        },
        _ => {
            return Err(StorageError::RootNotLocalFixedVolume {
                role,
                path: path.to_path_buf(),
            });
        }
    };

    let wide = [u16::from(letter), b':' as u16, b'\\' as u16, 0];
    let root = PathBuf::from(format!("{}:\\", char::from(letter)));
    Ok((root, wide))
}

#[cfg(not(windows))]
fn ensure_local_fixed_volume(_role: ManagedRootRole, _path: &Path) -> Result<(), StorageError> {
    Ok(())
}

#[cfg(windows)]
fn observe_volume(role: ManagedRootRole, path: &Path) -> Result<VolumeObservation, StorageError> {
    use std::ptr;

    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationW;

    let (volume_root, wide_root) = fixed_drive_root(role, path)?;
    let mut serial_number = 0_u32;
    let ok = unsafe {
        GetVolumeInformationW(
            wide_root.as_ptr(),
            ptr::null_mut(),
            0,
            &mut serial_number,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            0,
        )
    };
    if ok == 0 {
        return Err(StorageError::RootIo {
            role,
            path: volume_root,
            source: io::Error::last_os_error(),
        });
    }

    let free_bytes = fs2::available_space(path).map_err(|source| StorageError::RootIo {
        role,
        path: path.to_path_buf(),
        source,
    })?;
    let total_bytes = fs2::total_space(path).map_err(|source| StorageError::RootIo {
        role,
        path: path.to_path_buf(),
        source,
    })?;

    Ok(VolumeObservation {
        role,
        volume_root,
        serial_number,
        free_bytes,
        total_bytes,
    })
}

#[cfg(not(windows))]
fn observe_volume(_role: ManagedRootRole, _path: &Path) -> Result<VolumeObservation, StorageError> {
    Err(StorageError::VolumeObservationUnsupported)
}

#[cfg(windows)]
fn open_snapshot_file(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ};

    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(path)
}

#[cfg(not(windows))]
fn open_snapshot_file(path: &Path) -> io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

#[cfg(windows)]
fn file_evidence(file: &File, relative_path: &str) -> Result<FileEvidence, PortError> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY,
        FILE_ATTRIBUTE_REPARSE_POINT,
    };

    let mut info = BY_HANDLE_FILE_INFORMATION::default();
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    if ok == 0 {
        return Err(PortError::new(
            "STORAGE_EVIDENCE_UNAVAILABLE",
            io::Error::last_os_error().to_string(),
        ));
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(PortError::new(
            "STORAGE_REPARSE_POINT",
            format!("Incoming entry is a reparse point: {relative_path}"),
        ));
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0 {
        return Err(PortError::new(
            "STORAGE_NOT_FILE",
            format!("Incoming entry is not a regular file: {relative_path}"),
        ));
    }

    let file_id = (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow);
    let size = (u64::from(info.nFileSizeHigh) << 32) | u64::from(info.nFileSizeLow);
    let modified_marker = (u128::from(info.ftLastWriteTime.dwHighDateTime) << 32)
        | u128::from(info.ftLastWriteTime.dwLowDateTime);

    Ok(FileEvidence {
        identity: FileIdentity {
            volume_id: u64::from(info.dwVolumeSerialNumber),
            file_id,
        },
        size,
        modified_marker,
    })
}

#[cfg(unix)]
fn file_evidence(file: &File, relative_path: &str) -> Result<FileEvidence, PortError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file
        .metadata()
        .map_err(|error| PortError::new("STORAGE_EVIDENCE_UNAVAILABLE", error.to_string()))?;
    if !metadata.is_file() {
        return Err(PortError::new(
            "STORAGE_NOT_FILE",
            format!("Incoming entry is not a regular file: {relative_path}"),
        ));
    }
    let modified_marker = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |duration| duration.as_nanos());

    Ok(FileEvidence {
        identity: FileIdentity {
            volume_id: metadata.dev(),
            file_id: metadata.ino(),
        },
        size: metadata.size(),
        modified_marker,
    })
}

#[cfg(all(not(windows), not(unix)))]
fn file_evidence(_file: &File, _relative_path: &str) -> Result<FileEvidence, PortError> {
    Err(PortError::new(
        "STORAGE_EVIDENCE_UNAVAILABLE",
        "file identity is unavailable on this platform",
    ))
}

fn map_storage_port_error(error: StorageError) -> PortError {
    let code = match error {
        StorageError::RootReparsePoint { .. } => "STORAGE_REPARSE_POINT",
        StorageError::RootOverlap { .. }
        | StorageError::RootNotAbsolute { .. }
        | StorageError::RootContainsTraversal { .. }
        | StorageError::RootNotDirectory { .. }
        | StorageError::RootNotLocalFixedVolume { .. } => "STORAGE_ROOT_INVALID",
        StorageError::VolumeObservationUnsupported => "STORAGE_UNSUPPORTED",
        StorageError::RelativePathEmpty
        | StorageError::RelativePathAbsolute(_)
        | StorageError::InvalidRelativeComponent(_)
        | StorageError::ReservedDeviceName(_) => "STORAGE_PATH_INVALID",
        StorageError::RootIo { .. } => "STORAGE_IO",
    };
    PortError::new(code, error.to_string())
}

#[cfg(windows)]
fn paths_equal(first: &Path, second: &Path) -> bool {
    windows_path_components(first) == windows_path_components(second)
}

#[cfg(not(windows))]
fn paths_equal(first: &Path, second: &Path) -> bool {
    first == second
}

#[cfg(windows)]
fn roots_overlap(first: &Path, second: &Path) -> bool {
    let first = windows_path_components(first);
    let second = windows_path_components(second);
    first == second || first.starts_with(&second) || second.starts_with(&first)
}

#[cfg(windows)]
fn windows_path_components(path: &Path) -> Vec<String> {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy().to_uppercase())
        .collect()
}

#[cfg(not(windows))]
fn roots_overlap(first: &Path, second: &Path) -> bool {
    first == second || first.starts_with(second) || second.starts_with(first)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEMP_ROOT: AtomicU64 = AtomicU64::new(1);

    fn temp_root(label: &str) -> PathBuf {
        let sequence = NEXT_TEMP_ROOT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "qbctl-storage-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("temp root");
        path
    }

    #[test]
    fn managed_relative_path_normalizes_mixed_separators() {
        let path = ManagedRelativePath::parse(r"folder\child/file.txt").expect("valid path");
        assert_eq!(
            path.as_path(),
            PathBuf::from("folder").join("child").join("file.txt")
        );
    }

    #[test]
    fn managed_relative_path_rejects_windows_escape_and_device_names() {
        for invalid in [
            "",
            "/absolute",
            r"\absolute",
            r"C:\absolute",
            "C:drive-relative",
            r"..\file",
            r"folder\..\file",
            r".\file",
            r"folder\",
            "folder//file",
            "name:stream",
            "CON",
            "con.txt",
            "LPT9.log",
            "COM¹",
            "file.",
            "file ",
            "bad?.txt",
            "bad*.txt",
        ] {
            assert!(
                ManagedRelativePath::parse(invalid).is_err(),
                "accepted unsafe path: {invalid:?}"
            );
        }
    }

    #[test]
    fn managed_root_layout_rejects_overlap() {
        let temp = temp_root("overlap");
        let incoming = temp.join("incoming");
        let archive = temp.join("archive");
        let working = incoming.join("working");
        let completed = temp.join("completed");
        let runtime = temp.join("runtime");
        for path in [&incoming, &archive, &working, &completed, &runtime] {
            fs::create_dir_all(path).expect("create root");
        }

        let error = ManagedRootLayout {
            incoming,
            archive,
            working,
            completed,
            runtime,
        }
        .validate()
        .expect_err("overlap must fail");

        assert!(matches!(error, StorageError::RootOverlap { .. }));
        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[test]
    fn managed_root_layout_accepts_distinct_existing_directories() {
        let temp = temp_root("valid");
        let incoming = temp.join("incoming");
        let archive = temp.join("archive");
        let working = temp.join("working");
        let completed = temp.join("completed");
        let runtime = temp.join("runtime");
        for path in [&incoming, &archive, &working, &completed, &runtime] {
            fs::create_dir_all(path).expect("create root");
        }

        let validated = ManagedRootLayout {
            incoming,
            archive,
            working,
            completed,
            runtime,
        }
        .validate()
        .expect("valid roots");

        assert!(validated.incoming.is_absolute());
        assert!(validated.archive.is_absolute());
        assert!(validated.working.is_absolute());
        assert!(validated.completed.is_absolute());
        assert!(validated.runtime.is_absolute());
        fs::remove_dir_all(temp).expect("cleanup");
    }

    fn valid_roots(temp: &Path) -> ValidatedManagedRootLayout {
        let incoming = temp.join("incoming");
        let archive = temp.join("archive");
        let working = temp.join("working");
        let completed = temp.join("completed");
        let runtime = temp.join("runtime");
        for path in [&incoming, &archive, &working, &completed, &runtime] {
            fs::create_dir_all(path).expect("create root");
        }
        ManagedRootLayout {
            incoming,
            archive,
            working,
            completed,
            runtime,
        }
        .validate()
        .expect("valid roots")
    }

    #[test]
    fn managed_storage_lists_only_top_level_torrents_in_deterministic_order() {
        let temp = temp_root("scan");
        let roots = valid_roots(&temp);
        fs::write(roots.incoming.join("b.TORRENT"), b"b").expect("b");
        fs::write(roots.incoming.join("a.torrent"), b"a").expect("a");
        fs::write(roots.incoming.join("note.txt"), b"x").expect("note");
        fs::create_dir_all(roots.incoming.join("nested")).expect("nested");
        fs::write(roots.incoming.join("nested").join("c.torrent"), b"c").expect("c");

        let storage = ManagedStorage::new(roots);
        assert_eq!(
            storage.list_incoming().expect("list"),
            vec!["a.torrent".to_string(), "b.TORRENT".to_string()]
        );

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[test]
    fn managed_storage_reads_stable_file_evidence_and_bytes() {
        let temp = temp_root("snapshot");
        let roots = valid_roots(&temp);
        fs::write(roots.incoming.join("sample.torrent"), b"metainfo").expect("fixture");

        let storage = ManagedStorage::new(roots);
        let snapshot = storage
            .read_incoming("sample.torrent", 1024)
            .expect("snapshot");

        assert_eq!(snapshot.relative_path, "sample.torrent");
        assert_eq!(snapshot.bytes, b"metainfo");
        assert_eq!(snapshot.evidence.size, 8);
        assert_ne!(snapshot.evidence.identity.file_id, 0);

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[test]
    fn managed_storage_rejects_nested_or_oversized_incoming_reads() {
        let temp = temp_root("reject-read");
        let roots = valid_roots(&temp);
        fs::write(roots.incoming.join("large.torrent"), b"12345").expect("fixture");

        let storage = ManagedStorage::new(roots);
        let nested = storage
            .read_incoming(r"nested\file.torrent", 1024)
            .expect_err("nested must fail");
        assert_eq!(nested.code, "STORAGE_PATH_INVALID");

        let large = storage
            .read_incoming("large.torrent", 4)
            .expect_err("oversized must fail");
        assert_eq!(large.code, "STORAGE_SOURCE_TOO_LARGE");

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn managed_storage_rejects_incoming_symlink() {
        use std::os::unix::fs::symlink;

        let temp = temp_root("source-symlink");
        let roots = valid_roots(&temp);
        let outside = temp.join("outside.torrent");
        fs::write(&outside, b"secret").expect("outside");
        symlink(&outside, roots.incoming.join("link.torrent")).expect("symlink");

        let storage = ManagedStorage::new(roots);
        let error = storage
            .read_incoming("link.torrent", 1024)
            .expect_err("symlink must fail");
        assert_eq!(error.code, "STORAGE_REPARSE_POINT");

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn validated_root_exposes_volume_identity_and_capacity() {
        let temp = temp_root("volume");
        let incoming = temp.join("incoming");
        let archive = temp.join("archive");
        let working = temp.join("working");
        let completed = temp.join("completed");
        let runtime = temp.join("runtime");
        for path in [&incoming, &archive, &working, &completed, &runtime] {
            fs::create_dir_all(path).expect("create root");
        }

        let roots = ManagedRootLayout {
            incoming,
            archive,
            working,
            completed,
            runtime,
        }
        .validate()
        .expect("valid roots");

        let volume = roots
            .observe_volume(ManagedRootRole::Working)
            .expect("volume observation");
        assert_eq!(volume.role, ManagedRootRole::Working);
        assert!(volume.volume_root.is_absolute());
        assert!(volume.total_bytes > 0);
        assert!(volume.free_bytes <= volume.total_bytes);

        let storage = ManagedStorage::new(roots);
        let status = storage
            .volume_status(ManagedRoot::Working)
            .expect("application volume status");
        assert_eq!(status.root, ManagedRoot::Working);
        assert_eq!(status.volume_id, u64::from(volume.serial_number));
        assert_eq!(status.free_bytes, volume.free_bytes);
        assert_eq!(status.total_bytes, volume.total_bytes);

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn windows_root_overlap_is_case_insensitive_and_component_aware() {
        assert!(roots_overlap(
            Path::new(r"C:\Data"),
            Path::new(r"c:\data\child")
        ));
        assert!(!roots_overlap(
            Path::new(r"C:\Data"),
            Path::new(r"C:\Database")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn managed_root_layout_rejects_symlink_component() {
        use std::os::unix::fs::symlink;

        let temp = temp_root("symlink");
        let real = temp.join("real");
        let link = temp.join("link");
        fs::create_dir_all(&real).expect("real");
        symlink(&real, &link).expect("symlink");

        let mk = |name: &str| {
            let path = temp.join(name);
            fs::create_dir_all(&path).expect("root");
            path
        };

        let error = ManagedRootLayout {
            incoming: link,
            archive: mk("archive"),
            working: mk("working"),
            completed: mk("completed"),
            runtime: mk("runtime"),
        }
        .validate()
        .expect_err("symlink must fail");

        assert!(matches!(error, StorageError::RootReparsePoint { .. }));
        fs::remove_dir_all(temp).expect("cleanup");
    }
}
