use std::{
    fs, io,
    path::{Component, Path, PathBuf},
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
