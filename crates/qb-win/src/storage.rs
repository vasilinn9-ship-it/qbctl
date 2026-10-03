use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use qb_application::{
    storage::{
        FileEvidence, FileIdentity, IncomingDeleteOutcome, IncomingFileSnapshot,
        ManagedDeleteOutcome, ManagedRoot, SameVolumeMoveOutcome, Storage, StorageVolumeStatus,
        VerifiedCopyOutcome,
    },
    PortError,
};
use sha2::{Digest, Sha256};
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
        let role = managed_root_role(root);
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

    fn root_path(&self, root: ManagedRoot) -> Result<String, PortError> {
        let role = managed_root_role(root);
        self.revalidate_root(role)?;
        self.roots
            .path(role)
            .to_str()
            .map(str::to_owned)
            .ok_or_else(|| {
                PortError::new(
                    "STORAGE_PATH_INVALID",
                    format!("{role:?} root cannot be represented as Unicode"),
                )
            })
    }

    fn matches_root_path(&self, root: ManagedRoot, observed: &str) -> Result<bool, PortError> {
        let role = managed_root_role(root);
        self.revalidate_root(role)?;
        let observed = Path::new(observed);
        if !observed.is_absolute() {
            return Ok(false);
        }
        Ok(paths_equal(self.roots.path(role), observed))
    }

    fn observe_file(
        &self,
        root: ManagedRoot,
        relative_path: &str,
    ) -> Result<Option<FileEvidence>, PortError> {
        observe_managed_file(&self.roots, managed_root_role(root), relative_path)
    }

    fn move_same_volume_no_replace(
        &self,
        source_root: ManagedRoot,
        source_relative: &str,
        destination_root: ManagedRoot,
        destination_relative: &str,
        expected_source: &FileEvidence,
    ) -> Result<SameVolumeMoveOutcome, PortError> {
        move_same_volume_no_replace(
            &self.roots,
            managed_root_role(source_root),
            source_relative,
            managed_root_role(destination_root),
            destination_relative,
            expected_source,
        )
    }

    fn copy_to_temp_verified(
        &self,
        source_root: ManagedRoot,
        source_relative: &str,
        destination_root: ManagedRoot,
        temp_relative: &str,
        expected_source: &FileEvidence,
    ) -> Result<VerifiedCopyOutcome, PortError> {
        copy_to_temp_verified(
            &self.roots,
            managed_root_role(source_root),
            source_relative,
            managed_root_role(destination_root),
            temp_relative,
            expected_source,
        )
    }

    fn delete_managed_exact(
        &self,
        root: ManagedRoot,
        relative_path: &str,
        expected_evidence: &FileEvidence,
        expected_sha256: &[u8; 32],
    ) -> Result<ManagedDeleteOutcome, PortError> {
        delete_managed_exact(
            &self.roots,
            managed_root_role(root),
            relative_path,
            expected_evidence,
            expected_sha256,
        )
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

    fn delete_incoming_exact(
        &self,
        relative_path: &str,
        expected_evidence: &FileEvidence,
        expected_bytes: &[u8],
        max_bytes: usize,
    ) -> Result<IncomingDeleteOutcome, PortError> {
        delete_incoming_exact(
            &self.roots,
            relative_path,
            expected_evidence,
            expected_bytes,
            max_bytes,
        )
    }
}

fn observe_managed_file(
    roots: &ValidatedManagedRootLayout,
    role: ManagedRootRole,
    relative_path: &str,
) -> Result<Option<FileEvidence>, PortError> {
    let refreshed = validate_root(role, roots.path(role)).map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed, roots.path(role)) {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            format!("{role:?} root identity changed before file observation"),
        ));
    }

    let managed = ManagedRelativePath::parse(relative_path).map_err(map_storage_port_error)?;
    let mut current = roots.path(role).to_path_buf();
    let components = managed.as_path().components().collect::<Vec<_>>();

    for (index, component) in components.iter().enumerate() {
        current.push(component.as_os_str());
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
        };
        if is_reparse_metadata(&metadata) {
            return Err(PortError::new(
                "STORAGE_REPARSE_POINT",
                format!("managed {role:?} path traverses a reparse point: {relative_path}"),
            ));
        }
        if index + 1 < components.len() && !metadata.is_dir() {
            return Err(PortError::new(
                "STORAGE_PATH_INVALID",
                format!("managed {role:?} path has a non-directory parent: {relative_path}"),
            ));
        }
    }

    let file = match open_snapshot_file(&current) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };
    file_evidence(&file, relative_path).map(Some)
}

#[cfg(windows)]
fn copy_to_temp_verified(
    roots: &ValidatedManagedRootLayout,
    source_role: ManagedRootRole,
    source_relative: &str,
    destination_role: ManagedRootRole,
    temp_relative: &str,
    expected_source: &FileEvidence,
) -> Result<VerifiedCopyOutcome, PortError> {
    let refreshed_source =
        validate_root(source_role, roots.path(source_role)).map_err(map_storage_port_error)?;
    let refreshed_destination = validate_root(destination_role, roots.path(destination_role))
        .map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed_source, roots.path(source_role))
        || !paths_equal(&refreshed_destination, roots.path(destination_role))
    {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            "managed root identity changed before verified copy",
        ));
    }

    let source_volume = roots
        .observe_volume(source_role)
        .map_err(map_storage_port_error)?;
    let destination_volume = roots
        .observe_volume(destination_role)
        .map_err(map_storage_port_error)?;
    if source_volume.serial_number == destination_volume.serial_number {
        return Err(PortError::new(
            "STORAGE_VOLUME_MISMATCH",
            "verified cross-volume copy requires different source and destination volumes",
        ));
    }

    let source_managed =
        ManagedRelativePath::parse(source_relative).map_err(map_storage_port_error)?;
    let temp_managed = ManagedRelativePath::parse(temp_relative).map_err(map_storage_port_error)?;
    let source_path = roots.path(source_role).join(source_managed.as_path());
    let temp_path = roots.path(destination_role).join(temp_managed.as_path());

    match observe_managed_file(roots, source_role, source_relative)? {
        None => return Ok(VerifiedCopyOutcome::SourceMissing),
        Some(observed) if &observed != expected_source => {
            return Ok(VerifiedCopyOutcome::SourceChanged { observed });
        }
        Some(_) => {}
    }

    if observe_managed_file(roots, destination_role, temp_relative)?.is_some() {
        return verify_existing_temp(
            roots,
            source_role,
            source_relative,
            destination_role,
            temp_relative,
            expected_source,
        );
    }

    ensure_managed_parent_directories(roots, destination_role, temp_managed.as_path())?;

    let mut source = match open_snapshot_file(&source_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(VerifiedCopyOutcome::SourceMissing);
        }
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };
    let before = file_evidence(&source, source_relative)?;
    if &before != expected_source {
        return Ok(VerifiedCopyOutcome::SourceChanged { observed: before });
    }

    let mut temp = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return verify_existing_temp(
                roots,
                source_role,
                source_relative,
                destination_role,
                temp_relative,
                expected_source,
            );
        }
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };

    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        temp.write_all(&buffer[..read])
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
    }
    temp.flush()
        .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
    temp.sync_all()
        .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;

    let after = file_evidence(&source, source_relative)?;
    if &after != expected_source {
        return Err(PortError::new(
            "HANDOFF_SOURCE_CHANGED",
            "source evidence changed while cross-volume copy was in progress",
        ));
    }
    let source_sha256: [u8; 32] = digest.finalize().into();
    drop(temp);
    drop(source);

    let mut temp = open_snapshot_file(&temp_path)
        .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
    let temp_evidence = file_evidence(&temp, temp_relative)?;
    let temp_sha256 = sha256_reader(&mut temp)?;
    if temp_evidence.size != expected_source.size || temp_sha256 != source_sha256 {
        return Err(PortError::new(
            "STORAGE_COPY_VERIFY_FAILED",
            "operation temp does not cryptographically match the source after copy and flush",
        ));
    }

    Ok(VerifiedCopyOutcome::Verified {
        temp: temp_evidence,
        sha256: source_sha256,
        created: true,
    })
}

#[cfg(windows)]
fn verify_existing_temp(
    roots: &ValidatedManagedRootLayout,
    source_role: ManagedRootRole,
    source_relative: &str,
    destination_role: ManagedRootRole,
    temp_relative: &str,
    expected_source: &FileEvidence,
) -> Result<VerifiedCopyOutcome, PortError> {
    let source_path = roots.path(source_role).join(
        ManagedRelativePath::parse(source_relative)
            .map_err(map_storage_port_error)?
            .as_path(),
    );
    let temp_path = roots.path(destination_role).join(
        ManagedRelativePath::parse(temp_relative)
            .map_err(map_storage_port_error)?
            .as_path(),
    );

    let mut source = match open_snapshot_file(&source_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(VerifiedCopyOutcome::SourceMissing);
        }
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };
    let before = file_evidence(&source, source_relative)?;
    if &before != expected_source {
        return Ok(VerifiedCopyOutcome::SourceChanged { observed: before });
    }
    let source_sha256 = sha256_reader(&mut source)?;
    let after = file_evidence(&source, source_relative)?;
    if &after != expected_source {
        return Ok(VerifiedCopyOutcome::SourceChanged { observed: after });
    }

    let mut temp = open_snapshot_file(&temp_path)
        .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
    let temp_evidence = file_evidence(&temp, temp_relative)?;
    let temp_sha256 = sha256_reader(&mut temp)?;
    if temp_evidence.size == expected_source.size && temp_sha256 == source_sha256 {
        Ok(VerifiedCopyOutcome::Verified {
            temp: temp_evidence,
            sha256: source_sha256,
            created: false,
        })
    } else {
        Ok(VerifiedCopyOutcome::TempConflict {
            observed: temp_evidence,
            sha256: temp_sha256,
        })
    }
}

#[cfg(not(windows))]
fn copy_to_temp_verified(
    _roots: &ValidatedManagedRootLayout,
    _source_role: ManagedRootRole,
    _source_relative: &str,
    _destination_role: ManagedRootRole,
    _temp_relative: &str,
    _expected_source: &FileEvidence,
) -> Result<VerifiedCopyOutcome, PortError> {
    Err(PortError::new(
        "STORAGE_COPY_UNSUPPORTED",
        "verified cross-volume copy requires the Windows storage adapter",
    ))
}

#[cfg(windows)]
fn delete_managed_exact(
    roots: &ValidatedManagedRootLayout,
    role: ManagedRootRole,
    relative_path: &str,
    expected_evidence: &FileEvidence,
    expected_sha256: &[u8; 32],
) -> Result<ManagedDeleteOutcome, PortError> {
    use std::{mem::size_of, os::windows::fs::OpenOptionsExt, os::windows::io::AsRawHandle};

    use windows_sys::Win32::Storage::FileSystem::{
        FileDispositionInfo, SetFileInformationByHandle, FILE_DISPOSITION_INFO,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
    };

    let refreshed = validate_root(role, roots.path(role)).map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed, roots.path(role)) {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            format!("{role:?} root identity changed before exact delete"),
        ));
    }

    let managed = ManagedRelativePath::parse(relative_path).map_err(map_storage_port_error)?;
    let path = roots.path(role).join(managed.as_path());

    match observe_managed_file(roots, role, relative_path)? {
        None => return Ok(ManagedDeleteOutcome::Missing),
        Some(observed) if &observed != expected_evidence => {
            return Ok(ManagedDeleteOutcome::Changed);
        }
        Some(_) => {}
    }

    const GENERIC_READ_ACCESS: u32 = 0x8000_0000;
    const DELETE_ACCESS: u32 = 0x0001_0000;
    let mut file = match OpenOptions::new()
        .access_mode(GENERIC_READ_ACCESS | DELETE_ACCESS)
        .share_mode(FILE_SHARE_READ)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(ManagedDeleteOutcome::Missing);
        }
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };

    let before = file_evidence(&file, relative_path)?;
    if &before != expected_evidence {
        return Ok(ManagedDeleteOutcome::Changed);
    }
    let digest = sha256_reader(&mut file)?;
    let after = file_evidence(&file, relative_path)?;
    if &after != expected_evidence || &digest != expected_sha256 {
        return Ok(ManagedDeleteOutcome::Changed);
    }

    let disposition = FILE_DISPOSITION_INFO { DeleteFile: 1 };
    let success = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as _,
            FileDispositionInfo,
            std::ptr::from_ref(&disposition).cast(),
            size_of::<FILE_DISPOSITION_INFO>() as u32,
        )
    };
    if success == 0 {
        return Err(PortError::new(
            "STORAGE_DELETE_UNCERTAIN",
            io::Error::last_os_error().to_string(),
        ));
    }
    drop(file);

    if observe_managed_file(roots, role, relative_path)?.is_some() {
        return Err(PortError::new(
            "STORAGE_DELETE_POSTCONDITION",
            "managed source still exists after exact delete",
        ));
    }
    Ok(ManagedDeleteOutcome::Deleted)
}

#[cfg(not(windows))]
fn delete_managed_exact(
    _roots: &ValidatedManagedRootLayout,
    _role: ManagedRootRole,
    _relative_path: &str,
    _expected_evidence: &FileEvidence,
    _expected_sha256: &[u8; 32],
) -> Result<ManagedDeleteOutcome, PortError> {
    Err(PortError::new(
        "STORAGE_DELETE_UNSUPPORTED",
        "exact managed deletion requires the Windows storage adapter",
    ))
}

fn sha256_reader(reader: &mut File) -> Result<[u8; 32], PortError> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.finalize().into())
}

#[cfg(windows)]
fn move_same_volume_no_replace(
    roots: &ValidatedManagedRootLayout,
    source_role: ManagedRootRole,
    source_relative: &str,
    destination_role: ManagedRootRole,
    destination_relative: &str,
    expected_source: &FileEvidence,
) -> Result<SameVolumeMoveOutcome, PortError> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::{
        Foundation::{ERROR_ALREADY_EXISTS, ERROR_FILE_EXISTS},
        Storage::FileSystem::MoveFileW,
    };

    let refreshed_source =
        validate_root(source_role, roots.path(source_role)).map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed_source, roots.path(source_role)) {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            format!("{source_role:?} root identity changed before managed move"),
        ));
    }
    let refreshed_destination = validate_root(destination_role, roots.path(destination_role))
        .map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed_destination, roots.path(destination_role)) {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            format!("{destination_role:?} root identity changed before managed move"),
        ));
    }

    let source_volume = roots
        .observe_volume(source_role)
        .map_err(map_storage_port_error)?;
    let destination_volume = roots
        .observe_volume(destination_role)
        .map_err(map_storage_port_error)?;
    if source_volume.serial_number != destination_volume.serial_number {
        return Err(PortError::new(
            "STORAGE_VOLUME_MISMATCH",
            "same-volume move requires source and destination on the same volume",
        ));
    }

    let source_managed =
        ManagedRelativePath::parse(source_relative).map_err(map_storage_port_error)?;
    let destination_managed =
        ManagedRelativePath::parse(destination_relative).map_err(map_storage_port_error)?;
    let source_path = roots.path(source_role).join(source_managed.as_path());
    let destination_path = roots
        .path(destination_role)
        .join(destination_managed.as_path());

    match observe_managed_file(roots, source_role, source_relative)? {
        None => return Ok(SameVolumeMoveOutcome::SourceMissing),
        Some(observed) if &observed != expected_source => {
            return Ok(SameVolumeMoveOutcome::SourceChanged { observed });
        }
        Some(_) => {}
    }
    if let Some(observed) = observe_managed_file(roots, destination_role, destination_relative)? {
        return Ok(SameVolumeMoveOutcome::DestinationExists { observed });
    }

    ensure_managed_parent_directories(roots, destination_role, destination_managed.as_path())?;

    match observe_managed_file(roots, source_role, source_relative)? {
        None => return Ok(SameVolumeMoveOutcome::SourceMissing),
        Some(observed) if &observed != expected_source => {
            return Ok(SameVolumeMoveOutcome::SourceChanged { observed });
        }
        Some(_) => {}
    }
    if let Some(observed) = observe_managed_file(roots, destination_role, destination_relative)? {
        return Ok(SameVolumeMoveOutcome::DestinationExists { observed });
    }

    let source_wide = source_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let destination_wide = destination_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();

    let moved = unsafe { MoveFileW(source_wide.as_ptr(), destination_wide.as_ptr()) };
    if moved == 0 {
        let error = io::Error::last_os_error();
        if error
            .raw_os_error()
            .map(|value| value as u32)
            .is_some_and(|value| value == ERROR_ALREADY_EXISTS || value == ERROR_FILE_EXISTS)
        {
            if let Some(observed) =
                observe_managed_file(roots, destination_role, destination_relative)?
            {
                return Ok(SameVolumeMoveOutcome::DestinationExists { observed });
            }
        }
        return Err(PortError::new("STORAGE_MOVE_UNCERTAIN", error.to_string()));
    }

    if observe_managed_file(roots, source_role, source_relative)?.is_some() {
        return Err(PortError::new(
            "STORAGE_MOVE_POSTCONDITION",
            "source still exists after same-volume move",
        ));
    }
    let destination = observe_managed_file(roots, destination_role, destination_relative)?
        .ok_or_else(|| {
            PortError::new(
                "STORAGE_MOVE_POSTCONDITION",
                "destination is missing after same-volume move",
            )
        })?;
    if destination != *expected_source {
        return Err(PortError::new(
            "STORAGE_MOVE_POSTCONDITION",
            "destination evidence does not match the expected source after same-volume move",
        ));
    }

    Ok(SameVolumeMoveOutcome::Moved { destination })
}

#[cfg(not(windows))]
fn move_same_volume_no_replace(
    _roots: &ValidatedManagedRootLayout,
    _source_role: ManagedRootRole,
    _source_relative: &str,
    _destination_role: ManagedRootRole,
    _destination_relative: &str,
    _expected_source: &FileEvidence,
) -> Result<SameVolumeMoveOutcome, PortError> {
    Err(PortError::new(
        "STORAGE_MOVE_UNSUPPORTED",
        "safe same-volume move requires the Windows storage adapter",
    ))
}

fn ensure_managed_parent_directories(
    roots: &ValidatedManagedRootLayout,
    role: ManagedRootRole,
    relative_path: &Path,
) -> Result<(), PortError> {
    let parent = relative_path.parent().ok_or_else(|| {
        PortError::new(
            "STORAGE_PATH_INVALID",
            "managed move target has no relative parent",
        )
    })?;
    let mut current = roots.path(role).to_path_buf();
    for component in parent.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if is_reparse_metadata(&metadata) {
                    return Err(PortError::new(
                        "STORAGE_REPARSE_POINT",
                        format!(
                            "managed destination parent is a reparse point: {}",
                            current.display()
                        ),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(PortError::new(
                        "STORAGE_PATH_INVALID",
                        format!(
                            "managed destination parent is not a directory: {}",
                            current.display()
                        ),
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(PortError::new("STORAGE_IO", error.to_string()));
                    }
                }
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
                if is_reparse_metadata(&metadata) || !metadata.is_dir() {
                    return Err(PortError::new(
                        "STORAGE_PATH_INVALID",
                        format!(
                            "managed destination parent became unsafe: {}",
                            current.display()
                        ),
                    ));
                }
            }
            Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
        }
    }
    Ok(())
}

fn validate_incoming_delete_path(
    roots: &ValidatedManagedRootLayout,
    relative_path: &str,
) -> Result<PathBuf, PortError> {
    let managed = ManagedRelativePath::parse(relative_path).map_err(|_| {
        PortError::new(
            "STORAGE_PATH_INVALID",
            format!("unsafe Incoming path: {relative_path}"),
        )
    })?;
    if managed.as_path().components().count() != 1 || !is_torrent_name(relative_path) {
        return Err(PortError::new(
            "STORAGE_PATH_INVALID",
            "Incoming cleanup is limited to top-level .torrent files",
        ));
    }
    Ok(roots.incoming.join(managed.as_path()))
}

#[cfg(windows)]
fn delete_incoming_exact(
    roots: &ValidatedManagedRootLayout,
    relative_path: &str,
    expected_evidence: &FileEvidence,
    expected_bytes: &[u8],
    max_bytes: usize,
) -> Result<IncomingDeleteOutcome, PortError> {
    use std::{mem::size_of, os::windows::io::AsRawHandle};

    use windows_sys::Win32::Storage::FileSystem::{
        FileDispositionInfo, SetFileInformationByHandle, FILE_DISPOSITION_INFO,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
    };

    if max_bytes == 0 {
        return Err(PortError::new(
            "STORAGE_LIMIT_INVALID",
            "Incoming cleanup read limit must be greater than zero",
        ));
    }

    let refreshed = validate_root(ManagedRootRole::Incoming, &roots.incoming)
        .map_err(map_storage_port_error)?;
    if !paths_equal(&refreshed, &roots.incoming) {
        return Err(PortError::new(
            "STORAGE_ROOT_CHANGED",
            "Incoming root identity changed before cleanup",
        ));
    }

    let path = validate_incoming_delete_path(roots, relative_path)?;
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(IncomingDeleteOutcome::Missing);
        }
        Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
    };
    if is_reparse_metadata(&metadata) {
        return Ok(IncomingDeleteOutcome::Changed);
    }

    const GENERIC_READ_ACCESS: u32 = 0x8000_0000;
    const DELETE_ACCESS: u32 = 0x0001_0000;

    let mut file = {
        use std::os::windows::fs::OpenOptionsExt;

        match OpenOptions::new()
            .access_mode(GENERIC_READ_ACCESS | DELETE_ACCESS)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(IncomingDeleteOutcome::Missing);
            }
            Err(error) => return Err(PortError::new("STORAGE_IO", error.to_string())),
        }
    };

    let before = file_evidence(&file, relative_path)?;
    if &before != expected_evidence {
        return Ok(IncomingDeleteOutcome::Changed);
    }

    let read_limit = max_bytes.checked_add(1).ok_or_else(|| {
        PortError::new(
            "STORAGE_LIMIT_INVALID",
            "Incoming cleanup read limit overflow",
        )
    })?;
    let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
    (&mut file)
        .take(u64::try_from(read_limit).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .map_err(|error| PortError::new("STORAGE_IO", error.to_string()))?;
    if bytes.len() > max_bytes {
        return Ok(IncomingDeleteOutcome::Changed);
    }
    if before.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        return Ok(IncomingDeleteOutcome::Changed);
    }
    if bytes != expected_bytes {
        return Ok(IncomingDeleteOutcome::Changed);
    }

    let after = file_evidence(&file, relative_path)?;
    if after != before {
        return Ok(IncomingDeleteOutcome::Changed);
    }

    let disposition = FILE_DISPOSITION_INFO { DeleteFile: true };
    let ok = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as _,
            FileDispositionInfo,
            &disposition as *const FILE_DISPOSITION_INFO as *const core::ffi::c_void,
            u32::try_from(size_of::<FILE_DISPOSITION_INFO>())
                .expect("FILE_DISPOSITION_INFO size fits u32"),
        )
    };
    if ok == 0 {
        return Err(PortError::new(
            "STORAGE_IO",
            io::Error::last_os_error().to_string(),
        ));
    }

    drop(file);
    Ok(IncomingDeleteOutcome::Deleted)
}

#[cfg(not(windows))]
fn delete_incoming_exact(
    _roots: &ValidatedManagedRootLayout,
    _relative_path: &str,
    _expected_evidence: &FileEvidence,
    _expected_bytes: &[u8],
    _max_bytes: usize,
) -> Result<IncomingDeleteOutcome, PortError> {
    Err(PortError::new(
        "STORAGE_DELETE_UNSUPPORTED",
        "race-safe Incoming deletion requires the Windows storage adapter",
    ))
}

fn managed_root_role(root: ManagedRoot) -> ManagedRootRole {
    match root {
        ManagedRoot::Incoming => ManagedRootRole::Incoming,
        ManagedRoot::Archive => ManagedRootRole::Archive,
        ManagedRoot::Working => ManagedRootRole::Working,
        ManagedRoot::Completed => ManagedRootRole::Completed,
        ManagedRoot::Runtime => ManagedRootRole::Runtime,
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

    #[cfg(windows)]
    #[test]
    fn managed_storage_moves_same_volume_without_replace_and_preserves_identity() {
        let temp = temp_root("move-same-volume");
        let roots = valid_roots(&temp);
        let source = roots.working.join("dir").join("payload.bin");
        fs::create_dir_all(source.parent().expect("source parent")).expect("source parent");
        fs::write(&source, b"payload").expect("fixture");

        let storage = ManagedStorage::new(roots.clone());
        let evidence = storage
            .observe_file(ManagedRoot::Working, "dir/payload.bin")
            .expect("observe source")
            .expect("source evidence");

        let outcome = storage
            .move_same_volume_no_replace(
                ManagedRoot::Working,
                "dir/payload.bin",
                ManagedRoot::Completed,
                "dir/payload.bin",
                &evidence,
            )
            .expect("move");
        assert_eq!(
            outcome,
            SameVolumeMoveOutcome::Moved {
                destination: evidence.clone()
            }
        );
        assert!(!source.exists());
        let destination = roots.completed.join("dir").join("payload.bin");
        assert_eq!(
            fs::read(destination).expect("destination bytes"),
            b"payload"
        );

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn managed_storage_same_volume_move_refuses_existing_destination() {
        let temp = temp_root("move-destination-conflict");
        let roots = valid_roots(&temp);
        let source = roots.working.join("dir").join("payload.bin");
        let destination = roots.completed.join("dir").join("payload.bin");
        fs::create_dir_all(source.parent().expect("source parent")).expect("source parent");
        fs::create_dir_all(destination.parent().expect("destination parent"))
            .expect("destination parent");
        fs::write(&source, b"source").expect("source fixture");
        fs::write(&destination, b"destination").expect("destination fixture");

        let storage = ManagedStorage::new(roots.clone());
        let evidence = storage
            .observe_file(ManagedRoot::Working, "dir/payload.bin")
            .expect("observe source")
            .expect("source evidence");

        let outcome = storage
            .move_same_volume_no_replace(
                ManagedRoot::Working,
                "dir/payload.bin",
                ManagedRoot::Completed,
                "dir/payload.bin",
                &evidence,
            )
            .expect("conflict outcome");
        assert!(matches!(
            outcome,
            SameVolumeMoveOutcome::DestinationExists { .. }
        ));
        assert_eq!(fs::read(source).expect("source retained"), b"source");
        assert_eq!(
            fs::read(destination).expect("destination retained"),
            b"destination"
        );

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn managed_storage_deletes_only_exact_incoming_snapshot() {
        let temp = temp_root("delete-exact");
        let roots = valid_roots(&temp);
        fs::write(roots.incoming.join("duplicate.torrent"), b"same-metainfo").expect("fixture");

        let storage = ManagedStorage::new(roots.clone());
        let snapshot = storage
            .read_incoming("duplicate.torrent", 1024)
            .expect("snapshot");
        assert_eq!(
            storage
                .delete_incoming_exact(
                    "duplicate.torrent",
                    &snapshot.evidence,
                    &snapshot.bytes,
                    1024,
                )
                .expect("delete"),
            IncomingDeleteOutcome::Deleted
        );
        assert!(!roots.incoming.join("duplicate.torrent").exists());
        assert_eq!(
            storage
                .delete_incoming_exact(
                    "duplicate.torrent",
                    &snapshot.evidence,
                    &snapshot.bytes,
                    1024,
                )
                .expect("observe absent"),
            IncomingDeleteOutcome::Missing
        );

        fs::remove_dir_all(temp).expect("cleanup");
    }

    #[cfg(windows)]
    #[test]
    fn managed_storage_refuses_changed_incoming_cleanup_target() {
        let temp = temp_root("delete-changed");
        let roots = valid_roots(&temp);
        let path = roots.incoming.join("duplicate.torrent");
        fs::write(&path, b"original-metainfo").expect("fixture");

        let storage = ManagedStorage::new(roots.clone());
        let snapshot = storage
            .read_incoming("duplicate.torrent", 1024)
            .expect("snapshot");
        fs::write(&path, b"replacement-metainfo").expect("replace");

        assert_eq!(
            storage
                .delete_incoming_exact(
                    "duplicate.torrent",
                    &snapshot.evidence,
                    &snapshot.bytes,
                    1024,
                )
                .expect("changed"),
            IncomingDeleteOutcome::Changed
        );
        assert_eq!(
            fs::read(&path).expect("replacement retained"),
            b"replacement-metainfo"
        );

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
        assert!(status.total_bytes > 0);
        assert!(status.free_bytes <= status.total_bytes);
        assert_eq!(status.total_bytes, volume.total_bytes);
        let working_path = storage
            .root_path(ManagedRoot::Working)
            .expect("validated Working root path");
        assert_eq!(
            Path::new(&working_path),
            storage.roots().path(ManagedRootRole::Working)
        );
        assert!(storage
            .matches_root_path(ManagedRoot::Working, &working_path)
            .expect("matching Working path"));
        #[cfg(windows)]
        assert!(storage
            .matches_root_path(ManagedRoot::Working, &working_path.to_ascii_uppercase())
            .expect("case-insensitive Working path"));
        assert!(!storage
            .matches_root_path(ManagedRoot::Working, "relative")
            .expect("relative path must not match"));

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
