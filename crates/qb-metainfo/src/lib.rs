use std::collections::{BTreeMap, BTreeSet};

use qb_application::{torrent::MetainfoReader, PortError};
use qb_domain::torrent::{ManifestFile, TorrentIdentity, TorrentManifest, TorrentMetainfo};
use sha1::{Digest, Sha1};
use sha2::Sha256;
use thiserror::Error;
use urtorrent_bencode::{Decoder, Limits, Value};

pub const MAX_METAINFO_BYTES: usize = 32 * 1024 * 1024;
const MAX_BENCODE_DEPTH: usize = 64;
const MAX_CONTAINER_ITEMS: usize = 200_000;
const MAX_FILES: usize = 100_000;
const MAX_PATH_COMPONENT_BYTES: usize = 255;
const MAX_LOGICAL_PATH_BYTES: usize = 32 * 1024;
const V2_BLOCK_SIZE: u64 = 16 * 1024;

pub const ADAPTER_NAME: &str = "local BitTorrent metainfo";

#[derive(Clone, Copy, Debug, Default)]
pub struct LocalMetainfoReader;

#[derive(Debug, Error)]
enum MetainfoError {
    #[error("metainfo exceeds {MAX_METAINFO_BYTES} bytes")]
    TooLarge,
    #[error("invalid bencode: {0}")]
    Decode(String),
    #[error("invalid metainfo: {0}")]
    Invalid(String),
    #[error("unsupported metainfo: {0}")]
    Unsupported(String),
}

impl MetainfoReader for LocalMetainfoReader {
    fn parse(&self, bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
        parse_metainfo(bytes)
    }
}

pub fn parse_metainfo(bytes: &[u8]) -> Result<TorrentMetainfo, PortError> {
    parse_inner(bytes).map_err(|error| match error {
        MetainfoError::Unsupported(_) => PortError::new("METAINFO_UNSUPPORTED", error.to_string()),
        _ => PortError::new("METAINFO_INVALID", error.to_string()),
    })
}

fn parse_inner(bytes: &[u8]) -> Result<TorrentMetainfo, MetainfoError> {
    if bytes.len() > MAX_METAINFO_BYTES {
        return Err(MetainfoError::TooLarge);
    }

    let root = Decoder::with_limits(
        bytes,
        Limits {
            max_depth: MAX_BENCODE_DEPTH,
            max_container_len: MAX_CONTAINER_ITEMS,
        },
    )
    .decode_all()
    .map_err(|error| MetainfoError::Decode(error.to_string()))?;

    require_dict(&root, "top-level metainfo")?;
    let info = root
        .get(b"info")
        .ok_or_else(|| invalid("missing info dictionary"))?;
    require_dict(info, "info")?;
    let raw_info = info
        .raw()
        .ok_or_else(|| invalid("info dictionary raw bytes unavailable"))?;

    let meta_version = optional_int(info, b"meta version")?;
    if let Some(version) = meta_version {
        if version != 2 {
            return Err(MetainfoError::Unsupported(format!(
                "meta version {version} is not supported"
            )));
        }
    }

    let has_v2 = meta_version == Some(2);
    let has_v1 = info.get(b"pieces").is_some()
        || info.get(b"files").is_some()
        || info.get(b"length").is_some();

    if !has_v1 && !has_v2 {
        return Err(invalid("info dictionary is neither v1 nor v2"));
    }

    let v1 = if has_v1 { Some(parse_v1(info)?) } else { None };
    let v2 = if has_v2 {
        Some(parse_v2(&root, info)?)
    } else {
        None
    };

    if let (Some(v1), Some(v2)) = (&v1, &v2) {
        validate_hybrid(v1, v2)?;
    }

    let v1_hash = v1.as_ref().map(|_| digest20(raw_info));
    let v2_hash = v2.as_ref().map(|_| digest32(raw_info));
    let identity = TorrentIdentity::new(v1_hash, v2_hash)
        .ok_or_else(|| invalid("no torrent identity could be derived"))?;

    let manifest = match (v1, v2) {
        (_, Some(v2)) => TorrentManifest::new(
            v2.files
                .into_iter()
                .map(|file| ManifestFile {
                    path: join_path(&file.path),
                    size: file.size,
                })
                .collect(),
        ),
        (Some(v1), None) => TorrentManifest::new(
            v1.content_files
                .into_iter()
                .map(|file| ManifestFile {
                    path: join_path(&file.manifest_path),
                    size: file.size,
                })
                .collect(),
        ),
        (None, None) => None,
    }
    .ok_or_else(|| invalid("manifest size overflow"))?;

    Ok(TorrentMetainfo { identity, manifest })
}

struct V1Parsed {
    name: String,
    piece_length: u64,
    multi_file: bool,
    content_files: Vec<V1ContentFile>,
    layout: Vec<V1LayoutEntry>,
}

struct V1ContentFile {
    relative_path: Vec<String>,
    manifest_path: Vec<String>,
    size: u64,
}

struct V1LayoutEntry {
    size: u64,
    padding: bool,
}

fn parse_v1(info: &Value<'_>) -> Result<V1Parsed, MetainfoError> {
    let piece_length = positive_u64(info, b"piece length")?;
    let name = required_utf8(info, b"name.utf-8").or_else(|_| required_utf8(info, b"name"))?;
    validate_component(&name)?;

    let pieces = required_bytes(info, b"pieces")?;
    if pieces.len() % 20 != 0 {
        return Err(invalid("v1 pieces length is not a multiple of 20"));
    }

    let has_length = info.get(b"length").is_some();
    let has_files = info.get(b"files").is_some();
    if has_length == has_files {
        return Err(invalid(
            "v1 info must contain exactly one of length or files",
        ));
    }

    let mut content_files = Vec::new();
    let mut layout = Vec::new();
    let mut layout_total = 0_u64;

    if has_length {
        let size = nonnegative_u64(info, b"length")?;
        layout_total = size;
        content_files.push(V1ContentFile {
            relative_path: vec![name.clone()],
            manifest_path: vec![name.clone()],
            size,
        });
        layout.push(V1LayoutEntry {
            size,
            padding: false,
        });
    } else {
        let files = info
            .get(b"files")
            .and_then(Value::as_list)
            .ok_or_else(|| invalid("v1 files must be a list"))?;
        if files.len() > MAX_FILES {
            return Err(invalid("v1 file count exceeds parser limit"));
        }

        for file in files {
            require_dict(file, "v1 file")?;
            let size = nonnegative_u64(file, b"length")?;
            let path_value = file
                .get(b"path.utf-8")
                .or_else(|| file.get(b"path"))
                .ok_or_else(|| invalid("v1 file path is missing"))?;
            let relative_path = parse_path_list(path_value)?;
            let padding = file
                .get(b"attr")
                .and_then(Value::as_bytes)
                .is_some_and(|attr| attr.contains(&b'p'));

            layout_total = layout_total
                .checked_add(size)
                .ok_or_else(|| invalid("v1 total size overflow"))?;
            layout.push(V1LayoutEntry { size, padding });

            if !padding {
                if content_files.len() >= MAX_FILES {
                    return Err(invalid("v1 content file count exceeds parser limit"));
                }
                let mut manifest_path = Vec::with_capacity(relative_path.len() + 1);
                manifest_path.push(name.clone());
                manifest_path.extend(relative_path.iter().cloned());
                validate_path(&manifest_path)?;
                content_files.push(V1ContentFile {
                    relative_path,
                    manifest_path,
                    size,
                });
            }
        }
    }

    if content_files.is_empty() {
        return Err(invalid("v1 torrent contains no content files"));
    }

    let expected_pieces = if layout_total == 0 {
        0
    } else {
        div_ceil(layout_total, piece_length)
    };
    let actual_pieces = pieces.len() / 20;
    if u64::try_from(actual_pieces).ok() != Some(expected_pieces) {
        return Err(invalid(format!(
            "v1 piece count mismatch: expected {expected_pieces}, got {actual_pieces}"
        )));
    }

    Ok(V1Parsed {
        name,
        piece_length,
        multi_file: has_files,
        content_files,
        layout,
    })
}

#[derive(Clone)]
struct V2File {
    path: Vec<String>,
    size: u64,
    pieces_root: Option<[u8; 32]>,
}

struct V2Parsed {
    piece_length: u64,
    files: Vec<V2File>,
}

fn parse_v2(root: &Value<'_>, info: &Value<'_>) -> Result<V2Parsed, MetainfoError> {
    let piece_length = positive_u64(info, b"piece length")?;
    if piece_length < V2_BLOCK_SIZE || !piece_length.is_power_of_two() {
        return Err(invalid(
            "v2 piece length must be a power of two and at least 16 KiB",
        ));
    }

    let name = required_utf8(info, b"name")?;
    validate_component(&name)?;

    let tree = info
        .get(b"file tree")
        .ok_or_else(|| invalid("v2 file tree is missing"))?;
    require_dict(tree, "v2 file tree")?;

    let mut files = Vec::new();
    let mut path = Vec::new();
    walk_v2_tree(tree, &mut path, &mut files)?;

    if files.is_empty() {
        return Err(invalid("v2 torrent contains no files"));
    }
    if files.len() > MAX_FILES {
        return Err(invalid("v2 file count exceeds parser limit"));
    }

    validate_v2_piece_layers(root, piece_length, &files)?;

    Ok(V2Parsed {
        piece_length,
        files,
    })
}

fn walk_v2_tree(
    node: &Value<'_>,
    path: &mut Vec<String>,
    files: &mut Vec<V2File>,
) -> Result<(), MetainfoError> {
    let entries = require_dict(node, "v2 file tree node")?;

    if let Some(properties) = node.get(b"") {
        if entries.len() != 1 {
            return Err(invalid("v2 file leaf has sibling tree entries"));
        }
        require_dict(properties, "v2 file properties")?;
        let size = nonnegative_u64(properties, b"length")?;
        let pieces_root = match properties.get(b"pieces root") {
            Some(value) => {
                let bytes = value
                    .as_bytes()
                    .ok_or_else(|| invalid("v2 pieces root must be bytes"))?;
                if bytes.len() != 32 {
                    return Err(invalid("v2 pieces root must be 32 bytes"));
                }
                let mut root = [0_u8; 32];
                root.copy_from_slice(bytes);
                Some(root)
            }
            None => None,
        };

        if size > 0 && pieces_root.is_none() {
            return Err(invalid("non-empty v2 file is missing pieces root"));
        }
        if path.is_empty() {
            return Err(invalid("v2 file tree root cannot be a file"));
        }

        validate_path(path)?;
        files.push(V2File {
            path: path.clone(),
            size,
            pieces_root,
        });
        return Ok(());
    }

    for (raw_key, child) in entries {
        if raw_key.is_empty() {
            return Err(invalid("unexpected empty v2 path component"));
        }
        let component = std::str::from_utf8(raw_key)
            .map_err(|_| invalid("v2 path component is not valid UTF-8"))?
            .to_string();
        validate_component(&component)?;
        path.push(component);
        walk_v2_tree(child, path, files)?;
        path.pop();

        if files.len() > MAX_FILES {
            return Err(invalid("v2 file count exceeds parser limit"));
        }
    }

    Ok(())
}

fn validate_v2_piece_layers(
    root: &Value<'_>,
    piece_length: u64,
    files: &[V2File],
) -> Result<(), MetainfoError> {
    let mut expected = BTreeMap::<[u8; 32], u64>::new();
    for file in files {
        if file.size > piece_length {
            let pieces_root = file
                .pieces_root
                .ok_or_else(|| invalid("large v2 file is missing pieces root"))?;
            match expected.insert(pieces_root, file.size) {
                Some(existing) if existing != file.size => {
                    return Err(invalid(
                        "same v2 pieces root is associated with different file sizes",
                    ));
                }
                _ => {}
            }
        }
    }

    let piece_layers = root.get(b"piece layers");
    if expected.is_empty() {
        if let Some(value) = piece_layers {
            let entries = require_dict(value, "piece layers")?;
            if !entries.is_empty() {
                return Err(invalid("unexpected v2 piece layers"));
            }
        }
        return Ok(());
    }

    let layers = piece_layers.ok_or_else(|| invalid("v2 piece layers are missing"))?;
    let entries = require_dict(layers, "piece layers")?;
    let mut seen = BTreeSet::new();

    for (raw_root, value) in entries {
        if raw_root.len() != 32 {
            return Err(invalid("piece layer key must be a 32-byte pieces root"));
        }
        let mut root_hash = [0_u8; 32];
        root_hash.copy_from_slice(raw_root);

        let file_size = expected
            .get(&root_hash)
            .ok_or_else(|| invalid("piece layer does not match any large file"))?;
        let layer = value
            .as_bytes()
            .ok_or_else(|| invalid("piece layer value must be bytes"))?;

        let piece_count = div_ceil(*file_size, piece_length);
        let expected_len = usize::try_from(piece_count)
            .ok()
            .and_then(|count| count.checked_mul(32))
            .ok_or_else(|| invalid("piece layer size overflow"))?;
        if layer.len() != expected_len {
            return Err(invalid(format!(
                "piece layer length mismatch: expected {expected_len}, got {}",
                layer.len()
            )));
        }

        let computed = merkle_root_from_piece_layer(layer, piece_length)?;
        if computed != root_hash {
            return Err(invalid("piece layer does not match pieces root"));
        }

        seen.insert(root_hash);
    }

    if seen.len() != expected.len() {
        return Err(invalid("one or more required v2 piece layers are absent"));
    }

    Ok(())
}

fn merkle_root_from_piece_layer(
    layer: &[u8],
    piece_length: u64,
) -> Result<[u8; 32], MetainfoError> {
    if layer.is_empty() || !layer.len().is_multiple_of(32) {
        return Err(invalid("piece layer must contain 32-byte hashes"));
    }

    let (chunks, remainder) = layer.as_chunks::<32>();
    if !remainder.is_empty() {
        return Err(invalid("piece layer must contain 32-byte hashes"));
    }
    let mut hashes: Vec<[u8; 32]> = chunks.to_vec();

    let padded_len = hashes
        .len()
        .checked_next_power_of_two()
        .ok_or_else(|| invalid("piece layer hash count overflow"))?;
    let zero = zero_hash_for_piece_length(piece_length)?;
    hashes.resize(padded_len, zero);

    while hashes.len() > 1 {
        let mut parents = Vec::with_capacity(hashes.len() / 2);
        let (pairs, remainder) = hashes.as_slice().as_chunks::<2>();
        debug_assert!(remainder.is_empty());
        for pair in pairs {
            let mut input = [0_u8; 64];
            input[..32].copy_from_slice(&pair[0]);
            input[32..].copy_from_slice(&pair[1]);
            parents.push(digest32(&input));
        }
        hashes = parents;
    }

    Ok(hashes[0])
}

fn zero_hash_for_piece_length(piece_length: u64) -> Result<[u8; 32], MetainfoError> {
    if piece_length < V2_BLOCK_SIZE || !piece_length.is_power_of_two() {
        return Err(invalid("invalid v2 piece length"));
    }

    let mut zero = [0_u8; 32];
    let mut covered = V2_BLOCK_SIZE;
    while covered < piece_length {
        let mut input = [0_u8; 64];
        input[..32].copy_from_slice(&zero);
        input[32..].copy_from_slice(&zero);
        zero = digest32(&input);
        covered = covered
            .checked_mul(2)
            .ok_or_else(|| invalid("piece length overflow"))?;
    }
    Ok(zero)
}

fn validate_hybrid(v1: &V1Parsed, v2: &V2Parsed) -> Result<(), MetainfoError> {
    if v1.piece_length != v2.piece_length {
        return Err(invalid("hybrid v1/v2 piece length mismatch"));
    }

    let mut offset = 0_u64;
    for entry in &v1.layout {
        if !entry.padding && entry.size > 0 && !offset.is_multiple_of(v1.piece_length) {
            return Err(invalid(
                "hybrid v1 layout does not align content files to piece boundaries",
            ));
        }
        offset = offset
            .checked_add(entry.size)
            .ok_or_else(|| invalid("hybrid v1 layout size overflow"))?;
    }

    let v1_files: Vec<(Vec<String>, u64)> = v1
        .content_files
        .iter()
        .map(|file| (file.relative_path.clone(), file.size))
        .collect();

    let mut v2_files: Vec<(Vec<String>, u64)> = v2
        .files
        .iter()
        .map(|file| (file.path.clone(), file.size))
        .collect();

    if v1.multi_file {
        if !v2_files
            .iter()
            .all(|(path, _)| path.first().is_some_and(|part| part == &v1.name))
        {
            return Err(invalid(
                "hybrid v2 multi-file layout is missing the v1 root name",
            ));
        }
        for (path, _) in &mut v2_files {
            path.remove(0);
        }
    }

    if v1_files != v2_files {
        return Err(invalid("hybrid v1/v2 file layouts differ"));
    }

    Ok(())
}

fn parse_path_list(value: &Value<'_>) -> Result<Vec<String>, MetainfoError> {
    let items = value
        .as_list()
        .ok_or_else(|| invalid("v1 path must be a list"))?;
    if items.is_empty() {
        return Err(invalid("v1 path must not be empty"));
    }

    let mut path = Vec::with_capacity(items.len());
    for item in items {
        let component = item
            .as_str()
            .ok_or_else(|| invalid("v1 path component is not valid UTF-8"))?
            .to_string();
        validate_component(&component)?;
        path.push(component);
    }
    validate_path(&path)?;
    Ok(path)
}

fn validate_path(path: &[String]) -> Result<(), MetainfoError> {
    let mut length = 0_usize;
    for (index, component) in path.iter().enumerate() {
        validate_component(component)?;
        length = length
            .checked_add(component.len())
            .and_then(|value| value.checked_add(usize::from(index > 0)))
            .ok_or_else(|| invalid("logical path length overflow"))?;
    }
    if length > MAX_LOGICAL_PATH_BYTES {
        return Err(invalid("logical path exceeds parser limit"));
    }
    Ok(())
}

fn validate_component(component: &str) -> Result<(), MetainfoError> {
    if component.is_empty()
        || component == "."
        || component == ".."
        || component.len() > MAX_PATH_COMPONENT_BYTES
        || component.chars().any(char::is_control)
        || component.contains('/')
        || component.contains('\\')
    {
        return Err(invalid("unsafe or unsupported logical path component"));
    }
    Ok(())
}

fn required_utf8(value: &Value<'_>, key: &[u8]) -> Result<String, MetainfoError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid(format!("{} must be UTF-8 bytes", display_key(key))))
}

fn required_bytes<'a>(value: &'a Value<'a>, key: &[u8]) -> Result<&'a [u8], MetainfoError> {
    value
        .get(key)
        .and_then(Value::as_bytes)
        .ok_or_else(|| invalid(format!("{} must be bytes", display_key(key))))
}

fn positive_u64(value: &Value<'_>, key: &[u8]) -> Result<u64, MetainfoError> {
    let number = value
        .get(key)
        .and_then(Value::as_int)
        .ok_or_else(|| invalid(format!("{} must be an integer", display_key(key))))?;
    u64::try_from(number)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid(format!("{} must be positive", display_key(key))))
}

fn nonnegative_u64(value: &Value<'_>, key: &[u8]) -> Result<u64, MetainfoError> {
    let number = value
        .get(key)
        .and_then(Value::as_int)
        .ok_or_else(|| invalid(format!("{} must be an integer", display_key(key))))?;
    u64::try_from(number).map_err(|_| invalid(format!("{} must be non-negative", display_key(key))))
}

fn optional_int(value: &Value<'_>, key: &[u8]) -> Result<Option<i64>, MetainfoError> {
    match value.get(key) {
        Some(raw) => raw
            .as_int()
            .map(Some)
            .ok_or_else(|| invalid(format!("{} must be an integer", display_key(key)))),
        None => Ok(None),
    }
}

fn require_dict<'a>(
    value: &'a Value<'a>,
    context: &str,
) -> Result<&'a [(&'a [u8], Value<'a>)], MetainfoError> {
    value
        .as_dict()
        .ok_or_else(|| invalid(format!("{context} must be a dictionary")))
}

fn digest20(bytes: &[u8]) -> [u8; 20] {
    let digest = Sha1::digest(bytes);
    let mut output = [0_u8; 20];
    output.copy_from_slice(&digest);
    output
}

fn digest32(bytes: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(bytes);
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

fn join_path(path: &[String]) -> String {
    path.join("/")
}

fn div_ceil(value: u64, divisor: u64) -> u64 {
    value.div_ceil(divisor)
}

fn display_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn invalid(message: impl Into<String>) -> MetainfoError {
    MetainfoError::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const V2_PIECES_ROOT: [u8; 32] = [
        81, 137, 199, 125, 41, 254, 93, 84, 106, 4, 94, 196, 105, 134, 133, 39, 133, 254,
        165, 193, 58, 199, 218, 156, 17, 95, 245, 251, 110, 223, 129, 124,
    ];

    fn v2_piece_layer_fixture(second_piece_byte: u8) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(
            b"d4:infod9:file treed8:file.bind0:d6:lengthi20000e11:pieces root32:",
        );
        bytes.extend_from_slice(&V2_PIECES_ROOT);
        bytes.extend_from_slice(
            b"eee12:meta versioni2e4:name4:test12:piece lengthi16384ee12:piece layersd32:",
        );
        bytes.extend_from_slice(&V2_PIECES_ROOT);
        bytes.extend_from_slice(b"64:");
        bytes.extend_from_slice(&[0x11; 32]);
        bytes.extend_from_slice(&[second_piece_byte; 32]);
        bytes.extend_from_slice(b"ee");
        bytes
    }

    #[test]
    fn parses_v1_single_file_and_hashes_raw_info() {
        let bytes =
            b"d4:infod6:lengthi4e4:name8:file.bin12:piece lengthi4e6:pieces20:01234567890123456789ee";
        let parsed = parse_inner(bytes).expect("v1 parse");

        assert_eq!(
            parsed.identity.v1,
            Some([
                145, 220, 39, 1, 166, 54, 73, 96, 19, 94, 201, 143, 124, 177, 8, 95, 204, 242, 59,
                23,
            ])
        );
        assert!(parsed.identity.v2.is_none());
        assert_eq!(parsed.manifest.total_size, 4);
        assert_eq!(parsed.manifest.files[0].path, "file.bin");
    }

    #[test]
    fn parses_zero_length_v2_file() {
        let bytes = b"d4:infod9:file treed8:file.txtd0:d6:lengthi0eeee12:meta versioni2e4:name4:test12:piece lengthi16384eee";
        let parsed = parse_inner(bytes).expect("v2 parse");

        assert!(parsed.identity.v1.is_none());
        assert_eq!(
            parsed.identity.v2,
            Some([
                177, 67, 53, 221, 241, 120, 91, 54, 80, 223, 159, 163, 230, 139, 165, 211, 220,
                178, 196, 86, 86, 161, 56, 36, 121, 241, 37, 40, 132, 80, 102, 14,
            ])
        );
        assert_eq!(parsed.manifest.files[0].path, "file.txt");
        assert_eq!(parsed.manifest.files[0].size, 0);
    }

    #[test]
    fn parses_v2_piece_layer_with_pinned_info_hash() {
        let bytes = v2_piece_layer_fixture(0x22);
        let parsed = parse_inner(&bytes).expect("v2 piece-layer parse");

        assert_eq!(
            parsed.identity.v2,
            Some([
                8, 248, 145, 26, 216, 233, 43, 98, 66, 203, 217, 94, 86, 185, 210, 84, 122,
                39, 10, 40, 233, 119, 89, 14, 124, 59, 103, 34, 117, 142, 62, 204,
            ])
        );
        assert_eq!(parsed.manifest.total_size, 20_000);
        assert_eq!(parsed.manifest.files[0].path, "file.bin");
        assert_eq!(parsed.manifest.files[0].size, 20_000);
    }

    #[test]
    fn rejects_v2_piece_layer_that_does_not_match_pieces_root() {
        let bytes = v2_piece_layer_fixture(0x23);
        let error = parse_inner(&bytes).expect_err("piece-layer mismatch");
        assert!(error.to_string().contains("does not match pieces root"));
    }

    #[test]
    fn parses_zero_length_hybrid_file() {
        let bytes = b"d4:infod9:file treed8:file.txtd0:d6:lengthi0eeee6:lengthi0e12:meta versioni2e4:name8:file.txt12:piece lengthi16384e6:pieces0:ee";
        let parsed = parse_inner(bytes).expect("hybrid parse");

        assert!(parsed.identity.is_hybrid());
        assert_eq!(
            parsed.identity.v1,
            Some([
                227, 251, 105, 158, 61, 244, 46, 15, 2, 14, 108, 54, 57, 38, 202, 123, 79, 36, 151,
                242,
            ])
        );
        assert_eq!(
            parsed.identity.v2,
            Some([
                187, 117, 114, 56, 246, 32, 155, 17, 121, 127, 87, 181, 139, 71, 27, 103, 124, 35,
                182, 8, 255, 115, 213, 225, 90, 164, 86, 221, 27, 65, 148, 74,
            ])
        );
        assert_eq!(parsed.manifest.files[0].path, "file.txt");
    }

    #[test]
    fn rejects_path_traversal_component() {
        let bytes =
            b"d4:infod5:filesld6:lengthi1e4:pathl2:..1:aeee4:name4:test12:piece lengthi1e6:pieces20:01234567890123456789ee";
        let error = parse_inner(bytes).expect_err("unsafe path");
        assert!(error.to_string().contains("unsafe"));
    }

    #[test]
    fn rejects_future_meta_version() {
        let bytes = b"d4:infod12:meta versioni3e4:name4:testee";
        let error = parse_inner(bytes).expect_err("unsupported version");
        assert!(matches!(error, MetainfoError::Unsupported(_)));
    }
}
