# Metainfo parser contract

`qb-metainfo` parses local .torrent bytes independently of qBittorrent.

## Why local parsing

Identity and manifest extraction must:
- work when qBittorrent is unavailable;
- describe the exact Incoming bytes being admitted;
- avoid coupling to WebAPI response/version changes;
- validate hostile input before add.

qBittorrent parseMetadata is therefore not the source of truth for admission identity.

## Supported formats

Rust v1 supports only formats that can be validated strictly:
- BitTorrent v1;
- BitTorrent v2;
- hybrid v1/v2.

Unsupported/ambiguous metainfo fails closed.

## Identity

- v1: SHA-1 of the exact raw bencoded info dictionary bytes.
- v2: SHA-256 of the exact raw bencoded info dictionary bytes.
- hybrid: expose both aliases after validating compatible content layout.

Never hash a re-encoded info dictionary.

## Output

The application receives only:
- identity aliases;
- logical file paths;
- file sizes;
- total payload size.

Tracker/passkey/private URL data is not returned into durable application state.

## Validation

Parser must reject malformed input where identity or manifest cannot be proven.

For v2/hybrid validate the required BEP 52 structure, including file-tree consistency and the piece-layer requirements needed for trustworthy identity/manifest extraction.

Logical path components in managed Rust v1 must be valid UTF-8. Windows reserved names, traversal and reparse validation are owned by Slice 3 / qb-win.

## Resource safety

Parsing is bounded:
- input size limit;
- nesting/depth limit;
- file count limit;
- path component/path length limits at the parser/application boundary;
- no unbounded recursive allocation from attacker-controlled bencode.

Concrete limits are implementation constants with hostile-input tests.

## Library policy

A third-party bencode/metainfo crate may be used only if it passes independent v1/v2/hybrid fixtures and hostile-input tests.

If a library cannot preserve exact raw info bytes or validate the required v2/hybrid semantics, wrap/replace it rather than weakening the contract.
