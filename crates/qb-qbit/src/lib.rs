//! qBittorrent adapter crate.
//!
//! Slice 1 intentionally contains no HTTP implementation. Slice 2 owns the
//! WebUI API adapter and the isolated local metainfo parser.

pub const ADAPTER_NAME: &str = "qBittorrent WebUI API";
