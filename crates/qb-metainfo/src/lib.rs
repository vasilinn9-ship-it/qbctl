//! Local BitTorrent metainfo adapter.
//!
//! Slice 1 establishes this dependency boundary only.
//! Parsing, v1/v2/hybrid identity extraction, and manifest generation are
//! implemented in Slice 2 behind the application MetainfoReader port.
//!
//! This crate must not depend on the qBittorrent Web API adapter.

pub const ADAPTER_NAME: &str = "local BitTorrent metainfo";
