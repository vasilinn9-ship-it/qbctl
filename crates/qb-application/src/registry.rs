use qb_domain::torrent::TorrentIdentity;

use crate::PortError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryState {
    Incoming,
    Processing,
    Finished,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistryRecord {
    pub registry_id: String,
    pub identity: TorrentIdentity,
    pub state: RegistryState,
    pub source_relative: String,
    pub source_metainfo_digest: [u8; 32],
    pub operation_id: Option<String>,
    pub archive_ref: Option<String>,
    pub handoff_file_count: u32,
    pub handoff_receipt_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterIncoming {
    pub identity: TorrentIdentity,
    pub source_relative: String,
    pub source_metainfo_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegisterIncomingResult {
    Registered(RegistryRecord),
    AlreadyPresent(RegistryRecord),
}

pub trait TorrentRegistry: Send + Sync {
    fn find_by_identity(
        &self,
        identity: &TorrentIdentity,
    ) -> Result<Option<RegistryRecord>, PortError>;

    fn register_incoming(
        &self,
        candidate: &RegisterIncoming,
    ) -> Result<RegisterIncomingResult, PortError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_state_is_explicit_and_transport_independent() {
        let input = RegisterIncoming {
            identity: TorrentIdentity::new(Some([0x11; 20]), Some([0x22; 32]))
                .expect("identity"),
            source_relative: "sample.torrent".into(),
            source_metainfo_digest: [0x33; 32],
        };

        assert!(input.identity.is_hybrid());
        assert_eq!(input.source_relative, "sample.torrent");
        assert_eq!(RegistryState::Incoming, RegistryState::Incoming);
    }
}
