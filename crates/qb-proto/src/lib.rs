pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 0;

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/qbctl.v1.rs"));
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::v1::ClientHello;

    #[test]
    fn protobuf_round_trip() {
        let hello = ClientHello {
            protocol_major: super::PROTOCOL_MAJOR,
            protocol_minor: super::PROTOCOL_MINOR,
            client_version: "test".into(),
            capabilities: vec!["status.v1".into()],
        };
        let bytes = hello.encode_to_vec();
        let decoded = ClientHello::decode(bytes.as_slice()).expect("decode");
        assert_eq!(decoded.client_version, "test");
    }
}
