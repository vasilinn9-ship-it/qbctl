use anyhow::{Context as _, Result};
use prost::Message;
use qb_ipc::ServerConnection;
use qb_proto::{
    v1::{ClientHello, ServerHello},
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

use crate::runtime::RuntimeContext;

pub async fn perform(connection: &mut ServerConnection, runtime: &RuntimeContext) -> Result<bool> {
    let hello = ClientHello::decode(connection.recv_frame().await?)
        .context("decode protocol client hello")?;

    let response = ServerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: super::capabilities(),
        instance_id: runtime.instance_id().to_string(),
    };
    connection.send_frame(response.encode_to_vec()).await?;

    Ok(hello.protocol_major == PROTOCOL_MAJOR)
}
