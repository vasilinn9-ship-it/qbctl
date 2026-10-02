use qb_ipc::{IpcError, ServerConnection};
use qb_proto::{
    v1::{ClientHello, ServerHello},
    PROTOCOL_MAJOR, PROTOCOL_MINOR,
};

use crate::runtime::RuntimeContext;

pub async fn perform(
    connection: &mut ServerConnection,
    runtime: &RuntimeContext,
) -> Result<bool, IpcError> {
    let hello: ClientHello = connection.recv().await?;

    let response = ServerHello {
        protocol_major: PROTOCOL_MAJOR,
        protocol_minor: PROTOCOL_MINOR,
        daemon_version: env!("CARGO_PKG_VERSION").to_string(),
        capabilities: super::capabilities(),
        instance_id: runtime.instance_id().to_string(),
    };
    connection.send(&response).await?;

    Ok(hello.protocol_major == PROTOCOL_MAJOR)
}
