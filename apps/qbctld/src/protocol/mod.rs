mod dispatch;
mod encode;
mod handshake;

use std::sync::Arc;

use anyhow::{Context as _, Result};
use prost::Message;
use qb_application::system::SystemService;
use qb_ipc::{IpcError, ServerConnection};
use qb_proto::v1::Request;

use crate::runtime::RuntimeContext;

pub async fn serve_connection(
    mut connection: ServerConnection,
    runtime: Arc<RuntimeContext>,
    system: Arc<SystemService>,
) -> Result<()> {
    if !handshake::perform(&mut connection, &runtime).await? {
        return Ok(());
    }

    loop {
        let frame = match connection.recv_frame().await {
            Ok(frame) => frame,
            Err(IpcError::Closed) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let request = Request::decode(frame).context("decode protocol request")?;

        let response = dispatch::dispatch(request, &system);
        connection.send_frame(response.encode_to_vec()).await?;
    }
}

pub(crate) fn capabilities() -> Vec<String> {
    vec![
        "ipc.protobuf.v1".into(),
        "journal.sqlite.v1".into(),
        "status.v1".into(),
        "doctor.v1".into(),
    ]
}
