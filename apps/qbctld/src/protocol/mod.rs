mod dispatch;
mod encode;
mod handshake;

use std::sync::Arc;

use qb_application::system::SystemService;
use qb_ipc::{IpcError, ServerConnection};
use qb_proto::v1::Request;

use crate::runtime::RuntimeContext;

pub async fn serve_connection(
    mut connection: ServerConnection,
    runtime: Arc<RuntimeContext>,
    system: Arc<SystemService>,
) -> Result<(), IpcError> {
    if !handshake::perform(&mut connection, &runtime).await? {
        return Ok(());
    }

    loop {
        let request: Request = match connection.recv().await {
            Ok(request) => request,
            Err(IpcError::Closed) => return Ok(()),
            Err(error) => return Err(error),
        };

        let response = dispatch::dispatch(request, &system);
        connection.send(&response).await?;
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
