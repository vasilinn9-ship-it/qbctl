mod dispatch;
mod encode;
mod handshake;

use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use prost::Message;
use qb_application::{system::SystemService, torrent::TorrentService};
use qb_ipc::{IpcError, ServerConnection};
use qb_proto::v1::Request;
use tokio::time::timeout;

use crate::runtime::RuntimeContext;

const IPC_IO_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn serve_connection(
    mut connection: ServerConnection,
    runtime: Arc<RuntimeContext>,
    system: Arc<SystemService>,
    torrents: Option<Arc<TorrentService>>,
    qbit_startup_problem: Option<Arc<str>>,
) -> Result<()> {
    let handshake = timeout(
        IPC_IO_TIMEOUT,
        handshake::perform(&mut connection, &runtime),
    )
    .await
    .map_err(|_| IpcError::TimedOut)??;

    if !handshake {
        return Ok(());
    }

    loop {
        let frame = match timeout(IPC_IO_TIMEOUT, connection.recv_frame()).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(IpcError::Closed)) => return Ok(()),
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err(IpcError::TimedOut.into()),
        };
        let request = Request::decode(frame).context("decode protocol request")?;

        let response = dispatch::dispatch(
            request,
            &system,
            torrents.as_deref(),
            qbit_startup_problem.as_deref(),
        )
        .await;
        timeout(
            IPC_IO_TIMEOUT,
            connection.send_frame(response.encode_to_vec()),
        )
        .await
        .map_err(|_| IpcError::TimedOut)??;
    }
}

pub(crate) fn capabilities() -> Vec<String> {
    vec![
        "ipc.protobuf.v1".into(),
        "journal.sqlite.v1".into(),
        "status.v1".into(),
        "doctor.v1".into(),
        "torrent.read.v1".into(),
        "queue.read.v1".into(),
        "transfer.limits.read.v1".into(),
        "qbit.probe.v1".into(),
    ]
}
