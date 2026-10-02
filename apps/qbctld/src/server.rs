use qb_ipc::{IpcError, ServerConnection, ServerListener};

pub struct Server {
    listener: ServerListener,
}

impl Server {
    pub fn bind(pipe_name: impl Into<String>) -> Result<Self, IpcError> {
        Ok(Self {
            listener: ServerListener::bind(pipe_name)?,
        })
    }

    pub async fn accept(&mut self) -> Result<ServerConnection, IpcError> {
        self.listener.accept().await
    }
}
