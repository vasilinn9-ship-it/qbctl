use std::io;

use bytes::Bytes;
use thiserror::Error;

pub const DEFAULT_PIPE: &str = r"\\.\pipe\qbctl";
pub const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("IPC connection closed")]
    Closed,
    #[error("IPC is only supported on Windows")]
    UnsupportedPlatform,
    #[error("IPC I/O error: {0}")]
    Io(#[from] io::Error),
}

#[cfg(windows)]
mod platform {
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncRead, AsyncWrite},
        net::windows::named_pipe::{
            ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
        },
    };
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use super::{IpcError, MAX_FRAME_LENGTH};

    fn codec() -> LengthDelimitedCodec {
        LengthDelimitedCodec::builder()
            .little_endian()
            .max_frame_length(MAX_FRAME_LENGTH)
            .new_codec()
    }

    async fn send_frame<S>(
        framed: &mut Framed<S, LengthDelimitedCodec>,
        frame: Bytes,
    ) -> Result<(), IpcError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        framed.send(frame).await?;
        Ok(())
    }

    async fn recv_frame<S>(framed: &mut Framed<S, LengthDelimitedCodec>) -> Result<Bytes, IpcError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match framed.next().await {
            Some(Ok(bytes)) => Ok(bytes.freeze()),
            Some(Err(error)) => Err(IpcError::Io(error)),
            None => Err(IpcError::Closed),
        }
    }

    pub struct ClientConnection {
        framed: Framed<NamedPipeClient, LengthDelimitedCodec>,
    }

    impl ClientConnection {
        pub async fn connect(pipe_name: &str) -> Result<Self, IpcError> {
            let stream = ClientOptions::new().open(pipe_name)?;
            Ok(Self {
                framed: Framed::new(stream, codec()),
            })
        }

        pub async fn send_frame(&mut self, frame: impl Into<Bytes>) -> Result<(), IpcError> {
            send_frame(&mut self.framed, frame.into()).await
        }

        pub async fn recv_frame(&mut self) -> Result<Bytes, IpcError> {
            recv_frame(&mut self.framed).await
        }
    }

    pub struct ServerConnection {
        framed: Framed<NamedPipeServer, LengthDelimitedCodec>,
    }

    impl ServerConnection {
        pub async fn send_frame(&mut self, frame: impl Into<Bytes>) -> Result<(), IpcError> {
            send_frame(&mut self.framed, frame.into()).await
        }

        pub async fn recv_frame(&mut self) -> Result<Bytes, IpcError> {
            recv_frame(&mut self.framed).await
        }
    }

    pub struct ServerListener {
        pipe_name: String,
        pending: Option<NamedPipeServer>,
        first: bool,
    }

    impl ServerListener {
        pub fn bind(pipe_name: impl Into<String>) -> Result<Self, IpcError> {
            let pipe_name = pipe_name.into();
            let pending = Some(create_server(&pipe_name, true)?);
            Ok(Self {
                pipe_name,
                pending,
                first: false,
            })
        }

        pub async fn accept(&mut self) -> Result<ServerConnection, IpcError> {
            if self.pending.is_none() {
                self.pending = Some(create_server(&self.pipe_name, self.first)?);
                self.first = false;
            }

            let server = self.pending.take().expect("pending server exists");
            server.connect().await?;
            Ok(ServerConnection {
                framed: Framed::new(server, codec()),
            })
        }
    }

    fn create_server(pipe_name: &str, first: bool) -> Result<NamedPipeServer, IpcError> {
        let mut options = ServerOptions::new();
        options.first_pipe_instance(first);
        options.reject_remote_clients(true);
        Ok(options.create(pipe_name)?)
    }
}

#[cfg(not(windows))]
mod platform {
    use bytes::Bytes;

    use super::IpcError;

    pub struct ClientConnection;
    pub struct ServerConnection;
    pub struct ServerListener;

    impl ClientConnection {
        pub async fn connect(_pipe_name: &str) -> Result<Self, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn send_frame(&mut self, _frame: impl Into<Bytes>) -> Result<(), IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn recv_frame(&mut self) -> Result<Bytes, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }
    }

    impl ServerConnection {
        pub async fn send_frame(&mut self, _frame: impl Into<Bytes>) -> Result<(), IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn recv_frame(&mut self) -> Result<Bytes, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }
    }

    impl ServerListener {
        pub fn bind(_pipe_name: impl Into<String>) -> Result<Self, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn accept(&mut self) -> Result<ServerConnection, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }
    }
}

pub use platform::{ClientConnection, ServerConnection, ServerListener};

pub fn validate_frame_size(frame: &Bytes) -> Result<(), IpcError> {
    if frame.len() > MAX_FRAME_LENGTH {
        return Err(IpcError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds maximum length",
        )));
    }
    Ok(())
}
