use std::io;

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
    #[error("invalid protobuf payload: {0}")]
    Decode(#[from] prost::DecodeError),
}

#[cfg(windows)]
mod platform {
    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use prost::Message;
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

    async fn send_message<S, M>(
        framed: &mut Framed<S, LengthDelimitedCodec>,
        message: &M,
    ) -> Result<(), IpcError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        M: Message,
    {
        framed.send(Bytes::from(message.encode_to_vec())).await?;
        Ok(())
    }

    async fn recv_message<S, M>(
        framed: &mut Framed<S, LengthDelimitedCodec>,
    ) -> Result<M, IpcError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        M: Message + Default,
    {
        match framed.next().await {
            Some(Ok(bytes)) => Ok(M::decode(bytes)?),
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

        pub async fn send<M: Message>(&mut self, message: &M) -> Result<(), IpcError> {
            send_message(&mut self.framed, message).await
        }

        pub async fn recv<M: Message + Default>(&mut self) -> Result<M, IpcError> {
            recv_message(&mut self.framed).await
        }
    }

    pub struct ServerConnection {
        framed: Framed<NamedPipeServer, LengthDelimitedCodec>,
    }

    impl ServerConnection {
        pub async fn send<M: Message>(&mut self, message: &M) -> Result<(), IpcError> {
            send_message(&mut self.framed, message).await
        }

        pub async fn recv<M: Message + Default>(&mut self) -> Result<M, IpcError> {
            recv_message(&mut self.framed).await
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
    use prost::Message;

    use super::IpcError;

    pub struct ClientConnection;
    pub struct ServerConnection;
    pub struct ServerListener;

    impl ClientConnection {
        pub async fn connect(_pipe_name: &str) -> Result<Self, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn send<M: Message>(&mut self, _message: &M) -> Result<(), IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn recv<M: Message + Default>(&mut self) -> Result<M, IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }
    }

    impl ServerConnection {
        pub async fn send<M: Message>(&mut self, _message: &M) -> Result<(), IpcError> {
            Err(IpcError::UnsupportedPlatform)
        }

        pub async fn recv<M: Message + Default>(&mut self) -> Result<M, IpcError> {
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
