use std::io;

use bytes::Bytes;
use thiserror::Error;

pub const DEFAULT_PIPE: &str = r"\\.\pipe\qbctl";
pub const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum IpcError {
    #[error("IPC connection closed")]
    Closed,
    #[error("IPC timed out")]
    TimedOut,
    #[error("IPC is only supported on Windows")]
    UnsupportedPlatform,
    #[error("IPC I/O error: {0}")]
    Io(#[from] io::Error),
}

fn checked_frame(frame: impl Into<Bytes>) -> Result<Bytes, IpcError> {
    let frame = frame.into();
    validate_frame_size(&frame)?;
    Ok(frame)
}

#[cfg(windows)]
mod platform {
    use std::{ffi::c_void, mem::size_of, ptr};

    use bytes::Bytes;
    use futures_util::{SinkExt, StreamExt};
    use tokio::{
        io::{AsyncRead, AsyncWrite},
        net::windows::named_pipe::{
            ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
        },
    };
    use tokio_util::codec::{Framed, LengthDelimitedCodec};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::{
                ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
            },
            PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES,
        },
    };

    use super::{checked_frame, IpcError, MAX_FRAME_LENGTH};

    const PIPE_SDDL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)";

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
        framed.send(checked_frame(frame)?).await?;
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
            send_frame(&mut self.framed, checked_frame(frame)?).await
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
            send_frame(&mut self.framed, checked_frame(frame)?).await
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
        let sddl: Vec<u16> = PIPE_SDDL.encode_utf16().chain(std::iter::once(0)).collect();
        let mut descriptor: PSECURITY_DESCRIPTOR = ptr::null_mut();

        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(IpcError::Io(io::Error::last_os_error()));
        }

        let mut attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };

        let mut options = ServerOptions::new();
        options.first_pipe_instance(first);
        options.reject_remote_clients(true);

        let created = unsafe {
            options.create_with_security_attributes_raw(
                pipe_name,
                (&mut attributes as *mut SECURITY_ATTRIBUTES).cast::<c_void>(),
            )
        };

        unsafe {
            LocalFree(descriptor);
        }

        Ok(created?)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn secure_server_can_be_created() {
            let name = format!(r"\\.\pipe\qbctl-ipc-test-{}", std::process::id());
            let _server = create_server(&name, true).expect("secure pipe");
        }

        #[test]
        fn security_descriptor_is_protected_and_local_principals_only() {
            assert!(PIPE_SDDL.starts_with("D:P"));
            assert!(PIPE_SDDL.contains(";;;SY)"));
            assert!(PIPE_SDDL.contains(";;;BA)"));
            assert!(PIPE_SDDL.contains(";;;OW)"));
            assert!(!PIPE_SDDL.contains(";;;WD)"));
            assert!(!PIPE_SDDL.contains(";;;AN)"));
        }
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
