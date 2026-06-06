#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::interest::ClientId;
use crate::protocol::DownstreamCommand;

const WS_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Clone)]
pub struct RelayServer {
    engine: Arc<Mutex<RelayEngine>>,
}

impl RelayServer {
    #[must_use]
    pub fn new(engine: Arc<Mutex<RelayEngine>>) -> Self {
        Self { engine }
    }

    #[must_use]
    pub fn engine(&self) -> Arc<Mutex<RelayEngine>> {
        self.engine.clone()
    }

    pub async fn handle_text(
        &self,
        raw_client_id: u64,
        text: String,
    ) -> RelayResult<Vec<DownstreamFrame>> {
        let value: Value = serde_json::from_str(&text)
            .map_err(|err| RelayError::invalid_protocol(format!("invalid JSON frame: {err}")))?;
        let command = DownstreamCommand::from_value(value)?;
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.handle_command(ClientId::new(raw_client_id), command)
    }

    pub async fn serve_once(&self, listener: TcpListener) -> RelayResult<()> {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|err| RelayError::Transport(format!("websocket accept failed: {err}")))?;
        self.serve_stream(ClientId::new(1), &mut stream).await
    }

    pub async fn serve_until(
        &self,
        listener: TcpListener,
        mut shutdown: oneshot::Receiver<()>,
    ) -> RelayResult<()> {
        let mut next_client_id = 1_u64;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(()),
                accepted = listener.accept() => {
                    let (mut stream, _) = accepted.map_err(|err| {
                        RelayError::Transport(format!("websocket accept failed: {err}"))
                    })?;
                    let client_id = ClientId::new(next_client_id);
                    next_client_id = next_client_id.saturating_add(1);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = server.serve_stream(client_id, &mut stream).await {
                            eprintln!("{err}");
                        }
                        if let Err(err) = server.remove_client(client_id) {
                            eprintln!("{err}");
                        }
                    });
                }
            }
        }
    }

    async fn serve_stream(&self, client_id: ClientId, stream: &mut TcpStream) -> RelayResult<()> {
        accept_handshake(stream).await?;
        loop {
            match read_client_text_frame(stream).await {
                Ok(text) => {
                    let frames = self.handle_text(client_id.value(), text).await?;
                    for frame in frames {
                        write_server_text_frame(stream, frame.payload.to_string()).await?;
                    }
                }
                Err(RelayError::Transport(message)) if message.contains("early eof") => {
                    return Ok(());
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn remove_client(&self, client_id: ClientId) -> RelayResult<()> {
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.remove_client(client_id);
        Ok(())
    }
}

async fn accept_handshake(stream: &mut TcpStream) -> RelayResult<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.map_err(|err| {
            RelayError::Transport(format!("websocket handshake read failed: {err}"))
        })?;
        if read == 0 {
            return Err(RelayError::invalid_protocol(
                "websocket handshake ended early",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let request = String::from_utf8(buffer).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid websocket handshake: {err}"))
    })?;
    let key = request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("sec-websocket-key")
                .then_some(value.trim())
        })
        .ok_or_else(|| RelayError::invalid_protocol("missing sec-websocket-key"))?;
    let accept = websocket_accept_key(key);
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: {accept}\r\n\
\r\n"
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| RelayError::Transport(format!("websocket handshake write failed: {err}")))?;
    Ok(())
}

async fn read_client_text_frame(stream: &mut TcpStream) -> RelayResult<String> {
    let mut header = [0_u8; 2];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket frame read failed: {err}")))?;
    let opcode = header[0] & 0x0f;
    if opcode == 0x8 {
        return Ok(r#"{"aid":"peek_message"}"#.to_string());
    }
    if opcode != 0x1 {
        return Err(RelayError::invalid_protocol(
            "relay expects websocket text frames",
        ));
    }
    let masked = header[1] & 0x80 != 0;
    let mut len = u64::from(header[1] & 0x7f);
    if len == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).await.map_err(|err| {
            RelayError::Transport(format!("websocket extended length read failed: {err}"))
        })?;
        len = u64::from(u16::from_be_bytes(extended));
    }
    let mut mask = [0_u8; 4];
    if masked {
        stream
            .read_exact(&mut mask)
            .await
            .map_err(|err| RelayError::Transport(format!("websocket mask read failed: {err}")))?;
    }
    let mut payload = vec![
        0_u8;
        usize::try_from(len).map_err(|_| {
            RelayError::invalid_protocol("websocket payload length exceeds usize")
        })?
    ];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket payload read failed: {err}")))?;
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % mask.len()];
        }
    }
    String::from_utf8(payload).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid websocket text payload: {err}"))
    })
}

async fn write_server_text_frame(stream: &mut TcpStream, text: String) -> RelayResult<()> {
    let bytes = text.as_bytes();
    let mut frame = Vec::with_capacity(bytes.len() + 10);
    frame.push(0x81);
    match bytes.len() {
        len @ 0..=125 => frame.push(len as u8),
        len @ 126..=65535 => {
            frame.push(126);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        }
        len => {
            frame.push(127);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(bytes);
    stream
        .write_all(&frame)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket frame write failed: {err}")))
}

fn websocket_accept_key(client_key: &str) -> String {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(client_key.as_bytes());
    hasher.update(WS_ACCEPT_GUID.as_bytes());
    base64_standard(&hasher.finalize())
}

fn base64_standard(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        encoded.push(ALPHABET[(b0 >> 2) as usize] as char);
        encoded.push(ALPHABET[(((b0 & 0b0000_0011) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((b1 & 0b0000_1111) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(b2 & 0b0011_1111) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}
