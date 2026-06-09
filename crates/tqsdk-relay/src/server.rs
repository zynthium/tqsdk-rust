#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::engine::{DownstreamFrame, RelayEngine};
use crate::error::{RelayError, RelayResult};
use crate::interest::ClientId;
use crate::protocol::DownstreamCommand;
use crate::upstream::{UpstreamMarketEvent, UpstreamSourceUpdate, UpstreamTickSource};

const WS_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

enum ClientWebSocketFrame {
    Text(String),
    Ping(Vec<u8>),
    Pong,
    Close,
}

#[derive(Clone)]
pub struct RelayServer {
    engine: Arc<Mutex<RelayEngine>>,
    outbound: Arc<Mutex<HashMap<ClientId, mpsc::UnboundedSender<Value>>>>,
}

impl RelayServer {
    #[must_use]
    pub fn new(engine: Arc<Mutex<RelayEngine>>) -> Self {
        Self {
            engine,
            outbound: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn engine(&self) -> Arc<Mutex<RelayEngine>> {
        self.engine.clone()
    }

    pub fn dispatch_frames<I>(&self, frames: I) -> RelayResult<usize>
    where
        I: IntoIterator<Item = DownstreamFrame>,
    {
        let mut sent = 0_usize;
        let mut stale_clients = Vec::new();
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?;
        for frame in frames {
            let Some(sender) = outbound.get(&frame.client_id) else {
                continue;
            };
            if sender.send(frame.payload).is_ok() {
                sent = sent.saturating_add(1);
            } else {
                stale_clients.push(frame.client_id);
            }
        }
        for client_id in stale_clients {
            outbound.remove(&client_id);
        }
        Ok(sent)
    }

    pub async fn pump_upstream_once<S>(&self, source: &mut S) -> RelayResult<usize>
    where
        S: UpstreamTickSource + Send,
    {
        let update = source.next_update().await;
        let progress = source.take_progress();
        let invalid_rows = source.take_invalid_tick_rows();
        let last_error = source.take_last_invalid_tick_row_error();
        let Some(update) = update else {
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            engine.record_upstream_progress(progress);
            engine.record_upstream_invalid_tick_rows(invalid_rows, last_error);
            return Ok(0);
        };
        let frames = {
            let mut engine = self
                .engine
                .lock()
                .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
            engine.record_upstream_progress(progress);
            engine.record_upstream_invalid_tick_rows(invalid_rows, last_error);
            ingest_upstream_update(&mut engine, update)?
        };
        self.dispatch_frames(frames)
    }

    pub async fn pump_upstream_until<S>(
        &self,
        source: &mut S,
        mut shutdown: oneshot::Receiver<()>,
    ) -> RelayResult<usize>
    where
        S: UpstreamTickSource + Send,
    {
        let mut sent = 0_usize;
        loop {
            let update = tokio::select! {
                biased;
                _ = &mut shutdown => return Ok(sent),
                update = source.next_update() => update,
            };
            let progress = source.take_progress();
            let invalid_rows = source.take_invalid_tick_rows();
            let last_error = source.take_last_invalid_tick_row_error();
            let frames = {
                let mut engine = self
                    .engine
                    .lock()
                    .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
                engine.record_upstream_progress(progress);
                engine.record_upstream_invalid_tick_rows(invalid_rows, last_error);
                let Some(update) = update else {
                    return Ok(sent);
                };
                ingest_upstream_update(&mut engine, update)?
            };
            sent = sent.saturating_add(self.dispatch_frames(frames)?);
        }
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
        let mut outbound = self.register_client(client_id)?;
        loop {
            tokio::select! {
                read = read_client_frame(stream) => {
                    match read {
                        Ok(ClientWebSocketFrame::Text(text)) => {
                            let frames = self.handle_text(client_id.value(), text).await?;
                            self.dispatch_frames(frames)?;
                        }
                        Ok(ClientWebSocketFrame::Ping(payload)) => {
                            write_server_control_frame(stream, 0x0a, &payload).await?;
                        }
                        Ok(ClientWebSocketFrame::Pong) => {}
                        Ok(ClientWebSocketFrame::Close) => {
                            return Ok(());
                        }
                        Err(RelayError::Transport(message)) if message.contains("early eof") => {
                            return Ok(());
                        }
                        Err(err) => return Err(err),
                    }
                }
                payload = outbound.recv() => {
                    let Some(payload) = payload else {
                        return Ok(());
                    };
                    write_server_text_frame(stream, payload.to_string()).await?;
                }
            }
        }
    }

    fn remove_client(&self, client_id: ClientId) -> RelayResult<()> {
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| RelayError::Internal("relay engine lock poisoned".to_string()))?;
        engine.remove_client(client_id);
        drop(engine);
        self.outbound
            .lock()
            .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?
            .remove(&client_id);
        Ok(())
    }

    fn register_client(&self, client_id: ClientId) -> RelayResult<mpsc::UnboundedReceiver<Value>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        self.outbound
            .lock()
            .map_err(|_| RelayError::Internal("relay outbound lock poisoned".to_string()))?
            .insert(client_id, sender);
        Ok(receiver)
    }
}

fn ingest_upstream_update(
    engine: &mut RelayEngine,
    update: UpstreamSourceUpdate,
) -> RelayResult<Vec<DownstreamFrame>> {
    match update {
        UpstreamSourceUpdate::Event(event) => ingest_upstream_event(engine, event),
        UpstreamSourceUpdate::Progress => Ok(Vec::new()),
    }
}

fn ingest_upstream_event(
    engine: &mut RelayEngine,
    event: UpstreamMarketEvent,
) -> RelayResult<Vec<DownstreamFrame>> {
    match event {
        UpstreamMarketEvent::Tick(tick) => engine.ingest_tick(tick.symbol, tick.row),
        UpstreamMarketEvent::Quote(quote) => engine.ingest_quote(quote.symbol, quote.quote),
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

async fn read_client_frame(stream: &mut TcpStream) -> RelayResult<ClientWebSocketFrame> {
    let mut header = [0_u8; 2];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|err| RelayError::Transport(format!("websocket frame read failed: {err}")))?;
    let opcode = header[0] & 0x0f;
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
    match opcode {
        0x1 => String::from_utf8(payload)
            .map(ClientWebSocketFrame::Text)
            .map_err(|err| {
                RelayError::invalid_protocol(format!("invalid websocket text payload: {err}"))
            }),
        0x8 => Ok(ClientWebSocketFrame::Close),
        0x9 => Ok(ClientWebSocketFrame::Ping(payload)),
        0x0a => Ok(ClientWebSocketFrame::Pong),
        _ => Err(RelayError::invalid_protocol(
            "relay expects websocket text or control frames",
        )),
    }
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

async fn write_server_control_frame(
    stream: &mut TcpStream,
    opcode: u8,
    payload: &[u8],
) -> RelayResult<()> {
    if payload.len() > 125 {
        return Err(RelayError::invalid_protocol(
            "websocket control frame payload exceeds 125 bytes",
        ));
    }
    let mut frame = Vec::with_capacity(payload.len() + 2);
    frame.push(0x80 | opcode);
    frame.push(payload.len() as u8);
    frame.extend_from_slice(payload);
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
