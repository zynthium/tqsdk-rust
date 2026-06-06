use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};

use sha1::{Digest, Sha1};

const WS_ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Pong,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandshakeRequest {
    pub method: String,
    pub path: String,
    headers: BTreeMap<String, String>,
}

impl HandshakeRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }
}

pub struct TestWebSocketConnection {
    request: HandshakeRequest,
    stream: TcpStream,
}

#[allow(dead_code)]
impl TestWebSocketConnection {
    pub fn request(&self) -> &HandshakeRequest {
        &self.request
    }

    pub fn recv(&mut self) -> io::Result<ClientFrame> {
        let mut header = [0_u8; 2];
        self.stream.read_exact(&mut header)?;

        let opcode = header[0] & 0x0f;
        let masked = header[1] & 0x80 != 0;
        let mut payload_len = u64::from(header[1] & 0x7f);
        if payload_len == 126 {
            let mut extended = [0_u8; 2];
            self.stream.read_exact(&mut extended)?;
            payload_len = u64::from(u16::from_be_bytes(extended));
        } else if payload_len == 127 {
            let mut extended = [0_u8; 8];
            self.stream.read_exact(&mut extended)?;
            payload_len = u64::from_be_bytes(extended);
        }

        let mut mask = [0_u8; 4];
        if masked {
            self.stream.read_exact(&mut mask)?;
        }

        let payload_len = usize::try_from(payload_len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "payload too large"))?;
        let mut payload = vec![0_u8; payload_len];
        self.stream.read_exact(&mut payload)?;

        if masked {
            for (index, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[index % mask.len()];
            }
        }

        match opcode {
            0x1 => String::from_utf8(payload)
                .map(ClientFrame::Text)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err)),
            0x2 => Ok(ClientFrame::Binary(payload)),
            0x9 => Ok(ClientFrame::Ping),
            0xA => Ok(ClientFrame::Pong),
            0x8 => Ok(ClientFrame::Close),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported websocket opcode: {other:#x}"),
            )),
        }
    }

    pub fn send_text(&mut self, text: impl Into<String>) -> io::Result<()> {
        self.send_frame(0x1, text.into().as_bytes())
    }

    pub fn send_binary(&mut self, payload: Vec<u8>) -> io::Result<()> {
        self.send_frame(0x2, &payload)
    }

    pub fn send_close(&mut self) -> io::Result<()> {
        self.send_frame(0x8, &[])
    }

    fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let mut frame = Vec::with_capacity(payload.len() + 10);
        frame.push(0x80 | opcode);

        match payload.len() {
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

        frame.extend_from_slice(payload);
        self.stream.write_all(&frame)?;
        self.stream.flush()
    }
}

pub struct TestWebSocketServer {
    addr: SocketAddr,
    handle: JoinHandle<()>,
}

impl TestWebSocketServer {
    pub fn spawn<F>(handler: F) -> io::Result<Self>
    where
        F: FnOnce(TestWebSocketConnection) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        Self::spawn_with_listener(listener, addr, handler)
    }

    #[allow(dead_code)]
    pub fn spawn_on<F>(addr: SocketAddr, handler: F) -> io::Result<Self>
    where
        F: FnOnce(TestWebSocketConnection) + Send + 'static,
    {
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        Self::spawn_with_listener(listener, addr, handler)
    }

    fn spawn_with_listener<F>(
        listener: TcpListener,
        addr: SocketAddr,
        handler: F,
    ) -> io::Result<Self>
    where
        F: FnOnce(TestWebSocketConnection) + Send + 'static,
    {
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("websocket test accept failed");
            let request = read_handshake_request(&mut stream).expect("websocket test handshake");
            let accept_key = websocket_accept_key(
                request
                    .header("sec-websocket-key")
                    .expect("missing sec-websocket-key header"),
            );
            write_handshake_response(&mut stream, &accept_key)
                .expect("websocket test handshake response");
            handler(TestWebSocketConnection { request, stream });
        });

        Ok(Self { addr, handle })
    }

    pub fn url(&self, path: &str) -> String {
        format!("ws://{}{}", self.addr, path)
    }

    pub fn join(self) {
        self.handle.join().expect("websocket test server panicked");
    }
}

fn read_handshake_request(stream: &mut TcpStream) -> io::Result<HandshakeRequest> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];

    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected eof while reading websocket handshake",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
    }

    let request =
        String::from_utf8(buffer).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut lines = request.split("\r\n").filter(|line| !line.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing method"))?;
    let path = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing path"))?;

    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();

    Ok(HandshakeRequest {
        method: method.to_string(),
        path: path.to_string(),
        headers,
    })
}

fn websocket_accept_key(client_key: &str) -> String {
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

fn write_handshake_response(stream: &mut TcpStream, accept_key: &str) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Accept: {accept_key}\r\n\
\r\n"
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()
}
