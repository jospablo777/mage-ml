//! STOMP 1.1 and 1.2 frames, and a connection's start: CONNECT, then CONNECTED or ERROR.
//!
//! A frame is a command line, header lines, an empty line, the body and a NUL. Header values
//! escape `\`, newlines and `:` (not in CONNECT and CONNECTED). A body with `content-length`
//! may hold NULs; the broker sends lone newlines between frames as heart-beats.

use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use mage_stream_ports::ConnectorError;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl Frame {
    pub fn header(&self, name: &str) -> Option<&str> {
        // The first of repeated headers counts.
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// An ERROR frame's message and body.
    pub fn error_text(&self) -> String {
        let message = self.header("message").unwrap_or("");
        let body = String::from_utf8_lossy(&self.body);
        format!("{message} {}", body.trim()).trim().to_string()
    }
}

fn escape(value: &str, out: &mut Vec<u8>) {
    for c in value.bytes() {
        match c {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b':' => out.extend_from_slice(b"\\c"),
            _ => out.push(c),
        }
    }
}

fn unescape(value: &[u8]) -> String {
    let mut out = Vec::with_capacity(value.len());
    let mut bytes = value.iter();
    while let Some(&c) = bytes.next() {
        if c != b'\\' {
            out.push(c);
            continue;
        }
        match bytes.next() {
            Some(b'n') => out.push(b'\n'),
            Some(b'r') => out.push(b'\r'),
            Some(b'c') => out.push(b':'),
            Some(b'\\') => out.push(b'\\'),
            Some(&other) => out.extend_from_slice(&[b'\\', other]),
            None => out.push(b'\\'),
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Appends a frame to `out`; `content-length` is added for the body.
pub fn encode(command: &str, headers: &[(&str, &str)], body: &[u8], out: &mut Vec<u8>) {
    let raw = command == "CONNECT";
    out.extend_from_slice(command.as_bytes());
    out.push(b'\n');
    for (name, value) in headers {
        if raw {
            out.extend_from_slice(name.as_bytes());
            out.push(b':');
            out.extend_from_slice(value.as_bytes());
        } else {
            escape(name, out);
            out.push(b':');
            escape(value, out);
        }
        out.push(b'\n');
    }
    if !body.is_empty() {
        out.extend_from_slice(format!("content-length:{}\n", body.len()).as_bytes());
    }
    out.push(b'\n');
    out.extend_from_slice(body);
    out.push(0);
}

/// The next whole frame in `buffer`, which it leaves; None until one is whole.
pub fn decode(buffer: &mut BytesMut) -> Result<Option<Frame>, ConnectorError> {
    // Heart-beats.
    while matches!(buffer.first(), Some(b'\n' | b'\r')) {
        buffer.advance(1);
    }
    // The headers end at the first empty line, LF or CRLF.
    let lf = buffer.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let crlf = buffer
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, 4));
    let Some(end) = [lf, crlf].into_iter().flatten().min_by_key(|(i, _)| *i) else {
        return Ok(None);
    };
    let (head_end, gap) = end;
    let head = &buffer[..head_end];
    let mut lines = head
        .split(|b| *b == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let command = String::from_utf8_lossy(lines.next().unwrap_or_default()).into_owned();
    let raw = command == "CONNECTED";
    let mut headers = Vec::new();
    for line in lines {
        let Some(colon) = line.iter().position(|b| *b == b':') else {
            continue;
        };
        let (name, value) = (&line[..colon], &line[colon + 1..]);
        if raw {
            headers.push((
                String::from_utf8_lossy(name).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            ));
        } else {
            headers.push((unescape(name), unescape(value)));
        }
    }
    let start = head_end + gap;
    let length = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.trim().parse::<usize>().ok());
    let body_end = match length {
        Some(length) => {
            if buffer.len() < start + length + 1 {
                return Ok(None);
            }
            if buffer[start + length] != 0 {
                return Err(ConnectorError::retryable(
                    "ActiveMQ sent a frame whose body is longer than its content-length",
                ));
            }
            start + length
        }
        None => match buffer[start..].iter().position(|b| *b == 0) {
            Some(nul) => start + nul,
            None => return Ok(None),
        },
    };
    let mut frame = buffer.split_to(body_end + 1);
    frame.truncate(body_end);
    let body = frame.split_off(start).freeze();
    Ok(Some(Frame {
        command,
        headers,
        body,
    }))
}

pub struct Settings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
}

/// A connected STOMP session: the version, and the halves of its socket.
pub struct Session {
    pub version: String,
    pub reader: Reader,
    pub writer: OwnedWriteHalf,
}

pub struct Reader {
    half: OwnedReadHalf,
    buffer: BytesMut,
}

impl Reader {
    /// The next frame; safe to cancel, as what was read stays buffered.
    pub async fn next(&mut self) -> Result<Frame, ConnectorError> {
        loop {
            if let Some(frame) = decode(&mut self.buffer)? {
                return Ok(frame);
            }
            self.buffer.reserve(64 * 1024);
            let read = self
                .half
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| ConnectorError::retryable(format!("ActiveMQ: {e}")))?;
            if read == 0 {
                return Err(ConnectorError::retryable("ActiveMQ closed the connection"));
            }
        }
    }
}

pub async fn connect(settings: &Settings) -> Result<Session, ConnectorError> {
    let label = format!("{}:{}", settings.host, settings.port);
    let stream = tokio::time::timeout(
        Duration::from_secs(15),
        TcpStream::connect((settings.host.as_str(), settings.port)),
    )
    .await
    .map_err(|_| ConnectorError::retryable(format!("ActiveMQ at {label}: connecting timed out")))?
    .map_err(|e| ConnectorError::retryable(format!("ActiveMQ at {label}: {e}")))?;
    let _ = stream.set_nodelay(true);
    let (half, mut writer) = stream.into_split();
    let mut out = Vec::new();
    encode(
        "CONNECT",
        &[
            ("accept-version", "1.1,1.2"),
            ("host", &settings.host),
            ("login", &settings.user),
            ("passcode", &settings.password),
            ("heart-beat", "0,0"),
        ],
        b"",
        &mut out,
    );
    writer
        .write_all(&out)
        .await
        .map_err(|e| ConnectorError::retryable(format!("ActiveMQ at {label}: {e}")))?;
    let mut reader = Reader {
        half,
        buffer: BytesMut::with_capacity(64 * 1024),
    };
    let frame = tokio::time::timeout(Duration::from_secs(15), reader.next())
        .await
        .map_err(|_| {
            ConnectorError::retryable(format!("ActiveMQ at {label} did not answer CONNECT"))
        })??;
    match frame.command.as_str() {
        "CONNECTED" => Ok(Session {
            version: frame.header("version").unwrap_or("1.0").to_string(),
            reader,
            writer,
        }),
        "ERROR" => Err(ConnectorError::fatal(format!(
            "ActiveMQ at {label} refused the connection: {}",
            frame.error_text()
        ))),
        other => Err(ConnectorError::retryable(format!(
            "ActiveMQ at {label} answered CONNECT with {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_with_escaped_headers_and_binary_bodies() {
        let mut out = Vec::new();
        encode(
            "SEND",
            &[("destination", "/queue/a:b"), ("note", "x\ny\\")],
            b"a\0b",
            &mut out,
        );
        assert!(out.starts_with(b"SEND\ndestination:/queue/a\\cb\nnote:x\\ny\\\\\n"));
        let mut buffer = BytesMut::from(&b"\n\r\n"[..]);
        buffer.extend_from_slice(&out);
        buffer.extend_from_slice(b"MESSAGE\nmessage-id:1\n\nhi\0");
        let frame = decode(&mut buffer).unwrap().unwrap();
        assert_eq!(frame.command, "SEND");
        assert_eq!(frame.header("destination"), Some("/queue/a:b"));
        assert_eq!(frame.header("note"), Some("x\ny\\"));
        assert_eq!(&frame.body[..], b"a\0b");
        let frame = decode(&mut buffer).unwrap().unwrap();
        assert_eq!(
            (frame.command.as_str(), &frame.body[..]),
            ("MESSAGE", &b"hi"[..])
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn partial_frames_wait_for_the_rest() {
        let mut buffer = BytesMut::from(&b"MESSAGE\ncontent-length:5\n\nab"[..]);
        assert!(decode(&mut buffer).unwrap().is_none());
        buffer.extend_from_slice(b"cde\0");
        assert_eq!(&decode(&mut buffer).unwrap().unwrap().body[..], b"abcde");
        let mut connected = BytesMut::from(&b"CONNECTED\r\nversion:1.2\r\n\r\n\0"[..]);
        let frame = decode(&mut connected).unwrap().unwrap();
        assert_eq!(frame.header("version"), Some("1.2"));
    }
}
