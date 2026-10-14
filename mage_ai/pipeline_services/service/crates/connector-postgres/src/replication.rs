//! A logical replication connection: PostgreSQL's wire protocol, which tokio-postgres does
//! not speak for replication. The connection starts with `replication=database`, logs in
//! (SCRAM-SHA-256, MD5 or a password), and `START_REPLICATION` turns it into a copy stream in
//! both directions: the server sends the log (`XLogData`) and keepalives, the client the
//! position it has handled (standby status updates).

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use mage_stream_ports::ConnectorError;
use postgres_protocol::authentication::sasl::{
    ChannelBinding, SCRAM_SHA_256, SCRAM_SHA_256_PLUS, ScramSha256,
};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::tls;

/// Microseconds from the Unix epoch to PostgreSQL's (2000-01-01).
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

trait Io: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> Io for T {}

pub struct Target<'a> {
    pub host: &'a str,
    pub port: u16,
    pub database: &'a str,
    pub user: &'a str,
    pub password: Option<&'a str>,
    pub sslmode: Option<&'a str>,
    pub sslrootcert: Option<&'a str>,
    pub connect_timeout: Duration,
}

/// What the server sent on the copy stream.
pub enum Replicated {
    /// A pgoutput message, and the log position it starts at.
    Data { start: u64, payload: Bytes },
    /// The server's log end; `reply` asks for a status update now.
    Keepalive { wal_end: u64, reply: bool },
}

/// An error the server sent, with its SQL state.
#[derive(Debug)]
pub struct ServerError {
    pub code: String,
    pub message: String,
}

impl ServerError {
    pub fn into_connector(self, context: &str) -> ConnectorError {
        let code = self.code.as_str();
        let text = format!("PostgreSQL {context}: {} ({code})", self.message);
        // 55006: the slot is held by another connection, such as the one of a stopped
        // instance that the server has not seen go yet.
        if code.starts_with("08")
            || code.starts_with("53")
            || code.starts_with("57P")
            || code == "55006"
        {
            ConnectorError::retryable(text)
        } else {
            ConnectorError::fatal(text)
        }
    }
}

pub struct Replication {
    io: Pin<Box<dyn Io>>,
    read: BytesMut,
    write: BytesMut,
    /// The hash of the server's certificate, for SCRAM channel binding over TLS.
    end_point: Option<Vec<u8>>,
}

fn lost(context: &str, error: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::retryable(format!("PostgreSQL {context}: {error}"))
}

impl Replication {
    pub async fn connect(target: &Target<'_>) -> Result<Replication, ConnectorError> {
        let label = format!("{}:{}/{}", target.host, target.port, target.database);
        let (_, check, root) = crate::tls_mode(target.sslmode, target.sslrootcert)?;
        let mode = target.sslmode.unwrap_or("prefer");
        let tcp = tokio::time::timeout(
            target.connect_timeout,
            TcpStream::connect((target.host, target.port)),
        )
        .await
        .map_err(|_| lost(&format!("connecting to {label}"), "timed out"))?
        .map_err(|e| lost(&format!("connecting to {label}"), e))?;
        let _ = tcp.set_nodelay(true);
        let mut end_point = None;
        let io: Pin<Box<dyn Io>> = if mode == "disable" {
            Box::pin(tcp)
        } else {
            let mut tcp = tcp;
            let mut request = BytesMut::new();
            frontend::ssl_request(&mut request);
            tcp.write_all(&request)
                .await
                .map_err(|e| lost(&format!("connecting to {label}"), e))?;
            let mut answer = [0u8; 1];
            tcp.read_exact(&mut answer)
                .await
                .map_err(|e| lost(&format!("connecting to {label}"), e))?;
            if answer[0] == b'S' {
                let config = tls::client_config(check, root.as_deref())
                    .map_err(|e| ConnectorError::fatal(format!("PostgreSQL TLS: {e}")))?;
                let name = rustls::pki_types::ServerName::try_from(target.host.to_string())
                    .map_err(|e| ConnectorError::fatal(format!("PostgreSQL TLS: {e}")))?;
                let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
                    .connect(name, tcp)
                    .await
                    .map_err(|e| lost(&format!("TLS with {label}"), e))?;
                end_point = stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|certificates| certificates.first())
                    .and_then(|certificate| tls_server_end_point(certificate));
                Box::pin(stream)
            } else if matches!(mode, "allow" | "prefer") {
                Box::pin(tcp)
            } else {
                return Err(ConnectorError::fatal(format!(
                    "PostgreSQL at {label} does not offer TLS, which sslmode {mode} needs"
                )));
            }
        };
        let mut connection = Replication {
            io,
            read: BytesMut::with_capacity(64 * 1024),
            write: BytesMut::new(),
            end_point,
        };
        tokio::time::timeout(target.connect_timeout, connection.start_up(target))
            .await
            .map_err(|_| lost(&format!("logging in to {label}"), "timed out"))??;
        Ok(connection)
    }

    async fn start_up(&mut self, target: &Target<'_>) -> Result<(), ConnectorError> {
        // The server writes values with the walsender's settings: ISO dates in UTC and floats
        // with every digit.
        let parameters = [
            ("user", target.user),
            ("database", target.database),
            ("replication", "database"),
            ("application_name", "mage-stream"),
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO"),
            ("TimeZone", "UTC"),
            ("extra_float_digits", "3"),
        ];
        frontend::startup_message(parameters.iter().copied(), &mut self.write)
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
        self.flush().await?;
        let password = || {
            target.password.ok_or_else(|| {
                ConnectorError::fatal("PostgreSQL asks for a password and the settings have none")
            })
        };
        let mut scram: Option<ScramSha256> = None;
        loop {
            let (tag, mut body) = self.frame().await?;
            match tag {
                b'R' => match body.get_i32() {
                    0 => {}
                    3 => {
                        frontend::password_message(password()?.as_bytes(), &mut self.write)
                            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
                        self.flush().await?;
                    }
                    5 => {
                        let mut salt = [0u8; 4];
                        body.copy_to_slice(&mut salt);
                        let hash = postgres_protocol::authentication::md5_hash(
                            target.user.as_bytes(),
                            password()?.as_bytes(),
                            salt,
                        );
                        frontend::password_message(hash.as_bytes(), &mut self.write)
                            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
                        self.flush().await?;
                    }
                    10 => {
                        let mechanisms = String::from_utf8_lossy(&body).to_string();
                        let offered = |name: &str| mechanisms.split('\0').any(|m| m == name);
                        // Over TLS, the login is bound to the server's certificate when the
                        // server offers it; a client that only says it could bind is refused
                        // as a downgrade, so without binding it says it does not.
                        let (mechanism, binding) = match &self.end_point {
                            Some(hash) if offered(SCRAM_SHA_256_PLUS) => (
                                SCRAM_SHA_256_PLUS,
                                ChannelBinding::tls_server_end_point(hash.clone()),
                            ),
                            _ if offered(SCRAM_SHA_256) => {
                                (SCRAM_SHA_256, ChannelBinding::unsupported())
                            }
                            _ => {
                                return Err(ConnectorError::fatal(format!(
                                    "PostgreSQL asks for {} and MageML logs in with SCRAM-SHA-256",
                                    mechanisms.trim_matches('\0').replace('\0', ", ")
                                )));
                            }
                        };
                        let state = ScramSha256::new(password()?.as_bytes(), binding);
                        frontend::sasl_initial_response(
                            mechanism,
                            state.message(),
                            &mut self.write,
                        )
                        .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
                        scram = Some(state);
                        self.flush().await?;
                    }
                    11 => {
                        let state = scram.as_mut().ok_or_else(|| {
                            ConnectorError::fatal("PostgreSQL: SCRAM out of order")
                        })?;
                        state
                            .update(&body)
                            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL login: {e}")))?;
                        frontend::sasl_response(state.message(), &mut self.write)
                            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
                        self.flush().await?;
                    }
                    12 => {
                        let state = scram.as_mut().ok_or_else(|| {
                            ConnectorError::fatal("PostgreSQL: SCRAM out of order")
                        })?;
                        state
                            .finish(&body)
                            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL login: {e}")))?;
                    }
                    other => {
                        return Err(ConnectorError::fatal(format!(
                            "PostgreSQL asks for a login method MageML does not know ({other})"
                        )));
                    }
                },
                b'E' => return Err(server_error(&body).into_connector("login")),
                b'Z' => return Ok(()),
                // Parameter status, the cancel key, notices.
                _ => {}
            }
        }
    }

    /// Starts reading the slot from `start` (the server starts at the slot's confirmed
    /// position when that is later).
    pub async fn start(
        &mut self,
        slot: &str,
        publication: &str,
        start: u64,
    ) -> Result<(), ConnectorError> {
        let query = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            identifier(slot),
            lsn_text(start),
            identifier(publication).replace('\'', "''"),
        );
        frontend::query(&query, &mut self.write)
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?;
        self.flush().await?;
        loop {
            let (tag, body) = self.frame().await?;
            match tag {
                b'W' => return Ok(()),
                b'E' => {
                    return Err(server_error(&body)
                        .into_connector(&format!("starting replication of slot {slot}")));
                }
                _ => {}
            }
        }
    }

    /// The next message of the copy stream.
    pub async fn next(&mut self) -> Result<Replicated, ConnectorError> {
        loop {
            let (tag, mut body) = self.frame().await?;
            match tag {
                b'd' if !body.is_empty() => match body.get_u8() {
                    b'w' if body.len() >= 24 => {
                        let start = body.get_u64();
                        body.advance(16);
                        return Ok(Replicated::Data {
                            start,
                            payload: body,
                        });
                    }
                    b'k' if body.len() >= 17 => {
                        let wal_end = body.get_u64();
                        body.advance(8);
                        return Ok(Replicated::Keepalive {
                            wal_end,
                            reply: body.get_u8() == 1,
                        });
                    }
                    _ => {}
                },
                b'E' => return Err(server_error(&body).into_connector("replication")),
                b'c' | b'C' => {
                    return Err(ConnectorError::retryable(
                        "PostgreSQL ended the replication stream",
                    ));
                }
                _ => {}
            }
        }
    }

    /// Tells the server the log through `flushed` is handled, so it may free it.
    pub async fn status(&mut self, flushed: u64) -> Result<(), ConnectorError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64)
            - POSTGRES_EPOCH_MICROS;
        let mut update = BytesMut::with_capacity(34);
        update.put_u8(b'r');
        update.put_u64(flushed);
        update.put_u64(flushed);
        update.put_u64(flushed);
        update.put_i64(now);
        update.put_u8(0);
        frontend::CopyData::new(update.freeze())
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL: {e}")))?
            .write(&mut self.write);
        self.flush().await
    }

    /// Ends the connection politely, so the slot is free at once for the next one.
    pub async fn close(mut self) {
        frontend::terminate(&mut self.write);
        let _ = self.flush().await;
        let _ = self.io.shutdown().await;
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        let data = self.write.split();
        self.io
            .write_all(&data)
            .await
            .map_err(|e| lost("writing", e))?;
        self.io.flush().await.map_err(|e| lost("writing", e))
    }

    /// A backend message: its tag and body. Safe to cancel: what was read stays buffered.
    async fn frame(&mut self) -> Result<(u8, Bytes), ConnectorError> {
        loop {
            if self.read.len() >= 5 {
                let length =
                    u32::from_be_bytes(self.read[1..5].try_into().expect("4 bytes")) as usize;
                if length < 4 {
                    return Err(ConnectorError::fatal("PostgreSQL sent a malformed message"));
                }
                if self.read.len() > length {
                    let mut frame = self.read.split_to(1 + length).freeze();
                    let tag = frame.get_u8();
                    frame.advance(4);
                    return Ok((tag, frame));
                }
                self.read.reserve(1 + length - self.read.len());
            } else {
                self.read.reserve(64 * 1024);
            }
            let read = self
                .io
                .read_buf(&mut self.read)
                .await
                .map_err(|e| lost("reading", e))?;
            if read == 0 {
                return Err(ConnectorError::retryable(
                    "PostgreSQL closed the connection",
                ));
            }
        }
    }
}

fn server_error(body: &[u8]) -> ServerError {
    let mut error = ServerError {
        code: String::new(),
        message: String::new(),
    };
    let mut detail = String::new();
    for field in body.split(|b| *b == 0) {
        let Some((kind, text)) = field.split_first() else {
            continue;
        };
        let text = String::from_utf8_lossy(text).to_string();
        match kind {
            b'C' => error.code = text,
            b'M' => error.message = text,
            b'D' => detail = text,
            _ => {}
        }
    }
    if !detail.is_empty() {
        error.message = format!("{}: {detail}", error.message);
    }
    error
}

/// RFC 5929's tls-server-end-point: the certificate's hash, with the hash of its signature
/// (SHA-256 for MD5 and SHA-1), or None for a signature without one, such as RSA-PSS.
fn tls_server_end_point(certificate: &[u8]) -> Option<Vec<u8>> {
    use ring::digest;

    let algorithm = match signature_oid(certificate)? {
        // md5WithRSA, sha1WithRSA, sha256WithRSA, ecdsa-with-SHA1, ecdsa-with-SHA256
        [
            0x2A,
            0x86,
            0x48,
            0x86,
            0xF7,
            0x0D,
            0x01,
            0x01,
            0x04 | 0x05 | 0x0B,
        ]
        | [0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x01]
        | [0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x02] => &digest::SHA256,
        [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C]
        | [0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x03] => &digest::SHA384,
        [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0D]
        | [0x2A, 0x86, 0x48, 0xCE, 0x3D, 0x04, 0x03, 0x04] => &digest::SHA512,
        _ => return None,
    };
    Some(digest::digest(algorithm, certificate).as_ref().to_vec())
}

/// The signature algorithm of a DER certificate: SEQUENCE { tbsCertificate,
/// SEQUENCE { OID, ... }, signature }.
fn signature_oid(der: &[u8]) -> Option<&[u8]> {
    // A DER element: its tag, its content, and what follows it.
    fn element(der: &[u8]) -> Option<(u8, &[u8], &[u8])> {
        let (&tag, rest) = der.split_first()?;
        let (&first, rest) = rest.split_first()?;
        let (length, rest) = if first < 0x80 {
            (first as usize, rest)
        } else {
            let count = (first & 0x7F) as usize;
            if count == 0 || count > 4 || rest.len() < count {
                return None;
            }
            let length = rest[..count]
                .iter()
                .fold(0usize, |n, b| (n << 8) | *b as usize);
            (length, &rest[count..])
        };
        (rest.len() >= length).then(|| (tag, &rest[..length], &rest[length..]))
    }
    let (0x30, certificate, _) = element(der)? else {
        return None;
    };
    let (_, _, rest) = element(certificate)?;
    let (0x30, algorithm, _) = element(rest)? else {
        return None;
    };
    let (0x06, oid, _) = element(algorithm)? else {
        return None;
    };
    Some(oid)
}

pub fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

pub fn parse_lsn(text: &str) -> Option<u64> {
    let (high, low) = text.trim().split_once('/')?;
    Some((u64::from_str_radix(high, 16).ok()? << 32) | u64::from_str_radix(low, 16).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_positions_read_and_write_as_postgres_does() {
        assert_eq!(parse_lsn("0/16B3748"), Some(0x16B3748));
        assert_eq!(parse_lsn("1A/FF"), Some((0x1A << 32) | 0xFF));
        assert_eq!(lsn_text((0x1A << 32) | 0xFF), "1A/FF");
        assert_eq!(parse_lsn("nope"), None);
    }

    #[test]
    fn certificates_name_their_signature_hash() {
        // SEQUENCE { SEQUENCE {}, SEQUENCE { OID sha384WithRSAEncryption }, BIT STRING }
        let mut der = vec![0x30, 0x13, 0x30, 0x00, 0x30, 0x0B, 0x06, 0x09];
        der.extend([0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0C]);
        der.extend([0x03, 0x02, 0x00, 0x00]);
        assert_eq!(signature_oid(&der).unwrap()[8], 0x0C);
        assert_eq!(tls_server_end_point(&der).unwrap().len(), 48);
        // RSA-PSS names its hash in parameters: no binding.
        der[16] = 0x0A;
        assert!(tls_server_end_point(&der).is_none());
        assert!(signature_oid(&[0x30, 0x05, 0x01]).is_none());
    }

    #[test]
    fn server_errors_keep_their_state_and_detail() {
        let error = server_error(b"SERROR\0C55006\0Mslot is active\0Dheld by 42\0\0");
        assert_eq!(error.code, "55006");
        assert_eq!(error.message, "slot is active: held by 42");
        assert!(error.into_connector("x").retryable);
        let error = server_error(b"C42704\0Mno slot\0\0");
        assert!(!error.into_connector("x").retryable);
    }
}
