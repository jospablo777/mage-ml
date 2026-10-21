//! PostgreSQL connections shared by the stream connector and the run history: libpq
//! connection strings with libpq's six `sslmode` values and an `sslrootcert` given as a
//! file, PEM or base64 PEM (tokio-postgres alone knows three modes).

pub mod tls;

use std::time::Duration;

use mage_stream_ports::ConnectorError;
use tokio_postgres::{Client, Config};

/// A certificate given as PEM text or base64 PEM, as a file the TLS setup reads.
pub fn root_file(value: &str) -> Result<String, ConnectorError> {
    const MARKER: &str = "-----BEGIN CERTIFICATE-----";
    if !value.contains(MARKER) && std::path::Path::new(value).exists() {
        return Ok(value.to_string());
    }
    let pem = if value.contains(MARKER) {
        value.to_string()
    } else {
        decode_base64(value)
            .and_then(|b| String::from_utf8(b).ok())
            .filter(|p| p.contains(MARKER))
            .ok_or_else(|| {
                ConnectorError::fatal(
                    "PostgreSQL sslrootcert is neither a file, PEM, nor base64 PEM.",
                )
            })?
    };
    let digest: u64 = pem.bytes().fold(0xcbf29ce484222325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
    });
    let path = std::env::temp_dir().join(format!("mage-ca-{digest:016x}.pem"));
    if !path.exists() {
        std::fs::write(&path, pem)
            .map_err(|e| ConnectorError::fatal(format!("PostgreSQL sslrootcert: {e}")))?;
    }
    Ok(path.display().to_string())
}

pub fn decode_base64(text: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut buffer, mut bits) = (0u32, 0u32);
    for c in text
        .bytes()
        .filter(|c| !c.is_ascii_whitespace() && *c != b'=')
    {
        let value = ALPHABET.iter().position(|a| *a == c)? as u32;
        buffer = (buffer << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// Connects with a libpq connection string (`postgres://...` or `key=value ...`), as the
/// pipeline service's run history does. `sslmode` takes libpq's six modes and `sslrootcert`
/// a file, PEM or base64 PEM, as the sinks do; tokio-postgres alone knows three modes. The
/// connection runs on the caller's runtime.
pub async fn connect_url(url: &str, application: &str) -> Result<Client, String> {
    let (rest, sslmode, sslrootcert) = split_tls(url);
    let mut config: Config = rest
        .parse()
        .map_err(|e| format!("the PostgreSQL connection string is not valid: {e}"))?;
    if config.get_application_name().is_none() {
        config.application_name(application);
    }
    if config.get_connect_timeout().is_none() {
        config.connect_timeout(Duration::from_secs(10));
    }
    let (mode, check, root) =
        tls_mode(sslmode.as_deref(), sslrootcert.as_deref()).map_err(|e| e.to_string())?;
    let connector = tls::connector(check, root.as_deref()).map_err(|e| format!("TLS: {e}"))?;
    config.ssl_mode(mode);
    let (client, connection) = config.connect(connector).await.map_err(|e| {
        let hosts: Vec<String> = config
            .get_hosts()
            .iter()
            .map(|h| match h {
                tokio_postgres::config::Host::Tcp(name) => name.clone(),
                #[cfg(unix)]
                tokio_postgres::config::Host::Unix(path) => path.display().to_string(),
            })
            .collect();
        // The cause too: tokio-postgres says only "error performing TLS handshake".
        let mut message = format!("connecting to PostgreSQL at {}: {e}", hosts.join(","));
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            message.push_str(&format!(": {cause}"));
            source = cause.source();
        }
        message
    })?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// The connection string without `sslmode` and `sslrootcert`, and their values.
pub fn split_tls(url: &str) -> (String, Option<String>, Option<String>) {
    let (mut sslmode, mut sslrootcert) = (None, None);
    let mut take = |key: &str, value: String| match key {
        "sslmode" => {
            sslmode = Some(value);
            true
        }
        "sslrootcert" => {
            sslrootcert = Some(value);
            true
        }
        _ => false,
    };
    let rest = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        match url.split_once('?') {
            Some((base, query)) => {
                let kept: Vec<&str> = query
                    .split('&')
                    .filter(|pair| {
                        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                        !take(key, percent_decode(value))
                    })
                    .collect();
                if kept.is_empty() {
                    base.to_string()
                } else {
                    format!("{base}?{}", kept.join("&"))
                }
            }
            None => url.to_string(),
        }
    } else {
        key_values(url)
            .into_iter()
            .filter(|(key, value, _)| !take(key, value.clone()))
            .map(|(_, _, text)| text)
            .collect::<Vec<_>>()
            .join(" ")
    };
    (rest, sslmode, sslrootcert)
}

/// The pairs of a `key=value` connection string, as libpq reads them (`key = 'a b'`, with
/// `\'` and `\\` in quotes): each key, its value, and its text as written.
fn key_values(text: &str) -> Vec<(String, String, String)> {
    let chars: Vec<char> = text.chars().collect();
    let mut pairs = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        let start = i;
        while i < chars.len() && chars[i] != '=' && !chars[i].is_whitespace() {
            i += 1;
        }
        let key: String = chars[start..i].iter().collect();
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        let mut value = String::new();
        if i < chars.len() && chars[i] == '=' {
            i += 1;
            while i < chars.len() && chars[i].is_whitespace() {
                i += 1;
            }
            if i < chars.len() && chars[i] == '\'' {
                i += 1;
                while i < chars.len() && chars[i] != '\'' {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    value.push(chars[i]);
                    i += 1;
                }
                i += 1;
            } else {
                while i < chars.len() && !chars[i].is_whitespace() {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                    }
                    value.push(chars[i]);
                    i += 1;
                }
            }
        }
        let end = i.min(chars.len());
        pairs.push((key, value, chars[start..end].iter().collect()));
    }
    pairs
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Some(byte) = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// What `sslmode` asks of the connection and of the server's certificate, and the root
/// certificate file.
pub fn tls_mode(
    sslmode: Option<&str>,
    sslrootcert: Option<&str>,
) -> Result<(tokio_postgres::config::SslMode, tls::Check, Option<String>), ConnectorError> {
    use tls::Check;
    use tokio_postgres::config::SslMode;

    let root = sslrootcert.map(root_file).transpose()?;
    let (mode, check) = match sslmode.unwrap_or("prefer") {
        "disable" => (SslMode::Disable, Check::None),
        "allow" | "prefer" => (SslMode::Prefer, Check::None),
        "require" if root.is_some() => (SslMode::Require, Check::Chain),
        "require" => (SslMode::Require, Check::None),
        "verify-ca" => (SslMode::Require, Check::Chain),
        "verify-full" => (SslMode::Require, Check::Full),
        other => {
            return Err(ConnectorError::fatal(format!(
                "PostgreSQL sslmode {other} is not one of disable, allow, prefer, require, \
                 verify-ca, verify-full"
            )));
        }
    };
    Ok((mode, check, root))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_certificates_decode() {
        assert_eq!(decode_base64("aGVsbG8=").unwrap(), b"hello");
        assert!(root_file("not a certificate").is_err());
    }

    #[test]
    fn the_tls_settings_come_out_of_a_connection_string() {
        let (rest, mode, root) =
            split_tls("postgres://u:p@db:5432/runs?sslmode=verify-full&sslrootcert=%2Fca.pem");
        assert_eq!(rest, "postgres://u:p@db:5432/runs");
        assert_eq!(mode.as_deref(), Some("verify-full"));
        assert_eq!(root.as_deref(), Some("/ca.pem"));
        let (rest, mode, _) =
            split_tls("postgresql://db/runs?application_name=x&sslmode=require&connect_timeout=3");
        assert_eq!(
            rest,
            "postgresql://db/runs?application_name=x&connect_timeout=3"
        );
        assert_eq!(mode.as_deref(), Some("require"));
        let (rest, mode, root) = split_tls("host=db dbname=runs sslmode=verify-ca user=u");
        assert_eq!(rest, "host=db dbname=runs user=u");
        assert_eq!(mode.as_deref(), Some("verify-ca"));
        assert_eq!(root, None);
        assert_eq!(split_tls("postgres://db/runs").0, "postgres://db/runs");
        let (rest, mode, root) = split_tls(
            "host=db password='a b\\'c' sslrootcert = '/My Certs/ca.pem' sslmode=verify-full",
        );
        assert_eq!(rest, "host=db password='a b\\'c'");
        assert_eq!(mode.as_deref(), Some("verify-full"));
        assert_eq!(root.as_deref(), Some("/My Certs/ca.pem"));
    }
}
