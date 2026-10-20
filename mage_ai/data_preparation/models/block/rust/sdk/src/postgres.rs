//! PostgreSQL for Rust blocks: queries into Polars frames and frames into tables, both
//! through binary COPY, the fastest path PostgreSQL has, with exact types.
//!
//! ```ignore
//! use mage::postgres::{Postgres, WritePolicy};
//! use mage::prelude::*;
//!
//! fn load_data() -> Result<DataFrame> {
//!     Postgres::connect()?.query("SELECT * FROM shop.orders")
//! }
//!
//! fn export_data(scores: DataFrame) -> Result<()> {
//!     Postgres::connect()?.write(&scores, "shop.scores", WritePolicy::Replace)?;
//!     Ok(())
//! }
//! ```
//!
//! `connect` uses the block's profile of the project's io_config.yaml
//! (`data_provider_profile` in the block's settings, `default` otherwise), which Mage
//! renders, secrets included, and hands to the block's process in memory.
//!
//! Reading: booleans, integers, floats, text, bytea, date, time, timestamp and
//! timestamptz become the matching Polars types; numeric becomes an exact Decimal
//! (Float64 when a value is NaN or infinite, or does not fit 38 digits); one-dimensional
//! arrays of those become Lists. Other types (uuid, json, interval, enums, ranges) are
//! read as their text.
//!
//! Writing: Replace and Fail create the table with the frame's types; Append loads the
//! rows into a temporary table and inserts them into the existing table by column name,
//! each cast to its column's type. Each write is one transaction.

use std::collections::HashMap;
use std::io::{Read, Write};

use anyhow::{Context as _, Result, anyhow, bail};
use polars::prelude::*;
use postgres::types::{Kind, Type};
use postgres::{Client, Config, NoTls};

const PG_EPOCH_DAYS: i64 = 10_957; // 2000-01-01 - 1970-01-01
const PG_EPOCH_MICROS: i64 = PG_EPOCH_DAYS * 86_400_000_000;
const COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// What `write` does when the table exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePolicy {
    /// Drop the table and create it again with the frame's types.
    Replace,
    /// Insert the rows into the existing table, by column name; create it if missing.
    Append,
    /// Fail if the table exists.
    Fail,
}

pub struct Postgres {
    client: Client,
    schema: Option<String>,
}

fn settings_from_environment() -> Result<HashMap<String, String>> {
    let text = std::env::var("MAGE_IO_CONFIG").map_err(|_| {
        anyhow!(
            "The block has no io_config profile: set data_provider_profile in its \
             settings, or use Postgres::connect_url"
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&text).context("MAGE_IO_CONFIG")?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("MAGE_IO_CONFIG is not an object"))?;
    Ok(object
        .iter()
        .filter_map(|(key, value)| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(text) => Some((key.clone(), text.clone())),
            other => Some((key.clone(), other.to_string())),
        })
        .collect())
}

/// A schema or table name as written in SQL: plain names stay unquoted (and fold to lower
/// case), others are quoted, as Mage's Python client writes them.
pub fn identifier(name: &str) -> String {
    if name.len() >= 2 && name.starts_with('"') && name.ends_with('"') {
        return name.to_string();
    }
    let mut chars = name.chars();
    let plain = matches!(chars.next(), Some(c) if c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_' || c == '$');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn column_name(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl Postgres {
    /// Connects with the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&settings_from_environment()?)
    }

    /// Connects with POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DBNAME, POSTGRES_USER,
    /// POSTGRES_PASSWORD, POSTGRES_SCHEMA, POSTGRES_CONNECT_TIMEOUT, POSTGRES_SSLMODE,
    /// POSTGRES_SSLROOTCERT and POSTGRES_OPTIONS (libpq's `options`, such as
    /// `-c search_path=analytics`). TLS modes are libpq's: see `Tls`.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(String::as_str)
                .filter(|v| !v.is_empty())
        };
        let mut config = Config::new();
        config.host(get("POSTGRES_HOST").unwrap_or("localhost"));
        if let Some(port) = get("POSTGRES_PORT") {
            config.port(
                port.parse::<u16>()
                    .context("POSTGRES_PORT is not a port number")?,
            );
        }
        if let Some(name) = get("POSTGRES_DBNAME") {
            config.dbname(name);
        }
        if let Some(user) = get("POSTGRES_USER") {
            config.user(user);
        }
        if let Some(password) = get("POSTGRES_PASSWORD") {
            config.password(password);
        }
        if let Some(seconds) = get("POSTGRES_CONNECT_TIMEOUT") {
            config.connect_timeout(std::time::Duration::from_secs(
                seconds
                    .parse::<u64>()
                    .context("POSTGRES_CONNECT_TIMEOUT is not a number")?,
            ));
        }
        if let Some(options) = get("POSTGRES_OPTIONS") {
            config.options(options);
        }
        let tls = Tls::from_settings(get("POSTGRES_SSLMODE"), get("POSTGRES_SSLROOTCERT"))?;
        Self::open(config, &tls, get("POSTGRES_SCHEMA").map(str::to_string))
    }

    /// Connects with a URL, such as postgresql://user:password@host:5432/db, or key=value
    /// settings. `sslmode` and `sslrootcert` take libpq's values, as in `Tls`.
    pub fn connect_url(url: &str) -> Result<Self> {
        let (url, mode) = take_parameter(url, "sslmode");
        let (url, root) = take_parameter(&url, "sslrootcert");
        let config = url
            .parse::<Config>()
            .context("the connection URL is not valid")?;
        let tls = Tls::from_settings(mode.as_deref(), root.as_deref())?;
        Self::open(config, &tls, None)
    }

    fn open(mut config: Config, tls: &Tls, schema: Option<String>) -> Result<Self> {
        config.application_name("mage rust block");
        config.ssl_mode(tls.mode);
        let client = match tls.check {
            None => config.connect(NoTls)?,
            Some(check) => config.connect(tls::connector(check, tls.root.as_deref())?)?,
        };
        let mut connection = Postgres { client, schema };
        // Timestamps with time zone travel in UTC, whatever the server's TimeZone.
        connection.client.batch_execute("SET TIME ZONE 'UTC'")?;
        Ok(connection)
    }

    /// The underlying rust-postgres client, for anything else.
    pub fn client(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Runs statements that return no rows.
    pub fn execute(&mut self, sql: &str) -> Result<()> {
        self.client.batch_execute(sql)?;
        Ok(())
    }

    fn full_name(&self, table: &str) -> String {
        match table.split_once('.') {
            Some((schema, name)) => format!("{}.{}", identifier(schema), identifier(name)),
            None => match &self.schema {
                Some(schema) => format!("{}.{}", identifier(schema), identifier(table)),
                None => identifier(table),
            },
        }
    }

    /// The query's rows as a frame.
    pub fn query(&mut self, sql: &str) -> Result<DataFrame> {
        let sql = sql.trim().trim_end_matches(';');
        let statement = self.client.prepare(sql).context("Preparing the query")?;
        let columns: Vec<(String, Type)> = statement
            .columns()
            .iter()
            .map(|c| (c.name().to_string(), c.type_().clone()))
            .collect();
        let mut seen = std::collections::HashSet::new();
        for (name, _) in &columns {
            if !seen.insert(name) {
                bail!("The query returns two columns named {name}; give them other names");
            }
        }
        let readers: Vec<Reader> = columns.iter().map(|(_, t)| Reader::of(t)).collect();
        let select = columns
            .iter()
            .zip(&readers)
            .map(|((name, _), reader)| {
                let quoted = format!("mage_query.{}", column_name(name));
                if matches!(reader, Reader::Text) {
                    format!("{quoted}::text")
                } else {
                    quoted
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        let copy =
            format!("COPY (SELECT {select} FROM ({sql}) AS mage_query) TO STDOUT (FORMAT binary)");
        // Decoded as it arrives: the rows are never held twice, as bytes and as columns.
        let stream = self.client.copy_out(copy.as_str())?;
        let mut builders: Vec<ColumnBuilder> = readers.iter().map(ColumnBuilder::new).collect();
        decode_copy(
            std::io::BufReader::with_capacity(1 << 20, stream),
            &mut builders,
        )?;
        let series: Vec<Column> = columns
            .iter()
            .zip(builders)
            .map(|((name, _), builder)| builder.finish(name).map(Column::from))
            .collect::<Result<_>>()?;
        Ok(DataFrame::new_infer_height(series)?)
    }

    /// Writes the frame to the table; returns the rows written.
    pub fn write(&mut self, frame: &DataFrame, table: &str, policy: WritePolicy) -> Result<u64> {
        let full = self.full_name(table);
        let writers = writers_of(frame)?;
        let mut transaction = self.client.transaction()?;
        let exists: bool = transaction
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&full])?
            .get(0);
        let definition = frame
            .columns()
            .iter()
            .zip(&writers)
            .map(|(c, w)| format!("{} {}", column_name(c.name()), w.sql_type()))
            .collect::<Vec<_>>()
            .join(", ");
        let names = frame
            .columns()
            .iter()
            .map(|c| column_name(c.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let target = if exists && policy == WritePolicy::Append {
            let staging = "mage_write_staging";
            transaction.batch_execute(&format!(
                "CREATE TEMPORARY TABLE {staging} ({definition}) ON COMMIT DROP"
            ))?;
            staging.to_string()
        } else {
            if exists {
                match policy {
                    WritePolicy::Fail => bail!("Table {full} already exists"),
                    WritePolicy::Replace => {
                        transaction.batch_execute(&format!("DROP TABLE {full}"))?
                    }
                    WritePolicy::Append => unreachable!(),
                }
            }
            if let Some((schema, _)) = full.split_once('.') {
                transaction.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))?;
            }
            transaction.batch_execute(&format!("CREATE TABLE {full} ({definition})"))?;
            full.clone()
        };
        let copy = format!("COPY {target} ({names}) FROM STDIN (FORMAT binary)");
        let mut sink = transaction.copy_in(copy.as_str())?;
        encode_copy(frame, &writers, |piece| Ok(sink.write_all(piece)?))?;
        let written = sink.finish()?;
        if target != full {
            let types: HashMap<String, String> = transaction
                .query(
                    "SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute \
                     WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
                    &[&full],
                )?
                .into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            let missing: Vec<&str> = frame
                .columns()
                .iter()
                .map(|c| c.name().as_str())
                .filter(|name| !types.contains_key(*name))
                .collect();
            if !missing.is_empty() {
                bail!(
                    "Table {full} has no column {}; append writes columns by name",
                    missing.join(", ")
                );
            }
            let casts = frame
                .columns()
                .iter()
                .map(|c| format!("{}::{}", column_name(c.name()), types[c.name().as_str()]))
                .collect::<Vec<_>>()
                .join(", ");
            transaction.batch_execute(&format!(
                "INSERT INTO {full} ({names}) SELECT {casts} FROM {target}"
            ))?;
        }
        transaction.commit()?;
        Ok(written)
    }
}

/// Each column's binary COPY writer, from its Polars type.
fn writers_of(frame: &DataFrame) -> Result<Vec<Writer>> {
    frame
        .columns()
        .iter()
        .map(|c| Writer::of(c.dtype(), c.name()))
        .collect()
}

/// Encodes the frame's rows as binary COPY data (signature, header, rows, trailer), handed
/// to `out` in pieces of about a megabyte.
fn encode_copy(
    frame: &DataFrame,
    writers: &[Writer],
    mut out: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let mut buffer = Vec::with_capacity(1 << 20);
    buffer.extend_from_slice(COPY_SIGNATURE);
    buffer.extend_from_slice(&0i32.to_be_bytes());
    buffer.extend_from_slice(&0i32.to_be_bytes());
    let columns: Vec<Series> = frame
        .columns()
        .iter()
        .zip(writers)
        .map(|(c, w)| prepared(c, w))
        .collect::<Result<_>>()?;
    let mut encoders: Vec<Encoder> = columns
        .iter()
        .zip(writers)
        .map(|(c, w)| Encoder::new(c, w))
        .collect::<Result<_>>()?;
    let field_count = (encoders.len() as i16).to_be_bytes();
    for _ in 0..frame.height() {
        buffer.extend_from_slice(&field_count);
        for (encoder, column) in encoders.iter_mut().zip(&columns) {
            encoder
                .encode_next(&mut buffer)
                .with_context(|| format!("Column {}", column.name()))?;
        }
        if buffer.len() >= 1 << 20 {
            out(&buffer)?;
            buffer.clear();
        }
    }
    buffer.extend_from_slice(&(-1i16).to_be_bytes());
    out(&buffer)
}

/// The column types binary COPY writes for this frame, as `CREATE TABLE` would declare them,
/// and the COPY data of its rows, for a client that runs the COPY itself: MageML's Python
/// client sends it in its own transaction, after creating or emptying the table.
pub fn binary_copy(frame: &DataFrame) -> Result<(Vec<String>, Vec<u8>)> {
    let writers = writers_of(frame)?;
    let types = writers.iter().map(Writer::sql_type).collect();
    let mut data = Vec::new();
    encode_copy(frame, &writers, |piece| {
        data.extend_from_slice(piece);
        Ok(())
    })?;
    Ok((types, data))
}

/// TLS as libpq does it, so a profile connects the same way from Python and Rust blocks:
///
/// - `disable`: no TLS;
/// - `allow`, `prefer` (the default): TLS when the server offers it, without checking its
///   certificate;
/// - `require`: TLS without checking the certificate, unless a root certificate is given,
///   which makes it `verify-ca`;
/// - `verify-ca`: the certificate must chain to a trusted root;
/// - `verify-full`: and name the host.
///
/// Trusted roots are Mozilla's plus the PEM file in POSTGRES_SSLROOTCERT (or PGSSLROOTCERT),
/// for a private CA such as Amazon RDS's. POSTGRES_SSLMODE falls back to PGSSLMODE.
#[derive(Debug, Clone, PartialEq)]
struct Tls {
    mode: postgres::config::SslMode,
    check: Option<tls::Check>,
    root: Option<String>,
}

impl Tls {
    fn from_settings(mode: Option<&str>, root: Option<&str>) -> Result<Tls> {
        use postgres::config::SslMode;
        use tls::Check;

        let mode = mode
            .map(str::to_string)
            .or_else(|| std::env::var("PGSSLMODE").ok().filter(|v| !v.is_empty()))
            .unwrap_or_else(|| "prefer".to_string());
        let root = root.map(str::to_string).or_else(|| {
            std::env::var("PGSSLROOTCERT")
                .ok()
                .filter(|v| !v.is_empty())
        });
        let (mode, check) = match mode.as_str() {
            "disable" => (SslMode::Disable, None),
            "allow" | "prefer" => (SslMode::Prefer, Some(Check::None)),
            "require" if root.is_some() => (SslMode::Require, Some(Check::Chain)),
            "require" => (SslMode::Require, Some(Check::None)),
            "verify-ca" => (SslMode::Require, Some(Check::Chain)),
            "verify-full" => (SslMode::Require, Some(Check::Full)),
            other => bail!(
                "sslmode {other} is not one of disable, allow, prefer, require, verify-ca, \
                 verify-full"
            ),
        };
        Ok(Tls { mode, check, root })
    }
}

/// The URL without `name`, and its value: `?name=v` or `&name=v` in a URL, `name=v` in
/// key=value settings.
fn take_parameter(url: &str, name: &str) -> (String, Option<String>) {
    let key = format!("{name}=");
    let is_url = url.contains("://");
    let separators: &[char] = if is_url { &['?', '&'] } else { &[' '] };
    let mut found = None;
    let mut position = 0;
    while let Some(offset) = url[position..].find(&key) {
        let start = position + offset;
        if start == 0 && !is_url
            || start > 0 && separators.contains(&url[..start].chars().last().unwrap())
        {
            found = Some(start);
            break;
        }
        position = start + key.len();
    }
    let Some(start) = found else {
        return (url.to_string(), None);
    };
    let value_start = start + key.len();
    let end = url[value_start..]
        .find(if is_url { '&' } else { ' ' })
        .map_or(url.len(), |e| value_start + e);
    let value = url[value_start..end].to_string();
    let (before, after) = (&url[..start], url.get(end + 1..).unwrap_or(""));
    let rest = if after.is_empty() {
        before.trim_end_matches(['?', '&', ' ']).to_string()
    } else {
        format!("{before}{after}")
    };
    (rest, Some(value))
}

mod tls {
    use std::sync::Arc;

    use anyhow::{Context as _, Result};
    use rustls::client::WebPkiServerVerifier;
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{CertificateError, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

    /// What of the server's certificate is checked.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum Check {
        None,
        Chain,
        Full,
    }

    pub fn connector(
        check: Check,
        root_file: Option<&str>,
    ) -> Result<tokio_postgres_rustls::MakeRustlsConnect> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        if let Some(path) = root_file {
            let certificates = CertificateDer::pem_file_iter(path)
                .with_context(|| format!("could not read the root certificates in {path}"))?;
            for certificate in certificates {
                let certificate =
                    certificate.with_context(|| format!("{path} is not a PEM certificate"))?;
                roots
                    .add(certificate)
                    .with_context(|| format!("{path} holds a certificate rustls can't use"))?;
            }
        }
        let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?;
        let config = if check == Check::Full {
            builder.with_root_certificates(roots).with_no_client_auth()
        } else {
            let webpki =
                WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                    .build()?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Verifier {
                    check,
                    webpki,
                    provider,
                }))
                .with_no_client_auth()
        };
        Ok(tokio_postgres_rustls::MakeRustlsConnect::new(config))
    }

    /// `require` and `verify-ca`: the handshake's signatures are always verified, the
    /// certificate's chain only for `verify-ca`, its name never.
    #[derive(Debug)]
    struct Verifier {
        check: Check,
        webpki: Arc<WebPkiServerVerifier>,
        provider: Arc<CryptoProvider>,
    }

    impl ServerCertVerifier for Verifier {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp_response: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            if self.check == Check::None {
                return Ok(ServerCertVerified::assertion());
            }
            match self.webpki.verify_server_cert(
                end_entity,
                intermediates,
                server_name,
                ocsp_response,
                now,
            ) {
                Err(Error::InvalidCertificate(
                    CertificateError::NotValidForName
                    | CertificateError::NotValidForNameContext { .. },
                )) => Ok(ServerCertVerified::assertion()),
                other => other,
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            certificate: &CertificateDer<'_>,
            signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(
                message,
                certificate,
                signature,
                &self.provider.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            certificate: &CertificateDer<'_>,
            signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(
                message,
                certificate,
                signature,
                &self.provider.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.provider
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}

// ---- reading --------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Reader {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Numeric,
    String,
    Bytes,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Array(Box<Reader>),
    /// Read as the type's text output (the query casts the column to text).
    Text,
}

impl Reader {
    fn of(pg: &Type) -> Reader {
        match *pg {
            Type::BOOL => Reader::Bool,
            Type::INT2 => Reader::Int2,
            Type::INT4 => Reader::Int4,
            Type::INT8 => Reader::Int8,
            Type::FLOAT4 => Reader::Float4,
            Type::FLOAT8 => Reader::Float8,
            Type::NUMERIC => Reader::Numeric,
            Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => Reader::String,
            Type::BYTEA => Reader::Bytes,
            Type::DATE => Reader::Date,
            Type::TIME => Reader::Time,
            Type::TIMESTAMP => Reader::Timestamp,
            Type::TIMESTAMPTZ => Reader::TimestampTz,
            _ => match pg.kind() {
                Kind::Array(element) => match Reader::of(element) {
                    Reader::Text | Reader::Array(_) | Reader::Numeric => Reader::Text,
                    scalar => Reader::Array(Box::new(scalar)),
                },
                _ => Reader::Text,
            },
        }
    }
}

enum ColumnBuilder {
    Bool(Vec<Option<bool>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Numeric(Vec<Option<NumericValue>>),
    String(Vec<Option<String>>),
    Bytes(Vec<Option<Vec<u8>>>),
    Date(Vec<Option<i32>>),
    Time(Vec<Option<i64>>),
    Timestamp(Vec<Option<i64>>, bool),
    List(Reader, Vec<Option<Series>>),
}

#[derive(Debug, Clone, PartialEq)]
enum NumericValue {
    Finite {
        unscaled: i128,
        scale: u32,
    },
    /// NaN, Infinity or a value past 38 digits, as PostgreSQL writes it.
    Text(String),
}

impl ColumnBuilder {
    fn new(reader: &Reader) -> Self {
        match reader {
            Reader::Bool => Self::Bool(vec![]),
            Reader::Int2 => Self::I16(vec![]),
            Reader::Int4 => Self::I32(vec![]),
            Reader::Int8 => Self::I64(vec![]),
            Reader::Float4 => Self::F32(vec![]),
            Reader::Float8 => Self::F64(vec![]),
            Reader::Numeric => Self::Numeric(vec![]),
            Reader::String | Reader::Text => Self::String(vec![]),
            Reader::Bytes => Self::Bytes(vec![]),
            Reader::Date => Self::Date(vec![]),
            Reader::Time => Self::Time(vec![]),
            Reader::Timestamp => Self::Timestamp(vec![], false),
            Reader::TimestampTz => Self::Timestamp(vec![], true),
            Reader::Array(element) => Self::List((**element).clone(), vec![]),
        }
    }

    fn push(&mut self, bytes: Option<&[u8]>) -> Result<()> {
        match self {
            Self::Bool(v) => v.push(bytes.map(|b| b[0] != 0)),
            Self::I16(v) => v.push(bytes.map(|b| i16::from_be_bytes(b.try_into().unwrap()))),
            Self::I32(v) => v.push(bytes.map(|b| i32::from_be_bytes(b.try_into().unwrap()))),
            Self::I64(v) => v.push(bytes.map(|b| i64::from_be_bytes(b.try_into().unwrap()))),
            Self::F32(v) => v.push(bytes.map(|b| f32::from_be_bytes(b.try_into().unwrap()))),
            Self::F64(v) => v.push(bytes.map(|b| f64::from_be_bytes(b.try_into().unwrap()))),
            Self::Numeric(v) => v.push(bytes.map(decode_numeric).transpose()?),
            Self::String(v) => v.push(
                bytes
                    .map(|b| String::from_utf8(b.to_vec()).context("Text is not UTF-8"))
                    .transpose()?,
            ),
            Self::Bytes(v) => v.push(bytes.map(<[u8]>::to_vec)),
            Self::Date(v) => v.push(
                bytes.map(|b| i32::from_be_bytes(b.try_into().unwrap()) + PG_EPOCH_DAYS as i32),
            ),
            Self::Time(v) => {
                v.push(bytes.map(|b| i64::from_be_bytes(b.try_into().unwrap()) * 1000))
            }
            Self::Timestamp(v, _) => {
                v.push(bytes.map(|b| i64::from_be_bytes(b.try_into().unwrap()) + PG_EPOCH_MICROS))
            }
            Self::List(element, v) => v.push(bytes.map(|b| decode_array(b, element)).transpose()?),
        }
        Ok(())
    }

    fn finish(self, name: &str) -> Result<Series> {
        let name: PlSmallStr = name.into();
        Ok(match self {
            Self::Bool(v) => Series::new(name, v),
            Self::I16(v) => Series::new(name, v),
            Self::I32(v) => Series::new(name, v),
            Self::I64(v) => Series::new(name, v),
            Self::F32(v) => Series::new(name, v),
            Self::F64(v) => Series::new(name, v),
            Self::String(v) => Series::new(name, v),
            Self::Bytes(v) => Series::new(name, v),
            Self::Date(v) => Series::new(name, v).cast(&DataType::Date)?,
            Self::Time(v) => Series::new(name, v).cast(&DataType::Time)?,
            Self::Timestamp(v, zoned) => Series::new(name, v).cast(&DataType::Datetime(
                TimeUnit::Microseconds,
                zoned.then_some(TimeZone::UTC),
            ))?,
            Self::Numeric(v) => numeric_series(name, v)?,
            Self::List(element, v) => {
                // Arrays of one column can have different dimensions; empty ones have none.
                // The deepest value sets the type; empty values take it, others must match.
                let scalar = ColumnBuilder::new(&element).finish("")?.dtype().clone();
                let inner = v
                    .iter()
                    .flatten()
                    .max_by_key(|s| depth(s.dtype()))
                    .map(|s| s.dtype().clone())
                    .unwrap_or(scalar);
                let values = v
                    .into_iter()
                    .map(|value| match value {
                        Some(s) if s.dtype() != &inner => {
                            if s.is_empty() {
                                Ok(Some(s.cast(&inner)?))
                            } else {
                                bail!(
                                    "Column {name} has arrays of different dimensions; cast \
                                     it to text in the query"
                                )
                            }
                        }
                        other => Ok(other),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let series = Series::new(name.clone(), values);
                let wanted = DataType::List(Box::new(inner));
                if series.dtype() == &wanted {
                    series
                } else {
                    series.cast(&wanted)?
                }
            }
        })
    }
}

fn numeric_series(name: PlSmallStr, values: Vec<Option<NumericValue>>) -> Result<Series> {
    let exact = values
        .iter()
        .all(|v| !matches!(v, Some(NumericValue::Text(_))));
    let scale = values
        .iter()
        .filter_map(|v| match v {
            Some(NumericValue::Finite { scale, .. }) => Some(*scale),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    if exact && scale <= 38 {
        let mut unscaled = Vec::with_capacity(values.len());
        let mut fits = true;
        for value in &values {
            unscaled.push(match value {
                Some(NumericValue::Finite {
                    unscaled: u,
                    scale: s,
                }) => {
                    match 10i128
                        .checked_pow(scale - s)
                        .and_then(|factor| u.checked_mul(factor))
                    {
                        Some(v) if v.unsigned_abs() < 10u128.pow(38) => Some(v),
                        _ => {
                            fits = false;
                            None
                        }
                    }
                }
                _ => None,
            });
        }
        if fits {
            // The values are unscaled digits: a cast from integers would scale them again.
            return Ok(
                Int128Chunked::from_iter_options(name.clone(), unscaled.into_iter())
                    .into_decimal(38, scale as usize)?
                    .into_series(),
            );
        }
    }
    // Exact text when a value does not fit a Decimal: NaN, Infinity, or past 38 digits.
    let texts: Vec<Option<String>> = values
        .into_iter()
        .map(|v| {
            v.map(|v| match v {
                NumericValue::Finite { unscaled, scale } => decimal_text(unscaled, scale),
                NumericValue::Text(text) => text,
            })
        })
        .collect();
    Ok(Series::new(name, texts))
}

fn decimal_text(unscaled: i128, scale: u32) -> String {
    let negative = unscaled < 0;
    let mut digits = unscaled.unsigned_abs().to_string();
    if scale == 0 {
        return format!("{}{digits}", if negative { "-" } else { "" });
    }
    if digits.len() <= scale as usize {
        digits = format!("{}{digits}", "0".repeat(scale as usize + 1 - digits.len()));
    }
    let (integer, fraction) = digits.split_at(digits.len() - scale as usize);
    format!("{}{integer}.{fraction}", if negative { "-" } else { "" })
}

fn decode_numeric(bytes: &[u8]) -> Result<NumericValue> {
    let read = |i: usize| i16::from_be_bytes([bytes[i], bytes[i + 1]]);
    let count = read(0) as usize;
    let weight = read(2) as i32;
    let sign = u16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = read(6) as u32;
    match sign {
        0xC000 => return Ok(NumericValue::Text("NaN".into())),
        0xD000 => return Ok(NumericValue::Text("Infinity".into())),
        0xF000 => return Ok(NumericValue::Text("-Infinity".into())),
        _ => {}
    }
    // value = sum(digit_i * 10000^(weight - i)); scaled by 10^dscale to an integer.
    let mut unscaled: Option<i128> = Some(0);
    let mut digits = String::with_capacity(4 * count);
    for i in 0..count {
        let digit = read(8 + 2 * i) as i128;
        let exponent = weight - i as i32;
        digits.push_str(&format!("{digit:04}"));
        unscaled = unscaled.and_then(|acc| {
            let power = 4 * exponent + dscale as i32;
            if power >= 0 {
                10i128
                    .checked_pow(power as u32)
                    .and_then(|p| digit.checked_mul(p))
                    .and_then(|v| acc.checked_add(v))
            } else {
                // Digits below the display scale are zero in values PostgreSQL sends.
                Some(acc + digit / 10i128.pow((-power) as u32))
            }
        });
    }
    let negative = sign == 0x4000;
    Ok(match unscaled {
        Some(value) if value.unsigned_abs() < 10u128.pow(38) && dscale <= 38 => {
            NumericValue::Finite {
                unscaled: if negative { -value } else { value },
                scale: dscale,
            }
        }
        _ => {
            // The exact digits: the integer part is the first weight + 1 groups, and the
            // display scale sets the fraction's length.
            let integer_length = 4 * (weight + 1);
            let (integer, fraction) = if integer_length <= 0 {
                (
                    "0".to_string(),
                    format!("{}{digits}", "0".repeat((-integer_length) as usize)),
                )
            } else if integer_length as usize >= digits.len() {
                (
                    format!(
                        "{digits}{}",
                        "0".repeat(integer_length as usize - digits.len())
                    ),
                    String::new(),
                )
            } else {
                let (i, f) = digits.split_at(integer_length as usize);
                (i.to_string(), f.to_string())
            };
            let integer = integer.trim_start_matches('0');
            let mut fraction: String = fraction.chars().take(dscale as usize).collect();
            while fraction.len() < dscale as usize {
                fraction.push('0');
            }
            let mut text = String::new();
            if negative {
                text.push('-');
            }
            text.push_str(if integer.is_empty() { "0" } else { integer });
            if !fraction.is_empty() {
                text.push('.');
                text.push_str(&fraction);
            }
            NumericValue::Text(text)
        }
    })
}

fn decode_array(bytes: &[u8], element: &Reader) -> Result<Series> {
    let read_i32 = |i: usize| i32::from_be_bytes(bytes[i..i + 4].try_into().unwrap());
    let dimensions = read_i32(0).max(0) as usize;
    let mut builder = ColumnBuilder::new(element);
    if dimensions == 0 {
        return builder.finish("");
    }
    let lengths: Vec<usize> = (0..dimensions)
        .map(|d| read_i32(12 + 8 * d).max(0) as usize)
        .collect();
    let mut offset = 12 + 8 * dimensions;
    for _ in 0..lengths.iter().product::<usize>() {
        let size = read_i32(offset);
        offset += 4;
        if size < 0 {
            builder.push(None)?;
        } else {
            let size = size as usize;
            builder.push(Some(&bytes[offset..offset + size]))?;
            offset += size;
        }
    }
    // Row-major elements; each dimension past the first nests them in lists.
    let mut series = builder.finish("")?;
    for &length in lengths[1..].iter().rev() {
        let chunks: Vec<Series> = (0..series.len() / length.max(1))
            .map(|i| series.slice((i * length) as i64, length))
            .collect();
        series = Series::new("".into(), chunks);
    }
    Ok(series)
}

/// The depth of list nesting of a type: Int32 0, List(Int32) 1, List(List(Int32)) 2.
fn depth(dtype: &DataType) -> usize {
    match dtype {
        DataType::List(inner) => 1 + depth(inner),
        _ => 0,
    }
}

fn decode_copy(mut reader: impl Read, builders: &mut [ColumnBuilder]) -> Result<()> {
    let mut header = [0u8; 19];
    reader
        .read_exact(&mut header)
        .context("Reading the COPY header")?;
    if &header[..11] != COPY_SIGNATURE {
        bail!("PostgreSQL sent no binary COPY data");
    }
    let extension = u32::from_be_bytes(header[15..19].try_into().unwrap()) as usize;
    std::io::copy(
        &mut (&mut reader).take(extension as u64),
        &mut std::io::sink(),
    )?;
    let mut field = Vec::new();
    let mut two = [0u8; 2];
    let mut four = [0u8; 4];
    loop {
        reader.read_exact(&mut two)?;
        let fields = i16::from_be_bytes(two);
        if fields == -1 {
            return Ok(());
        }
        if fields as usize != builders.len() {
            bail!("A row has {fields} fields, not {}", builders.len());
        }
        for builder in builders.iter_mut() {
            reader.read_exact(&mut four)?;
            let size = i32::from_be_bytes(four);
            if size < 0 {
                builder.push(None)?;
            } else {
                field.resize(size as usize, 0);
                reader.read_exact(&mut field)?;
                builder.push(Some(&field))?;
            }
        }
    }
}

// ---- writing --------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Writer {
    Bool,
    Int2,
    Int4,
    Int8,
    /// UInt64, as numeric(20, 0).
    Unsigned,
    Float4,
    Float8,
    Numeric(usize, usize),
    Text,
    Bytes,
    Date,
    Time,
    Timestamp(TimeUnit, bool),
    Interval(TimeUnit),
    Array(Box<Writer>),
}

impl Writer {
    fn of(dtype: &DataType, name: &str) -> Result<Writer> {
        Ok(match dtype {
            DataType::Boolean => Writer::Bool,
            DataType::Int8 | DataType::Int16 | DataType::UInt8 => Writer::Int2,
            DataType::Int32 | DataType::UInt16 => Writer::Int4,
            DataType::Int64 | DataType::UInt32 => Writer::Int8,
            DataType::UInt64 => Writer::Unsigned,
            DataType::Float32 => Writer::Float4,
            DataType::Float64 => Writer::Float8,
            DataType::Decimal(precision, scale) => Writer::Numeric(*precision, *scale),
            DataType::String | DataType::Categorical(_, _) | DataType::Enum(_, _) => Writer::Text,
            DataType::Binary => Writer::Bytes,
            DataType::Date => Writer::Date,
            DataType::Time => Writer::Time,
            DataType::Datetime(unit, zone) => Writer::Timestamp(*unit, zone.is_some()),
            DataType::Duration(unit) => Writer::Interval(*unit),
            // Lists of lists are multidimensional arrays (rectangular ones).
            DataType::List(inner) => Writer::Array(Box::new(Writer::of(inner, name)?)),
            other => bail!(
                "Column {name} is {other}, which Rust blocks do not write to PostgreSQL; cast \
                 it, such as to a String of JSON"
            ),
        })
    }

    fn sql_type(&self) -> String {
        match self {
            Writer::Bool => "boolean".into(),
            Writer::Int2 => "smallint".into(),
            Writer::Int4 => "integer".into(),
            Writer::Int8 => "bigint".into(),
            Writer::Unsigned => "numeric(20, 0)".into(),
            Writer::Float4 => "real".into(),
            Writer::Float8 => "double precision".into(),
            Writer::Numeric(precision, scale) => format!("numeric({precision}, {scale})"),
            Writer::Text => "text".into(),
            Writer::Bytes => "bytea".into(),
            Writer::Date => "date".into(),
            Writer::Time => "time".into(),
            Writer::Timestamp(_, false) => "timestamp".into(),
            Writer::Timestamp(_, true) => "timestamptz".into(),
            Writer::Interval(_) => "interval".into(),
            // PostgreSQL array types have no dimensions: integer[] holds a matrix too.
            Writer::Array(element) => match element.as_ref() {
                Writer::Array(_) => element.sql_type(),
                scalar => format!("{}[]", scalar.sql_type()),
            },
        }
    }

    fn oid(&self) -> u32 {
        match self {
            Writer::Bool => Type::BOOL.oid(),
            Writer::Int2 => Type::INT2.oid(),
            Writer::Int4 => Type::INT4.oid(),
            Writer::Int8 => Type::INT8.oid(),
            Writer::Unsigned | Writer::Numeric(_, _) => Type::NUMERIC.oid(),
            Writer::Float4 => Type::FLOAT4.oid(),
            Writer::Float8 => Type::FLOAT8.oid(),
            Writer::Text => Type::TEXT.oid(),
            Writer::Bytes => Type::BYTEA.oid(),
            Writer::Date => Type::DATE.oid(),
            Writer::Time => Type::TIME.oid(),
            Writer::Timestamp(_, false) => Type::TIMESTAMP.oid(),
            Writer::Timestamp(_, true) => Type::TIMESTAMPTZ.oid(),
            Writer::Interval(_) => Type::INTERVAL.oid(),
            Writer::Array(element) => element.oid(),
        }
    }
}

fn micros(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Nanoseconds => value.div_euclid(1000),
        TimeUnit::Microseconds => value,
        TimeUnit::Milliseconds => value * 1000,
    }
}

fn encode_numeric(unscaled: i128, scale: usize, out: &mut Vec<u8>) {
    // The decimal digits split at the scale, then grouped in base 10000 from the point:
    // no arithmetic on the value, so 38 digits do not overflow.
    let negative = unscaled < 0;
    let mut digits = unscaled.unsigned_abs().to_string();
    if digits.len() <= scale {
        digits = format!("{}{digits}", "0".repeat(scale + 1 - digits.len()));
    }
    let (integer, fraction) = digits.split_at(digits.len() - scale);
    let integer = format!("{}{integer}", "0".repeat((4 - integer.len() % 4) % 4));
    let fraction = format!("{fraction}{}", "0".repeat((4 - fraction.len() % 4) % 4));
    let group = |text: &str| -> Vec<i16> {
        text.as_bytes()
            .chunks(4)
            .map(|c| std::str::from_utf8(c).unwrap().parse::<i16>().unwrap())
            .collect()
    };
    let integer_groups = group(&integer);
    let mut groups = integer_groups.clone();
    groups.extend(group(&fraction));
    let mut weight = integer_groups.len() as i32 - 1;
    // Leading and trailing zero groups are not stored.
    let first = groups.iter().position(|g| *g != 0).unwrap_or(groups.len());
    let last = groups
        .iter()
        .rposition(|g| *g != 0)
        .map(|i| i + 1)
        .unwrap_or(first);
    let stored = &groups[first..last];
    weight -= first as i32;
    if stored.is_empty() {
        weight = 0;
    }
    let mut body = Vec::with_capacity(8 + 2 * stored.len());
    body.extend_from_slice(&(stored.len() as i16).to_be_bytes());
    body.extend_from_slice(&(weight as i16).to_be_bytes());
    body.extend_from_slice(
        &(if negative && !stored.is_empty() {
            0x4000u16
        } else {
            0
        })
        .to_be_bytes(),
    );
    body.extend_from_slice(&(scale as i16).to_be_bytes());
    for digit in stored {
        body.extend_from_slice(&digit.to_be_bytes());
    }
    out.extend_from_slice(&(body.len() as i32).to_be_bytes());
    out.extend_from_slice(&body);
}

fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn null(out: &mut Vec<u8>) {
    out.extend_from_slice(&(-1i32).to_be_bytes());
}

type Values<'a, T> = Box<dyn Iterator<Item = Option<T>> + 'a>;

/// A column's values, typed, encoded one row at a time into binary COPY fields.
enum Encoder<'a> {
    Bool(Values<'a, bool>),
    I16(Values<'a, i16>),
    I32(Values<'a, i32>),
    I64(Values<'a, i64>),
    Unsigned(Values<'a, u64>),
    F32(Values<'a, f32>),
    F64(Values<'a, f64>),
    Numeric(usize, Values<'a, i128>),
    Text(Values<'a, &'a str>),
    Bytes(Values<'a, &'a [u8]>),
    Date(Values<'a, i32>),
    Time(Values<'a, i64>),
    Timestamp(TimeUnit, Values<'a, i64>),
    Interval(TimeUnit, Values<'a, i64>),
    Array(Writer, Values<'a, Series>),
}

/// The column cast to the type its encoder reads; kept alive while the encoders run.
fn prepared(column: &Column, writer: &Writer) -> Result<Series> {
    let series = column.as_materialized_series();
    // The exact type each encoder reads, whatever the frame holds (list elements can come
    // back from storage as their physical type).
    let wanted = match writer {
        Writer::Bool => DataType::Boolean,
        Writer::Int2 => DataType::Int16,
        Writer::Int4 => DataType::Int32,
        Writer::Int8 => DataType::Int64,
        Writer::Unsigned => DataType::UInt64,
        Writer::Float4 => DataType::Float32,
        Writer::Float8 => DataType::Float64,
        Writer::Numeric(precision, scale) => DataType::Decimal(*precision, *scale),
        Writer::Text => DataType::String,
        Writer::Bytes => DataType::Binary,
        Writer::Date => DataType::Date,
        Writer::Time => DataType::Time,
        Writer::Timestamp(unit, zoned) => {
            if *zoned && matches!(series.dtype(), DataType::Datetime(_, Some(_))) {
                return Ok(series.clone());
            }
            DataType::Datetime(*unit, zoned.then_some(TimeZone::UTC))
        }
        Writer::Interval(unit) => DataType::Duration(*unit),
        Writer::Array(_) => return Ok(series.clone()),
    };
    if series.dtype() == &wanted {
        Ok(series.clone())
    } else {
        Ok(series.cast(&wanted)?)
    }
}

impl<'a> Encoder<'a> {
    fn new(series: &'a Series, writer: &Writer) -> Result<Self> {
        Ok(match writer {
            Writer::Bool => Encoder::Bool(Box::new(series.bool()?.iter())),
            Writer::Int2 => Encoder::I16(Box::new(series.i16()?.iter())),
            Writer::Int4 => Encoder::I32(Box::new(series.i32()?.iter())),
            Writer::Int8 => Encoder::I64(Box::new(series.i64()?.iter())),
            Writer::Unsigned => Encoder::Unsigned(Box::new(series.u64()?.iter())),
            Writer::Float4 => Encoder::F32(Box::new(series.f32()?.iter())),
            Writer::Float8 => Encoder::F64(Box::new(series.f64()?.iter())),
            Writer::Numeric(_, scale) => {
                Encoder::Numeric(*scale, Box::new(series.decimal()?.physical().iter()))
            }
            Writer::Text => Encoder::Text(Box::new(series.str()?.iter())),
            Writer::Bytes => Encoder::Bytes(Box::new(series.binary()?.iter())),
            Writer::Date => Encoder::Date(Box::new(series.date()?.physical().iter())),
            Writer::Time => Encoder::Time(Box::new(series.time()?.physical().iter())),
            Writer::Timestamp(unit, _) => {
                Encoder::Timestamp(*unit, Box::new(series.datetime()?.physical().iter()))
            }
            Writer::Interval(unit) => {
                Encoder::Interval(*unit, Box::new(series.duration()?.physical().iter()))
            }
            Writer::Array(element) => {
                let lists = series.list()?;
                Encoder::Array(
                    (**element).clone(),
                    Box::new((0..lists.len()).map(move |i| lists.get_as_series(i))),
                )
            }
        })
    }

    fn encode_next(&mut self, out: &mut Vec<u8>) -> Result<()> {
        macro_rules! next {
            ($values:expr, $encode:expr) => {
                match $values.next().flatten() {
                    None => null(out),
                    Some(value) => $encode(value),
                }
            };
        }
        match self {
            Encoder::Bool(v) => next!(v, |b: bool| field(out, &[b as u8])),
            Encoder::I16(v) => next!(v, |x: i16| field(out, &x.to_be_bytes())),
            Encoder::I32(v) => next!(v, |x: i32| field(out, &x.to_be_bytes())),
            Encoder::I64(v) => next!(v, |x: i64| field(out, &x.to_be_bytes())),
            Encoder::Unsigned(v) => next!(v, |x: u64| encode_numeric(x as i128, 0, out)),
            Encoder::F32(v) => next!(v, |x: f32| field(out, &x.to_be_bytes())),
            Encoder::F64(v) => next!(v, |x: f64| field(out, &x.to_be_bytes())),
            Encoder::Numeric(scale, v) => {
                let scale = *scale;
                next!(v, |x: i128| encode_numeric(x, scale, out))
            }
            Encoder::Text(v) => next!(v, |x: &str| field(out, x.as_bytes())),
            Encoder::Bytes(v) => next!(v, |x: &[u8]| field(out, x)),
            Encoder::Date(v) => {
                next!(v, |days: i32| field(
                    out,
                    &((days as i64 - PG_EPOCH_DAYS) as i32).to_be_bytes()
                ))
            }
            Encoder::Time(v) => next!(v, |nanos: i64| field(out, &(nanos / 1000).to_be_bytes())),
            Encoder::Timestamp(unit, v) => {
                let unit = *unit;
                next!(v, |x: i64| field(
                    out,
                    &(micros(x, unit) - PG_EPOCH_MICROS).to_be_bytes()
                ))
            }
            Encoder::Interval(unit, v) => {
                let unit = *unit;
                next!(v, |x: i64| {
                    let mut bytes = [0u8; 16];
                    bytes[..8].copy_from_slice(&micros(x, unit).to_be_bytes());
                    field(out, &bytes)
                })
            }
            Encoder::Array(element, v) => match v.next().flatten() {
                None => null(out),
                Some(items) => {
                    let element = element.clone();
                    encode_array(&element, &items, out)?
                }
            },
        }
        Ok(())
    }
}

fn encode_array(element: &Writer, items: &Series, out: &mut Vec<u8>) -> Result<()> {
    // The shape, from the nesting; every list at a level must have the same length.
    let mut dimensions = vec![items.len()];
    let mut flat = items.clone();
    let mut scalar = element;
    while let Writer::Array(inner) = scalar {
        let lists = flat.list()?;
        if lists.is_empty() {
            // An empty array: its inner dimensions are empty too.
            dimensions.push(0);
            scalar = inner;
            continue;
        }
        let length = lists.get_as_series(0).map(|s| s.len()).unwrap_or(0);
        for index in 0..lists.len() {
            match lists.get_as_series(index) {
                Some(sub) if sub.len() == length => {}
                _ => bail!("A nested list is not rectangular, which a PostgreSQL array must be"),
            }
        }
        dimensions.push(length);
        flat = flat.explode(ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        })?;
        scalar = inner;
    }
    let empty = dimensions.contains(&0);
    let mut body = Vec::with_capacity(20 + flat.len() * 8);
    body.extend_from_slice(&(if empty { 0i32 } else { dimensions.len() as i32 }).to_be_bytes());
    body.extend_from_slice(&((flat.null_count() > 0) as i32).to_be_bytes());
    body.extend_from_slice(&scalar.oid().to_be_bytes());
    if !empty {
        for dimension in &dimensions {
            body.extend_from_slice(&(*dimension as i32).to_be_bytes());
            body.extend_from_slice(&1i32.to_be_bytes());
        }
        let cast = prepared(&Column::from(flat.clone()), scalar)?;
        let mut encoder = Encoder::new(&cast, scalar)?;
        for _ in 0..flat.len() {
            encoder.encode_next(&mut body)?;
        }
    }
    field(out, &body);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_modes_are_libpq_s() {
        use postgres::config::SslMode;
        use tls::Check;

        let tls = |mode, root| Tls::from_settings(Some(mode), root).unwrap();
        assert_eq!(tls("disable", None).check, None);
        assert_eq!(tls("prefer", None).mode, SslMode::Prefer);
        assert_eq!(tls("prefer", None).check, Some(Check::None));
        assert_eq!(tls("require", None).check, Some(Check::None));
        assert_eq!(tls("require", Some("/ca.pem")).check, Some(Check::Chain));
        assert_eq!(tls("verify-ca", None).check, Some(Check::Chain));
        let full = tls("verify-full", Some("/ca.pem"));
        assert_eq!(
            (full.mode, full.check),
            (SslMode::Require, Some(Check::Full))
        );
        assert_eq!(full.root.as_deref(), Some("/ca.pem"));
        let error = Tls::from_settings(Some("verify"), None)
            .unwrap_err()
            .to_string();
        assert!(error.contains("verify-full"), "{error}");
    }

    #[test]
    fn tls_connectors_build_for_every_check() {
        for check in [tls::Check::None, tls::Check::Chain, tls::Check::Full] {
            tls::connector(check, None).unwrap();
        }
        let error = tls::connector(tls::Check::Chain, Some("/no/such/ca.pem"))
            .err()
            .unwrap();
        assert!(error.to_string().contains("/no/such/ca.pem"), "{error}");
    }

    #[test]
    fn ssl_parameters_come_out_of_urls() {
        let take = |url: &str, name| take_parameter(url, name);
        assert_eq!(
            take("postgresql://u:p@h/db?sslmode=verify-full", "sslmode"),
            ("postgresql://u:p@h/db".into(), Some("verify-full".into()))
        );
        assert_eq!(
            take(
                "postgresql://h/db?connect_timeout=5&sslmode=require&application_name=x",
                "sslmode"
            ),
            (
                "postgresql://h/db?connect_timeout=5&application_name=x".into(),
                Some("require".into())
            )
        );
        assert_eq!(
            take(
                "postgresql://h/db?sslmode=require&connect_timeout=5",
                "sslmode"
            ),
            (
                "postgresql://h/db?connect_timeout=5".into(),
                Some("require".into())
            )
        );
        assert_eq!(
            take("host=h sslmode=verify-ca dbname=db", "sslmode"),
            ("host=h dbname=db".into(), Some("verify-ca".into()))
        );
        assert_eq!(
            take("postgresql://h/db?xsslmode=1", "sslmode"),
            ("postgresql://h/db?xsslmode=1".into(), None)
        );
    }

    fn roundtrip(unscaled: i128, scale: usize) -> NumericValue {
        let mut out = Vec::new();
        encode_numeric(unscaled, scale, &mut out);
        decode_numeric(&out[4..]).unwrap()
    }

    #[test]
    fn numeric_values_encode_and_decode_exactly() {
        for (unscaled, scale) in [
            (0i128, 0usize),
            (12345, 2),
            (-12345, 2),
            (1, 10),
            (10_000_000, 0),
            (9_999_999_999_999_999_999_999_999_999_999_999_999, 10),
            (-5, 1),
            (123_456_789, 9),
        ] {
            assert_eq!(
                roundtrip(unscaled, scale),
                NumericValue::Finite {
                    unscaled,
                    scale: scale as u32
                },
                "{unscaled} scale {scale}"
            );
        }
    }

    #[test]
    fn numeric_columns_keep_their_values_and_scale() {
        let values = vec![
            Some(NumericValue::Finite {
                unscaled: 125,
                scale: 2,
            }),
            None,
            Some(NumericValue::Finite {
                unscaled: -5,
                scale: 1,
            }),
        ];
        let series = numeric_series("n".into(), values).unwrap();
        assert_eq!(series.dtype(), &DataType::Decimal(38, 2));
        let physical: Vec<Option<i128>> = series.decimal().unwrap().physical().iter().collect();
        assert_eq!(physical, vec![Some(125), None, Some(-50)]);

        let mixed = vec![
            Some(NumericValue::Finite {
                unscaled: 1,
                scale: 0,
            }),
            Some(NumericValue::Finite {
                unscaled: -5,
                scale: 3,
            }),
            Some(NumericValue::Text("NaN".into())),
        ];
        let texts = numeric_series("n".into(), mixed).unwrap();
        let values: Vec<Option<&str>> = texts.str().unwrap().iter().collect();
        assert_eq!(values, vec![Some("1"), Some("-0.005"), Some("NaN")]);
    }

    #[test]
    fn numerics_past_38_digits_keep_every_digit() {
        // 123456789012345678901234567890123456789012345.6789 as PostgreSQL sends it.
        let groups: [i16; 13] = [
            1, 2345, 6789, 123, 4567, 8901, 2345, 6789, 123, 4567, 8901, 2345, 6789,
        ];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(groups.len() as i16).to_be_bytes());
        bytes.extend_from_slice(&11i16.to_be_bytes());
        bytes.extend_from_slice(&0u16.to_be_bytes());
        bytes.extend_from_slice(&4i16.to_be_bytes());
        for group in groups {
            bytes.extend_from_slice(&group.to_be_bytes());
        }
        assert_eq!(
            decode_numeric(&bytes).unwrap(),
            NumericValue::Text("123456789012345678901234567890123456789012345.6789".into())
        );
    }

    #[test]
    fn names_are_quoted_only_when_needed() {
        assert_eq!(identifier("sales"), "sales");
        assert_eq!(identifier("MyTable"), "MyTable");
        assert_eq!(identifier("with space"), "\"with space\"");
        assert_eq!(identifier("2024_sales"), "\"2024_sales\"");
        assert_eq!(identifier("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn frame_types_map_to_postgres_types() {
        let cases = [
            (DataType::Int64, "bigint"),
            (DataType::UInt64, "numeric(20, 0)"),
            (DataType::Decimal(12, 2), "numeric(12, 2)"),
            (
                DataType::Datetime(TimeUnit::Nanoseconds, Some(TimeZone::UTC)),
                "timestamptz",
            ),
            (DataType::List(Box::new(DataType::String)), "text[]"),
        ];
        for (dtype, expected) in cases {
            assert_eq!(Writer::of(&dtype, "c").unwrap().sql_type(), expected);
        }
        let matrix = DataType::List(Box::new(DataType::List(Box::new(DataType::Int32))));
        assert_eq!(Writer::of(&matrix, "c").unwrap().sql_type(), "integer[]");
    }
}
