//! HTTP sink: each batch is one POST of its messages, as a JSON array or JSON lines. A batch
//! is confirmed by a 2xx answer only. 408, 429 and 5xx answers and network errors are retried
//! (the stream's sink retry policy); other answers stop the stream with the receiver's reason.
//!
//! Requests carry `Idempotency-Key: <stream>:<partition>:<first>-<last>`, the same for every
//! retry of a batch, for receivers that deduplicate. Requests never follow redirects, verify
//! TLS, and never go to loopback, private, link-local or cloud metadata addresses unless the
//! sink allows private addresses: the check runs in the client's resolver, on the addresses it
//! connects to, so a name that changes its answer cannot get around it.

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use mage_stream_ports::{
    BatchMeta, BoxFuture, ConnectorError, RecordBatch, Sink, SinkCapabilities, read_envelope,
};
use serde_json::Value;
use ureq::unversioned::resolver::{DefaultResolver, ResolvedSocketAddrs, Resolver};
use ureq::unversioned::transport::{DefaultConnector, NextTimeout};

/// A position for the idempotency key: the number alone for offsets.
fn position_text(position: &mage_stream_ports::Position) -> String {
    match position {
        mage_stream_ports::Position::Offset(offset) => offset.to_string(),
        other => other.to_string().replace(' ', ""),
    }
}

/// Whether a request may go to this address.
pub fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => public_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return public_v4(v4);
            }
            let segments = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // fc00::/7 unique local, fe80::/10 link local.
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80)
        }
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local() // 169.254.0.0/16, the cloud metadata address among them
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || a == 0
        || (a == 100 && (64..128).contains(&b))) // 100.64.0.0/10, carrier-grade NAT
}

/// The default resolver, keeping only addresses a request may go to.
#[derive(Debug)]
struct GuardedResolver {
    allow_private: bool,
    inner: DefaultResolver,
}

impl Resolver for GuardedResolver {
    fn resolve(
        &self,
        uri: &ureq::http::Uri,
        config: &ureq::config::Config,
        timeout: NextTimeout,
    ) -> Result<ResolvedSocketAddrs, ureq::Error> {
        let found = self.inner.resolve(uri, config, timeout)?;
        if self.allow_private {
            return Ok(found);
        }
        let mut kept = self.empty();
        for address in found.iter() {
            if public_address(address.ip()) {
                kept.push(*address);
            }
        }
        if kept.is_empty() {
            return Err(ureq::Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "{} resolves to a loopback, private, link-local or metadata address; set \
                     allow_private on the sink to send there",
                    uri.host().unwrap_or("the host")
                ),
            )));
        }
        Ok(kept)
    }
}

/// A guarded HTTP client for one URL, shared by the HTTP and Feast sinks.
#[derive(Clone)]
pub(crate) struct Client {
    agent: ureq::Agent,
    pub(crate) url: String,
    headers: Vec<(String, String)>,
    /// Names the sink in errors: "HTTP sink", "Feast sink".
    label: &'static str,
}

impl Client {
    pub(crate) fn from_config(
        config: &Value,
        label: &'static str,
        path: &str,
    ) -> Result<Client, ConnectorError> {
        let url = config
            .get("url")
            .and_then(Value::as_str)
            .filter(|u| !u.is_empty())
            .ok_or_else(|| ConnectorError::fatal(format!("{label} settings need url.")))?;
        if !(url.starts_with("https://") || url.starts_with("http://")) {
            return Err(ConnectorError::fatal(format!(
                "the {label}'s url must start with https:// or http://"
            )));
        }
        let headers = config
            .get("headers")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .map(|(k, v)| {
                        let value = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                        (k.clone(), value)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let timeout = config
            .get("timeout_seconds")
            .and_then(Value::as_u64)
            .unwrap_or(10);
        let allow_private = config
            .get("allow_private")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let agent_config = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(timeout.max(1))))
            .max_redirects(0)
            .http_status_as_error(false)
            .build();
        let agent = ureq::Agent::with_parts(
            agent_config,
            DefaultConnector::new(),
            GuardedResolver {
                allow_private,
                inner: DefaultResolver::default(),
            },
        );
        Ok(Client {
            agent,
            url: if path.is_empty() {
                url.to_string()
            } else {
                format!("{}{path}", url.trim_end_matches('/'))
            },
            headers,
            label,
        })
    }

    /// POSTs the body; Ok only on a 2xx answer. 408, 429 and 5xx are retryable.
    pub(crate) async fn post(
        &self,
        body: String,
        content_type: &'static str,
        key: String,
    ) -> Result<(), ConnectorError> {
        match self.send(body, content_type, key).await? {
            Ok(()) => Ok(()),
            Err((status, message)) if status == 408 || status == 429 || status >= 500 => {
                Err(ConnectorError::retryable(message))
            }
            Err((_, message)) => Err(ConnectorError::fatal(message)),
        }
    }

    /// POSTs the body: Ok(Ok) on a 2xx answer, Ok(Err((status, message))) on another answer,
    /// Err when no answer came (network errors are retryable, refused addresses are not).
    pub(crate) async fn send(
        &self,
        body: String,
        content_type: &'static str,
        key: String,
    ) -> Result<Result<(), (u16, String)>, ConnectorError> {
        let (status, answer) = self.exchange(body, content_type, key).await?;
        if (200..300).contains(&status) {
            return Ok(Ok(()));
        }
        let reason: String = answer.chars().take(500).collect();
        Ok(Err((
            status,
            format!("{}: the receiver answered {status}: {reason}", self.label),
        )))
    }

    /// POSTs the body and returns the status and the answer's body (its first 64 KiB), for
    /// sinks that read the receiver's reasons. Err when no answer came, as for `send`.
    pub(crate) async fn exchange(
        &self,
        body: String,
        content_type: &'static str,
        key: String,
    ) -> Result<(u16, String), ConnectorError> {
        let client = self.clone();
        let label = self.label;
        let answer = tokio::task::spawn_blocking(move || {
            let mut request = client
                .agent
                .post(&client.url)
                .header("Content-Type", content_type)
                .header("Idempotency-Key", &key);
            for (name, value) in &client.headers {
                request = request.header(name, value);
            }
            request.send(body).map(|mut response| {
                let status = response.status().as_u16();
                let text = response
                    .body_mut()
                    .with_config()
                    .limit(64 * 1024)
                    .read_to_string()
                    .unwrap_or_default();
                (status, text)
            })
        })
        .await
        .map_err(|e| ConnectorError::retryable(e.to_string()))?;
        match answer {
            Ok(found) => Ok(found),
            Err(ureq::Error::Io(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                Err(ConnectorError::fatal(format!("{label}: {error}")))
            }
            Err(error) => Err(ConnectorError::retryable(format!("{label}: {error}"))),
        }
    }
}

/// The idempotency key of a batch: the same for every retry of it.
pub(crate) fn batch_key(stream: &str, meta: &BatchMeta) -> String {
    format!(
        "{stream}:{}:{}-{}",
        meta.id.partition.0,
        position_text(&meta.first),
        position_text(&meta.last)
    )
}

/// A message's value as JSON text: as it is when it is JSON, as a JSON string otherwise.
pub(crate) fn raw_json(value: &[u8]) -> String {
    match serde_json::from_slice::<&serde_json::value::RawValue>(value) {
        Ok(raw) => raw.get().to_string(),
        Err(_) => Value::String(String::from_utf8_lossy(value).into_owned()).to_string(),
    }
}

pub struct HttpSink {
    client: Client,
    lines: bool,
    stream: String,
}

impl HttpSink {
    /// `stream`: the stream's name, part of each request's idempotency key.
    pub fn new(config: &Value, stream: &str) -> Result<HttpSink, ConnectorError> {
        Ok(HttpSink {
            client: Client::from_config(config, "HTTP sink", "")?,
            lines: config.get("format").and_then(Value::as_str) == Some("ndjson"),
            stream: stream.to_string(),
        })
    }

    fn body(&self, batch: &RecordBatch) -> Result<String, ConnectorError> {
        let messages = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let values: Vec<String> = messages.iter().map(|m| raw_json(&m.value)).collect();
        Ok(if self.lines {
            values.iter().map(|v| format!("{v}\n")).collect()
        } else {
            format!("[{}]", values.join(","))
        })
    }
}

impl Sink for HttpSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let body = self.body(batch)?;
            let content_type = if self.lines {
                "application/x-ndjson"
            } else {
                "application/json"
            };
            self.client
                .post(body, content_type, batch_key(&self.stream, meta))
                .await
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: false,
            transactional: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_addresses_are_refused_and_public_ones_allowed() {
        for refused in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.9",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fe80::1",
            "fd00:ec2::254",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
        ] {
            assert!(!public_address(refused.parse().unwrap()), "{refused}");
        }
        for allowed in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "151.101.1.69"] {
            assert!(public_address(allowed.parse().unwrap()), "{allowed}");
        }
    }

    #[tokio::test]
    async fn a_sink_pointed_at_the_metadata_address_stops_instead_of_sending() {
        let sink = HttpSink::new(
            &serde_json::json!({"url": "http://169.254.169.254/latest/meta-data/"}),
            "s",
        )
        .unwrap();
        let mut builder = mage_stream_ports::EnvelopeBuilder::new();
        builder.push(&mage_stream_ports::Envelope {
            value: b"{}".to_vec(),
            ..Default::default()
        });
        let batch = builder.finish().unwrap();
        let meta = BatchMeta {
            id: mage_stream_core::BatchId {
                partition: mage_stream_ports::PartitionId(0),
                epoch: mage_stream_ports::AssignmentEpoch(1),
                seq: 0,
            },
            first: mage_stream_ports::Position::Offset(0),
            last: mage_stream_ports::Position::Offset(0),
            records: 1,
            bytes: 2,
        };

        let error = sink.write(&batch, &meta).await.unwrap_err();

        assert!(!error.retryable, "{error:?}");
        assert!(error.message.contains("metadata address"), "{error:?}");
    }

    /// A receiver that answers each request with the next status, and keeps what it got.
    fn receiver(statuses: Vec<u16>) -> (String, std::sync::mpsc::Receiver<String>) {
        use std::io::{BufRead, BufReader, Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for status in statuses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut head = String::new();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line.to_ascii_lowercase().starts_with("content-length:") {
                        length = line[15..].trim().parse().unwrap();
                    }
                    if line == "\r\n" {
                        break;
                    }
                    head.push_str(&line);
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                tx.send(format!("{head}\n{}", String::from_utf8(body).unwrap()))
                    .unwrap();
                write!(stream, "HTTP/1.1 {status} X\r\ncontent-length: 2\r\n\r\nno").unwrap();
            }
        });
        (url, rx)
    }

    fn one_batch(values: &[&str]) -> (RecordBatch, BatchMeta) {
        let mut builder = mage_stream_ports::EnvelopeBuilder::new();
        for value in values {
            builder.push(&mage_stream_ports::Envelope {
                value: value.as_bytes().to_vec(),
                ..Default::default()
            });
        }
        let meta = BatchMeta {
            id: mage_stream_core::BatchId {
                partition: mage_stream_ports::PartitionId(3),
                epoch: mage_stream_ports::AssignmentEpoch(1),
                seq: 0,
            },
            first: mage_stream_ports::Position::Offset(10),
            last: mage_stream_ports::Position::Offset(11),
            records: values.len() as u32,
            bytes: 10,
        };
        (builder.finish().unwrap(), meta)
    }

    #[tokio::test]
    async fn a_batch_is_posted_exactly_and_confirmed_only_by_a_2xx() {
        let (url, requests) = receiver(vec![200, 503, 400]);
        let sink = HttpSink::new(
            &serde_json::json!({"url": url, "allow_private": true,
                                "headers": {"Authorization": "Bearer t"}}),
            "orders",
        )
        .unwrap();
        let (batch, meta) = one_batch(&[r#"{"big": 123456789012345678901234567890}"#, "text"]);

        sink.write(&batch, &meta).await.unwrap();
        let request = requests.recv().unwrap().to_ascii_lowercase();
        assert!(
            request.contains("idempotency-key: orders:3:10-11"),
            "{request}"
        );
        assert!(request.contains("authorization: bearer t"), "{request}");
        assert!(
            request.ends_with(r#"[{"big": 123456789012345678901234567890},"text"]"#),
            "{request}"
        );

        let unavailable = sink.write(&batch, &meta).await.unwrap_err();
        assert!(unavailable.retryable, "{unavailable:?}");
        let rejected = sink.write(&batch, &meta).await.unwrap_err();
        assert!(!rejected.retryable, "{rejected:?}");
        assert!(rejected.message.contains("400"), "{rejected:?}");
    }
}
