//! OpenTelemetry traces of runs, blocks and API requests (ADR-0011), sent over OTLP/HTTP
//! as JSON to the collector the standard variables name:
//!
//! - `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (the URL as is), else `OTEL_EXPORTER_OTLP_ENDPOINT`
//!   with `/v1/traces` appended. Neither set, or `OTEL_SDK_DISABLED=true`: tracing is off and
//!   a span costs one check.
//! - `OTEL_EXPORTER_OTLP_HEADERS` / `OTEL_EXPORTER_OTLP_TRACES_HEADERS`: `key=value,...`.
//! - `OTEL_SERVICE_NAME` (else the service's name) and `OTEL_RESOURCE_ATTRIBUTES`.
//! - `OTEL_TRACES_SAMPLER` (`parentbased_always_on` by default; `always_on`, `always_off`,
//!   `traceidratio`, `parentbased_always_off`, `parentbased_traceidratio`) with
//!   `OTEL_TRACES_SAMPLER_ARG` for the ratio.
//!
//! Spans go to a bounded queue a thread drains, posting batches; a slow or absent collector
//! drops spans (counted on /metrics), never delays a run. The W3C `traceparent` of a span
//! goes to block processes as `TRACEPARENT`, so instrumented block code joins the trace.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// Spans waiting to be sent; past it, new spans are dropped.
const QUEUE: usize = 8192;
/// Spans per request, and the longest a span waits for its batch.
const BATCH: usize = 512;
const BATCH_WAIT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// A span's place in a trace, as a `traceparent` carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub sampled: bool,
}

impl SpanContext {
    /// A W3C `traceparent` (version 00): `00-<trace id>-<span id>-<flags>`.
    pub fn parse(value: &str) -> Option<SpanContext> {
        let mut parts = value.trim().split('-');
        let version = parts.next()?;
        let (trace, span, flags) = (parts.next()?, parts.next()?, parts.next()?);
        // A later version may add fields after these four; version ff is invalid.
        if version.len() != 2 || version == "ff" || (version == "00" && parts.next().is_some()) {
            return None;
        }
        let trace_id: [u8; 16] = hex_bytes(trace)?;
        let span_id: [u8; 8] = hex_bytes(span)?;
        let [flags]: [u8; 1] = hex_bytes(flags)?;
        if trace_id == [0; 16] || span_id == [0; 8] {
            return None;
        }
        Some(SpanContext {
            trace_id,
            span_id,
            sampled: flags & 1 == 1,
        })
    }

    pub fn traceparent(&self) -> String {
        format!(
            "00-{}-{}-{}",
            hex(&self.trace_id),
            hex(&self.span_id),
            if self.sampled { "01" } else { "00" }
        )
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn hex_bytes<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 || !text.bytes().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    // Uppercase hex is not valid in a traceparent.
    (text == text.to_ascii_lowercase()).then_some(out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Internal = 1,
    Server = 2,
}

#[derive(Debug, Clone, PartialEq)]
enum Sampler {
    AlwaysOn,
    AlwaysOff,
    Ratio(f64),
    ParentBased(Box<Sampler>),
}

impl Sampler {
    fn from_env(name: Option<&str>, arg: Option<&str>) -> Sampler {
        let ratio = || Sampler::Ratio(arg.and_then(|a| a.trim().parse().ok()).unwrap_or(1.0));
        match name.map(str::trim).unwrap_or("") {
            "always_on" => Sampler::AlwaysOn,
            "always_off" => Sampler::AlwaysOff,
            "traceidratio" => ratio(),
            "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
            "parentbased_traceidratio" => Sampler::ParentBased(Box::new(ratio())),
            _ => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        }
    }

    fn sampled(&self, trace_id: &[u8; 16], parent: Option<&SpanContext>) -> bool {
        match self {
            Sampler::AlwaysOn => true,
            Sampler::AlwaysOff => false,
            // The spec's rule: the trace id's last 8 bytes against the ratio, so every
            // service that samples by ratio keeps the same traces.
            Sampler::Ratio(ratio) => {
                let tail = u64::from_be_bytes(trace_id[8..].try_into().expect("8 bytes"));
                (tail as f64) < ratio.clamp(0.0, 1.0) * (u64::MAX as f64)
            }
            Sampler::ParentBased(root) => match parent {
                Some(parent) => parent.sampled,
                None => root.sampled(trace_id, None),
            },
        }
    }
}

/// What OTEL_EXPORTER_OTLP_ENDPOINT is when OTEL_EXPORTER_OTLP_PROTOCOL does not say: the
/// standard's HTTP for services, and gRPC beside the MageML server, whose own exporter
/// sends gRPC there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Http,
    Grpc,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub endpoint: String,
    pub headers: Vec<(String, String)>,
    pub resource: Vec<(String, String)>,
    sampler: Sampler,
}

impl Config {
    /// The configuration the environment gives, or None when tracing is off.
    pub fn from_env(service_name: &str, default_protocol: Protocol) -> Option<Config> {
        Config::from_vars(service_name, default_protocol, |name| {
            std::env::var(name).ok().filter(|v| !v.trim().is_empty())
        })
    }

    fn from_vars(
        service_name: &str,
        default_protocol: Protocol,
        var: impl Fn(&str) -> Option<String>,
    ) -> Option<Config> {
        if var("OTEL_SDK_DISABLED").is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
            || var("OTEL_TRACES_EXPORTER").is_some_and(|v| v.trim() == "none")
        {
            return None;
        }
        let endpoint = match var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
            Some(url) => url.trim().to_string(),
            // The engine speaks OTLP over HTTP; an endpoint given for gRPC (port 4317)
            // would refuse every request.
            None if var("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
                && var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
                    .or_else(|| var("OTEL_EXPORTER_OTLP_PROTOCOL"))
                    .map_or(default_protocol == Protocol::Grpc, |p| p.trim() == "grpc") =>
            {
                crate::log::warn(
                    "Traces are off: OTEL_EXPORTER_OTLP_ENDPOINT is taken as a gRPC endpoint \
                     (OTEL_EXPORTER_OTLP_PROTOCOL, or the MageML server's default), and the \
                     engine sends OTLP over HTTP. Set OTEL_EXPORTER_OTLP_TRACES_ENDPOINT to the \
                     collector's HTTP endpoint (port 4318, path /v1/traces).",
                );
                return None;
            }
            None => format!(
                "{}/v1/traces",
                var("OTEL_EXPORTER_OTLP_ENDPOINT")?
                    .trim()
                    .trim_end_matches('/')
            ),
        };
        let mut headers = Vec::new();
        for name in [
            "OTEL_EXPORTER_OTLP_HEADERS",
            "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        ] {
            headers.extend(pairs(var(name).as_deref().unwrap_or("")));
        }
        let mut resource = pairs(var("OTEL_RESOURCE_ATTRIBUTES").as_deref().unwrap_or(""));
        let name = var("OTEL_SERVICE_NAME").unwrap_or_else(|| service_name.to_string());
        resource.retain(|(k, _)| k != "service.name");
        resource.insert(0, ("service.name".into(), name));
        Some(Config {
            endpoint,
            headers,
            resource,
            sampler: Sampler::from_env(
                var("OTEL_TRACES_SAMPLER").as_deref(),
                var("OTEL_TRACES_SAMPLER_ARG").as_deref(),
            ),
        })
    }
}

/// `key=value,key2=value2`, values percent-decoded, as the OTEL variables write them.
fn pairs(text: &str) -> Vec<(String, String)> {
    text.split(',')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            let key = key.trim();
            (!key.is_empty()).then(|| (key.to_string(), percent_decode(value.trim())))
        })
        .collect()
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(byte) = text
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Debug, Clone, PartialEq)]
pub enum Attribute {
    Text(String),
    Int(i64),
    Bool(bool),
}

impl From<&str> for Attribute {
    fn from(value: &str) -> Attribute {
        Attribute::Text(value.to_string())
    }
}

impl From<String> for Attribute {
    fn from(value: String) -> Attribute {
        Attribute::Text(value)
    }
}

impl From<i64> for Attribute {
    fn from(value: i64) -> Attribute {
        Attribute::Int(value)
    }
}

impl From<u32> for Attribute {
    fn from(value: u32) -> Attribute {
        Attribute::Int(value.into())
    }
}

impl From<bool> for Attribute {
    fn from(value: bool) -> Attribute {
        Attribute::Bool(value)
    }
}

#[derive(Debug, Clone)]
struct SpanData {
    context: SpanContext,
    parent: Option<[u8; 8]>,
    name: String,
    kind: Kind,
    start: u64,
    started: Instant,
    end: u64,
    attributes: Vec<(&'static str, Attribute)>,
    error: Option<String>,
}

enum Job {
    Span(Box<SpanData>),
    Flush(std::sync::mpsc::Sender<()>),
}

/// Where spans go: a queue and the thread that sends it.
pub struct Tracer {
    config: Config,
    queue: SyncSender<Job>,
    pub counts: std::sync::Arc<Counts>,
}

/// Spans sent, not sent (the collector failed) and dropped (the queue was full).
#[derive(Default)]
pub struct Counts {
    pub sent: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
}

static TRACER: OnceLock<Option<Tracer>> = OnceLock::new();

/// Starts tracing when the environment configures it; once per process.
pub fn init(service_name: &str, default_protocol: Protocol) {
    TRACER.get_or_init(|| {
        let config = Config::from_env(service_name, default_protocol)?;
        crate::log::info(&format!(
            "Traces go to {} (OpenTelemetry, OTLP/HTTP JSON).",
            config.endpoint
        ));
        Some(Tracer::start(config))
    });
}

pub fn tracer() -> Option<&'static Tracer> {
    TRACER.get().and_then(Option::as_ref)
}

/// Sends the spans waiting, for at most `wait`: at the end of the process.
pub fn flush(wait: Duration) {
    if let Some(tracer) = tracer() {
        tracer.flush(wait);
    }
}

impl Tracer {
    pub fn start(config: Config) -> Tracer {
        let (queue, jobs) = sync_channel(QUEUE);
        let counts = std::sync::Arc::new(Counts::default());
        let tracer = Tracer {
            config: config.clone(),
            queue,
            counts: counts.clone(),
        };
        std::thread::Builder::new()
            .name("mage-traces".into())
            .spawn(move || export_loop(config, jobs, counts))
            .expect("the traces thread starts");
        tracer
    }

    pub fn flush(&self, wait: Duration) {
        let (done, flushed) = std::sync::mpsc::channel();
        if self.queue.send(Job::Flush(done)).is_ok() {
            let _ = flushed.recv_timeout(wait);
        }
    }

    fn submit(&self, span: SpanData) {
        match self.queue.try_send(Job::Span(Box::new(span))) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.counts.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn export_loop(config: Config, jobs: Receiver<Job>, counts: std::sync::Arc<Counts>) {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(REQUEST_TIMEOUT))
        .http_status_as_error(false)
        .build()
        .into();
    let mut batch: Vec<SpanData> = Vec::new();
    let mut last_warning: Option<Instant> = None;
    let mut waiting_since = Instant::now();
    loop {
        let wait = BATCH_WAIT.saturating_sub(waiting_since.elapsed());
        let mut flushed = None;
        match jobs.recv_timeout(wait) {
            Ok(Job::Span(span)) => {
                if batch.is_empty() {
                    waiting_since = Instant::now();
                }
                batch.push(*span);
                if batch.len() < BATCH {
                    continue;
                }
            }
            Ok(Job::Flush(done)) => flushed = Some(done),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                if !batch.is_empty() {
                    let _ = post(&agent, &config, &batch);
                }
                return;
            }
        }
        if !batch.is_empty() {
            match post(&agent, &config, &batch) {
                Ok(()) => {
                    counts.sent.fetch_add(batch.len() as u64, Ordering::Relaxed);
                }
                Err(error) => {
                    counts
                        .failed
                        .fetch_add(batch.len() as u64, Ordering::Relaxed);
                    // One line a minute: a collector that is down does not flood the log.
                    if last_warning.is_none_or(|at| at.elapsed() > Duration::from_secs(60)) {
                        last_warning = Some(Instant::now());
                        crate::log::warn(&format!(
                            "Traces: {} span(s) not sent to {}: {error}",
                            batch.len(),
                            config.endpoint
                        ));
                    }
                }
            }
            batch.clear();
        }
        waiting_since = Instant::now();
        if let Some(done) = flushed {
            let _ = done.send(());
        }
    }
}

fn post(agent: &ureq::Agent, config: &Config, spans: &[SpanData]) -> Result<(), String> {
    let body = serde_json::to_vec(&encode(&config.resource, spans)).map_err(|e| e.to_string())?;
    let mut request = agent
        .post(&config.endpoint)
        .header("Content-Type", "application/json");
    for (key, value) in &config.headers {
        request = request.header(key.as_str(), value.as_str());
    }
    let response = request.send(&body[..]).map_err(|e| e.to_string())?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("the collector answered HTTP {status}"))
    }
}

fn attribute_json(key: &str, value: &Attribute) -> Value {
    let value = match value {
        Attribute::Text(text) => json!({"stringValue": text}),
        // OTLP/JSON writes 64-bit integers as strings.
        Attribute::Int(n) => json!({"intValue": n.to_string()}),
        Attribute::Bool(b) => json!({"boolValue": b}),
    };
    json!({"key": key, "value": value})
}

/// An OTLP/JSON ExportTraceServiceRequest: ids in hex, times as strings of nanoseconds.
fn encode(resource: &[(String, String)], spans: &[SpanData]) -> Value {
    let spans: Vec<Value> = spans
        .iter()
        .map(|span| {
            let mut value = json!({
                "traceId": hex(&span.context.trace_id),
                "spanId": hex(&span.context.span_id),
                "name": span.name,
                "kind": span.kind as u8,
                "startTimeUnixNano": span.start.to_string(),
                "endTimeUnixNano": span.end.to_string(),
                "attributes": span.attributes.iter()
                    .map(|(k, v)| attribute_json(k, v)).collect::<Vec<_>>(),
            });
            // Unset unless it failed, as instrumentation leaves it.
            if let Some(message) = &span.error {
                value["status"] = json!({"code": 2, "message": message});
            }
            if let Some(parent) = span.parent {
                value["parentSpanId"] = json!(hex(&parent));
            }
            value
        })
        .collect();
    json!({"resourceSpans": [{
        "resource": {"attributes": resource.iter()
            .map(|(k, v)| attribute_json(k, &Attribute::Text(v.clone()))).collect::<Vec<_>>()},
        "scopeSpans": [{
            "scope": {"name": "mageml-engine", "version": env!("CARGO_PKG_VERSION")},
            "spans": spans,
        }],
    }]})
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn random_bytes<const N: usize>() -> [u8; N] {
    use ring::rand::SecureRandom;
    // Every bit random: the ratio sampler reads a trace id's last 8 bytes, where a v4
    // UUID has fixed variant bits.
    static RANDOM: OnceLock<ring::rand::SystemRandom> = OnceLock::new();
    let random = RANDOM.get_or_init(ring::rand::SystemRandom::new);
    let mut out = [0u8; N];
    while out == [0; N] {
        random.fill(&mut out).expect("the system's random source");
    }
    out
}

/// A span in progress; sent when it ends or is dropped. Without a tracer, or when the
/// sampler leaves its trace out, it records nothing.
#[derive(Default)]
pub struct Span {
    data: Option<Box<SpanData>>,
    /// Where the span sends itself: the process's tracer, or a test's.
    tracer: Option<&'static Tracer>,
}

impl Span {
    /// A span with this parent, or the root of a new trace.
    pub fn start(name: impl Into<String>, kind: Kind, parent: Option<SpanContext>) -> Span {
        Span::start_in(tracer(), name, kind, parent)
    }

    /// A span sent to this tracer: the process's (`start`), or a test's.
    pub(crate) fn start_in(
        tracer: Option<&'static Tracer>,
        name: impl Into<String>,
        kind: Kind,
        parent: Option<SpanContext>,
    ) -> Span {
        let Some(tracer) = tracer else {
            return Span::default();
        };
        let trace_id = parent.map(|p| p.trace_id).unwrap_or_else(random_bytes);
        let sampled = tracer.config.sampler.sampled(&trace_id, parent.as_ref());
        Span {
            data: Some(Box::new(SpanData {
                context: SpanContext {
                    trace_id,
                    span_id: random_bytes(),
                    sampled,
                },
                parent: parent.map(|p| p.span_id),
                name: name.into(),
                kind,
                start: now_nanos(),
                started: Instant::now(),
                end: 0,
                attributes: Vec::new(),
                error: None,
            })),
            tracer: Some(tracer),
        }
    }

    pub fn set(&mut self, key: &'static str, value: impl Into<Attribute>) {
        if let Some(data) = &mut self.data {
            data.attributes.push((key, value.into()));
        }
    }

    pub fn fail(&mut self, message: &str) {
        if let Some(data) = &mut self.data {
            data.error = Some(message.chars().take(1000).collect());
        }
    }

    pub fn context(&self) -> Option<SpanContext> {
        self.data.as_ref().map(|d| d.context)
    }

    /// The `traceparent` that block processes get as TRACEPARENT.
    pub fn traceparent(&self) -> Option<String> {
        self.context().map(|c| c.traceparent())
    }

    pub fn end(self) {}
}

impl Drop for Span {
    fn drop(&mut self) {
        let (Some(mut data), Some(tracer)) = (self.data.take(), self.tracer) else {
            return;
        };
        if !data.context.sampled {
            return;
        }
        // From the monotonic clock: a wall clock step does not make a negative duration.
        data.end = data.start + data.started.elapsed().as_nanos() as u64;
        tracer.submit(*data);
    }
}

/// A collector and a tracer that sends to it, for tests of what is traced.
#[cfg(test)]
pub(crate) mod testing {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    use super::*;

    /// A collector on a local port: each request's body, as JSON.
    pub(crate) fn collector() -> (String, std::sync::mpsc::Receiver<Value>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/v1/traces", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let _ = tx.send(serde_json::from_slice(&body).unwrap());
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                    )
                    .unwrap();
            }
        });
        (url, rx)
    }

    pub(crate) fn tracer(url: &str) -> &'static Tracer {
        let config = Config::from_vars("svc", Protocol::Http, |name| {
            (name == "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").then(|| url.to_string())
        })
        .unwrap();
        Box::leak(Box::new(Tracer::start(config)))
    }
}

#[cfg(test)]
mod tests {
    use super::testing::collector;
    use super::*;

    #[test]
    fn traceparent_round_trips_and_bad_ones_are_refused() {
        let text = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let context = SpanContext::parse(text).unwrap();
        assert!(context.sampled);
        assert_eq!(context.traceparent(), text);
        assert!(
            !SpanContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
                .unwrap()
                .sampled
        );
        for bad in [
            "",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "00-xyz92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            assert_eq!(SpanContext::parse(bad), None, "{bad}");
        }
        // A later version may carry more fields.
        assert!(
            SpanContext::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x")
                .is_some()
        );
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn the_standard_variables_configure_it() {
        assert!(Config::from_vars("svc", Protocol::Http, vars(&[])).is_none());
        let config = Config::from_vars(
            "svc",
            Protocol::Http,
            vars(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318/"),
                (
                    "OTEL_EXPORTER_OTLP_HEADERS",
                    "authorization=Bearer%20abc, x-team=data",
                ),
                ("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prod"),
            ]),
        )
        .unwrap();
        assert_eq!(config.endpoint, "http://collector:4318/v1/traces");
        assert_eq!(
            config.headers,
            [
                ("authorization".to_string(), "Bearer abc".to_string()),
                ("x-team".to_string(), "data".to_string())
            ]
        );
        assert_eq!(config.resource[0], ("service.name".into(), "svc".into()));
        assert_eq!(
            config.resource[1],
            ("deployment.environment".into(), "prod".into())
        );
        let traces = Config::from_vars(
            "svc",
            Protocol::Http,
            vars(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://a:4318"),
                ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://b:4318/custom"),
                ("OTEL_SERVICE_NAME", "orders"),
            ]),
        )
        .unwrap();
        assert_eq!(traces.endpoint, "http://b:4318/custom");
        assert_eq!(traces.resource[0].1, "orders");
        assert!(
            Config::from_vars(
                "svc",
                Protocol::Http,
                vars(&[
                    ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://a:4318"),
                    ("OTEL_SDK_DISABLED", "true"),
                ]),
            )
            .is_none()
        );
        let grpc = [
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://a:4317"),
            ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc"),
        ];
        assert!(Config::from_vars("svc", Protocol::Http, vars(&grpc)).is_none());
        let both = [
            grpc[0],
            grpc[1],
            (
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                "http://a:4318/v1/traces",
            ),
        ];
        assert!(Config::from_vars("svc", Protocol::Http, vars(&both)).is_some());
        // Beside the MageML server, the shared endpoint is its gRPC one unless said.
        let shared = [("OTEL_EXPORTER_OTLP_ENDPOINT", "http://a:4317")];
        assert!(Config::from_vars("svc", Protocol::Grpc, vars(&shared)).is_none());
        let http = [shared[0], ("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")];
        assert!(Config::from_vars("svc", Protocol::Grpc, vars(&http)).is_some());
    }

    #[test]
    fn the_sampler_follows_the_parent_and_the_ratio() {
        let parent_on =
            SpanContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let parent_off =
            SpanContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
        let default = Sampler::from_env(None, None);
        assert!(default.sampled(&[1; 16], None));
        assert!(default.sampled(&[1; 16], parent_on.as_ref()));
        assert!(!default.sampled(&[1; 16], parent_off.as_ref()));
        let half = Sampler::from_env(Some("traceidratio"), Some("0.5"));
        let kept = (0..2000)
            .filter(|_| half.sampled(&random_bytes(), None))
            .count();
        assert!((800..1200).contains(&kept), "{kept} of 2000");
        assert!(!Sampler::from_env(Some("always_off"), None).sampled(&[1; 16], parent_on.as_ref()));
    }

    #[test]
    fn spans_reach_the_collector_as_otlp_json() {
        let (url, received) = collector();
        let config = Config::from_vars(
            "svc",
            Protocol::Http,
            vars(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", url.as_str())]),
        )
        .unwrap();
        let tracer: &'static Tracer = Box::leak(Box::new(Tracer::start(config)));
        let parent = SpanContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        let mut run = Span::start_in(Some(tracer), "run p", Kind::Server, parent);
        run.set("mage.pipeline", "p");
        let mut block = Span::start_in(Some(tracer), "block b", Kind::Internal, run.context());
        block.set("mage.attempt", 2u32);
        block.fail("ValueError: bad");
        let block_id = block.context().unwrap().span_id;
        block.end();
        run.end();
        tracer.flush(Duration::from_secs(5));

        let body = received.recv_timeout(Duration::from_secs(5)).unwrap();
        let resource = &body["resourceSpans"][0];
        assert_eq!(
            resource["resource"]["attributes"][0],
            json!({"key": "service.name", "value": {"stringValue": "svc"}})
        );
        let spans = resource["scopeSpans"][0]["spans"].as_array().unwrap();
        assert_eq!(spans.len(), 2);
        let (block, run) = (&spans[0], &spans[1]);
        assert_eq!(block["name"], "block b");
        assert_eq!(run["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(block["traceId"], run["traceId"]);
        assert_eq!(run["parentSpanId"], "00f067aa0ba902b7");
        assert_eq!(block["parentSpanId"], run["spanId"]);
        assert_eq!(block["spanId"], hex(&block_id));
        assert_eq!(run["kind"], 2);
        assert_eq!(
            block["status"],
            json!({"code": 2, "message": "ValueError: bad"})
        );
        assert_eq!(
            block["attributes"][0],
            json!({"key": "mage.attempt", "value": {"intValue": "2"}})
        );
        let start: u64 = run["startTimeUnixNano"].as_str().unwrap().parse().unwrap();
        let end: u64 = run["endTimeUnixNano"].as_str().unwrap().parse().unwrap();
        assert!(end >= start && start > 0);
        assert_eq!(tracer.counts.sent.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_trace_the_parent_did_not_sample_is_not_sent() {
        let (url, received) = collector();
        let config = Config::from_vars(
            "svc",
            Protocol::Http,
            vars(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", url.as_str())]),
        )
        .unwrap();
        let tracer: &'static Tracer = Box::leak(Box::new(Tracer::start(config)));
        let parent = SpanContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
        let run = Span::start_in(Some(tracer), "run p", Kind::Server, parent);
        // Still propagated, unsampled, so block code makes the same decision.
        assert!(run.traceparent().unwrap().ends_with("-00"));
        run.end();
        tracer.flush(Duration::from_secs(5));
        assert!(received.recv_timeout(Duration::from_millis(300)).is_err());
    }

    #[test]
    fn without_a_tracer_a_span_records_nothing() {
        let mut span = Span::start_in(None, "run", Kind::Internal, None);
        span.set("k", "v");
        assert!(span.traceparent().is_none());
    }
}
