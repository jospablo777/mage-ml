//! IBM MQ source and sink for streams, through IBM's MQ client library (`libmqm_r`), loaded
//! at runtime: the engine builds without IBM's SDK, and only images whose streams use IBM MQ
//! carry the client.
//!
//! Source: messages are read under syncpoint and committed only once every sink took their
//! batch. A unit of work belongs to its connection, and a commit takes every message read on
//! it, so each connection is a partition with one batch in flight: the next batch is read
//! after the commit. `connections` sets how many read in parallel; with more than one, queue
//! order holds within a batch only. A connection that drops backs its messages out, so the
//! queue manager delivers them again; its batch in flight is refused at commit.
//!
//! A normal MQDISC commits the open unit of work, so every disconnect (a stop, a failure, a
//! panic in a worker thread) backs out first: nothing a sink did not confirm is committed.
//!
//! Sink: a batch's messages are put persistent under syncpoint and committed as the receipt;
//! a failure backs the batch out. Each message's MsgId is derived from
//! `<stream>:<partition>:<position>`, the same on a replay, so consumers can deduplicate.
//!
//! Settings: `queue_manager`, `channel`, `connection_name` (`host(port)`, comma separated for
//! multi-instance queue managers) or `host` and `port`, `queue`, `user`, `password`,
//! `application_name`, `connections` (source), and for TLS `cipher_spec`, `key_repository`
//! (the key database path without its extension) and `certificate_label`.

use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock, mpsc as std_mpsc};
use std::thread;
use std::time::{Duration, Instant};

use dlopen2::wrapper::Container;
use libmqm_sys::dlopen2::MqWrapper;
use mage_stream_core::BatchId;
use mage_stream_ports::{
    AckKind, AssignmentEpoch, BatchMeta, BoxFuture, Commit, CommitOutcome, ConnectorError,
    Envelope, EnvelopeBuilder, Header, PartitionId, Position, RecordBatch, Sink, SinkCapabilities,
    Source, SourceCapabilities, SourceEvent, read_envelope,
};
use mqi::connection::{Credentials, MqServer, ThreadNone, Tls};
use mqi::get::{GetConvert, GetWait};
use mqi::header::{TextEnc, fmt};
use mqi::prelude::*;
use mqi::string::CCSID;
use mqi::types::{
    ApplName, CertificateLabel, CipherSpec, KeyRepo, MessageFormat, MessageId, QueueManagerName,
    QueueName,
};
use mqi::{MqStr, Object, Syncpoint, constants, structs};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};

type Lib = &'static Container<MqWrapper>;
type Conn = mqi::Connection<Lib, ThreadNone>;

/// Where the MQ client library is, when not in MQ_HOME/lib64 or the library path.
pub const LIBRARY_VARIABLE: &str = "MAGE_SERVICE_MQ_LIBRARY";
const LIBRARY_NAME: &str = if cfg!(target_os = "macos") {
    "libmqm_r.dylib"
} else {
    "libmqm_r.so"
};
/// Messages larger than this are refused; MQ's own maximum is 100 MB.
const MAX_MESSAGE_BYTES: usize = 100 * 1024 * 1024;
const RECONNECT_MIN: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// How a source groups messages into batches.
#[derive(Debug, Clone, Copy)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_wait: Duration,
}

static LIBRARY: OnceLock<Result<Lib, String>> = OnceLock::new();

fn library_candidates() -> Vec<String> {
    if let Ok(path) = std::env::var(LIBRARY_VARIABLE) {
        return vec![path];
    }
    let mut candidates = Vec::new();
    if let Ok(home) = std::env::var("MQ_HOME") {
        candidates.push(format!(
            "{}/lib64/{LIBRARY_NAME}",
            home.trim_end_matches('/')
        ));
    }
    candidates.push(format!("/opt/mqm/lib64/{LIBRARY_NAME}"));
    candidates.push(LIBRARY_NAME.to_string());
    candidates
}

/// The MQ client library, loaded once per process.
pub fn library() -> Result<Lib, ConnectorError> {
    LIBRARY
        .get_or_init(|| {
            // The client writes its error logs there and fails when it is missing.
            if let Ok(path) = std::env::var("MQ_OVERRIDE_DATA_PATH") {
                std::fs::create_dir_all(&path)
                    .map_err(|e| format!("MQ_OVERRIDE_DATA_PATH {path}: {e}"))?;
            }
            let mut failures = Vec::new();
            for candidate in library_candidates() {
                // SAFETY: libmqm_r is IBM's MQ client; loading it runs its initializers only.
                match unsafe { Container::<MqWrapper>::load(&candidate) } {
                    Ok(library) => return Ok(&*Box::leak(Box::new(library))),
                    Err(error) => failures.push(format!("{candidate}: {error}")),
                }
            }
            Err(format!(
                "the IBM MQ client library was not found ({}). Exported images of streams that \
                 use IBM MQ include it; elsewhere install the IBM MQ client and set MQ_HOME or \
                 {LIBRARY_VARIABLE}",
                failures.join("; ")
            ))
        })
        .clone()
        .map_err(ConnectorError::fatal)
}

fn text(config: &Value, key: &str) -> Option<String> {
    match config.get(key)? {
        Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// An MQ failure, by reason code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqError {
    pub reason: i32,
    pub message: String,
}

impl MqError {
    fn new(context: &str, error: &mqi::result::Error) -> MqError {
        let reason = error.2.0;
        MqError {
            reason,
            message: format!("IBM MQ {context}: {error}, reason {reason}{}", hint(reason)),
        }
    }

    fn local(message: impl Into<String>) -> MqError {
        MqError {
            reason: 0,
            message: message.into(),
        }
    }

    /// The connection is gone or unusable; its unit of work was backed out.
    pub fn lost(&self) -> bool {
        matches!(
            self.reason,
            2003 // BACKED_OUT
                | 2009 // CONNECTION_BROKEN
                | 2018 // HCONN_ERROR
                | 2019 // HOBJ_ERROR
                | 2059 // Q_MGR_NOT_AVAILABLE
                | 2161 // Q_MGR_QUIESCING
                | 2162 // Q_MGR_STOPPING
                | 2202 // CONNECTION_QUIESCING
                | 2203 // CONNECTION_STOPPING
                | 2537 // CHANNEL_NOT_AVAILABLE
                | 2538 // HOST_NOT_AVAILABLE
                | 2548 // RECONNECT_FAILED
        )
    }

    /// Worth trying again: an outage or a full queue, not a mistake in the settings.
    pub fn retryable(&self) -> bool {
        self.lost() || self.reason == 2053 // Q_FULL
    }

    fn into_connector(self) -> ConnectorError {
        if self.retryable() {
            ConnectorError::retryable(self.message)
        } else {
            ConnectorError::fatal(self.message)
        }
    }
}

fn hint(reason: i32) -> &'static str {
    match reason {
        2035 => {
            " (not authorized: check user and password, the channel's CHLAUTH rules and the \
             queue's authorities)"
        }
        2085 => " (the queue does not exist on this queue manager)",
        2058 => " (no queue manager of that name at this connection)",
        2540 => " (no server-connection channel of that name)",
        2024 => {
            " (a batch holds more messages than the queue manager's MAXUMSGS allows in one \
             unit of work; lower batch_size or raise MAXUMSGS)"
        }
        2030 | 2031 => " (a message is longer than the queue's or queue manager's MAXMSGL)",
        2053 => " (the queue is full)",
        2393 | 2381 => {
            " (TLS could not start: check key_repository, certificate_label and that the \
             cipher_spec matches the channel's SSLCIPH)"
        }
        2059 | 2538 => " (the queue manager could not be reached)",
        _ => "",
    }
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub queue_manager: Option<String>,
    pub channel: String,
    pub connection_name: String,
    pub queue: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub application: String,
    pub cipher_spec: Option<String>,
    pub key_repository: Option<String>,
    pub certificate_label: Option<String>,
}

impl Settings {
    pub fn new(config: &Value) -> Result<Settings, ConnectorError> {
        let connection_name = match text(config, "connection_name") {
            Some(name) => name,
            None => {
                let host = text(config, "host").ok_or_else(|| {
                    ConnectorError::fatal("IBM MQ settings need connection_name or host.")
                })?;
                format!(
                    "{host}({})",
                    text(config, "port").unwrap_or_else(|| "1414".into())
                )
            }
        };
        let channel = text(config, "channel")
            .ok_or_else(|| ConnectorError::fatal("IBM MQ settings need channel."))?;
        let queue = text(config, "queue")
            .or_else(|| text(config, "queue_name"))
            .ok_or_else(|| ConnectorError::fatal("IBM MQ settings need queue."))?;
        let settings = Settings {
            queue_manager: text(config, "queue_manager"),
            channel,
            connection_name,
            queue,
            user: text(config, "user"),
            password: text(config, "password"),
            application: text(config, "application_name").unwrap_or_else(|| "mage-stream".into()),
            cipher_spec: text(config, "cipher_spec"),
            key_repository: text(config, "key_repository"),
            certificate_label: text(config, "certificate_label"),
        };
        for (name, value, limit) in [
            ("queue", Some(&settings.queue), 48),
            ("channel", Some(&settings.channel), 20),
            ("queue_manager", settings.queue_manager.as_ref(), 48),
            ("application_name", Some(&settings.application), 28),
        ] {
            if value.is_some_and(|v| v.len() > limit) {
                return Err(ConnectorError::fatal(format!(
                    "IBM MQ {name} is longer than {limit} characters."
                )));
            }
        }
        if settings.key_repository.is_some() != settings.cipher_spec.is_some() {
            return Err(ConnectorError::fatal(
                "IBM MQ TLS needs both cipher_spec and key_repository.",
            ));
        }
        Ok(settings)
    }

    /// Where the stream connects, without credentials, for messages.
    pub fn label(&self) -> String {
        format!(
            "queue {} on {} ({}{})",
            self.queue,
            self.connection_name,
            self.channel,
            self.queue_manager
                .as_ref()
                .map(|q| format!(", queue manager {q}"))
                .unwrap_or_default()
        )
    }
}

fn mq_text<const N: usize>(name: &str, value: &str) -> Result<MqStr<N>, MqError> {
    MqStr::from_str(value)
        .map_err(|_| MqError::local(format!("IBM MQ {name} {value:?} is invalid")))
}

/// A connection and its open queue. Dropping it backs out first: MQDISC would commit.
struct Session {
    qm: Rc<Conn>,
    queue: Object<Rc<Conn>>,
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = Syncpoint::new(Rc::clone(&self.qm)).backout();
    }
}

impl Session {
    fn open(settings: &Settings, input: bool) -> Result<Session, MqError> {
        let library = library().map_err(|e| MqError::local(e.to_string()))?;
        let server_text = format!("{}/TCP/{}", settings.channel, settings.connection_name);
        let server = MqServer::try_from(server_text.as_str())
            .map_err(|e| MqError::local(format!("IBM MQ connection {server_text:?}: {e}")))?;
        let application = ApplName(mq_text("application_name", &settings.application)?);
        let queue_manager = settings
            .queue_manager
            .as_deref()
            .map(|q| {
                QueueManagerName::from_str(q)
                    .map_err(|_| MqError::local(format!("IBM MQ queue_manager {q:?} is invalid")))
            })
            .transpose()?;
        let password = settings.password.clone().unwrap_or_default();
        let credentials = match &settings.user {
            Some(user) => Credentials::User(user.as_str(), password.as_str().into()),
            None => Credentials::Default,
        };
        let tls_parts = match (&settings.key_repository, &settings.cipher_spec) {
            (Some(repository), Some(cipher)) => Some((
                KeyRepo(mq_text("key_repository", repository)?),
                CipherSpec(mq_text("cipher_spec", cipher)?),
                settings
                    .certificate_label
                    .as_deref()
                    .map(|l| mq_text("certificate_label", l).map(CertificateLabel))
                    .transpose()?,
            )),
            _ => None,
        };
        let tls = tls_parts
            .as_ref()
            .map(|(repository, cipher, label)| Tls::new(repository, label.as_ref(), cipher));
        let qm = mqi::connect_lib::<ThreadNone, _>(
            library,
            &(application, tls, queue_manager, credentials, server),
        )
        .discard_warning()
        .map_err(|e| MqError::new(&format!("connecting to {}", settings.label()), &e))?;
        let qm = Rc::new(qm);
        let name = QueueName::from_str(&settings.queue)
            .map_err(|_| MqError::local(format!("IBM MQ queue {:?} is invalid", settings.queue)))?;
        let options = if input {
            constants::MQOO_INPUT_AS_Q_DEF | constants::MQOO_FAIL_IF_QUIESCING
        } else {
            constants::MQOO_OUTPUT | constants::MQOO_FAIL_IF_QUIESCING
        };
        let queue = Object::open(Rc::clone(&qm), &(name, options))
            .discard_warning()
            .map_err(|e| MqError::new(&format!("opening {}", settings.label()), &e))?;
        Ok(Session { qm, queue })
    }

    fn commit(&self) -> Result<(), MqError> {
        Syncpoint::new(Rc::clone(&self.qm))
            .commit()
            .discard_warning()
            .map(|_| ())
            .map_err(|e| MqError::new("committing", &e))
    }

    fn backout(&self) -> Result<(), MqError> {
        Syncpoint::new(Rc::clone(&self.qm))
            .backout()
            .discard_warning()
            .map(|_| ())
            .map_err(|e| MqError::new("backing out", &e))
    }
}

/// A message read from a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub data: Vec<u8>,
    pub message_id: [u8; 24],
    pub correlation_id: [u8; 24],
    pub backout_count: i32,
    /// `YYYYMMDDHHMMSSTH`, as MQ records the put, in UTC.
    pub put_time: String,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn chars(raw: &[std::ffi::c_char]) -> String {
    raw.iter()
        .map(|c| *c as u8 as char)
        .collect::<String>()
        .trim()
        .to_string()
}

/// Milliseconds since the epoch of MQ's PutDate and PutTime (`YYYYMMDD`, `HHMMSSTH`, UTC).
pub fn put_millis(put_time: &str) -> Option<i64> {
    if put_time.len() < 14 || !put_time.is_ascii() {
        return None;
    }
    let number = |range: std::ops::Range<usize>| put_time.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(4..6)?, number(6..8)?);
    let (hour, minute, second) = (number(8..10)?, number(10..12)?, number(12..14)?);
    let hundredths = number(14..16).unwrap_or(0);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || minute > 59 {
        return None;
    }
    // Days from the civil date (Howard Hinnant's algorithm).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some(((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1000) + hundredths * 10)
}

fn read_batch(session: &Session, limits: BatchLimits) -> Result<Vec<Message>, MqError> {
    let started = Instant::now();
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    let mut buffer = vec![0u8; 256 * 1024];
    while messages.len() < limits.max_records && bytes < limits.max_bytes {
        let wait = if messages.is_empty() {
            limits.max_wait
        } else {
            (started + limits.max_wait).saturating_duration_since(Instant::now())
        };
        let wait = match wait.as_millis() {
            0 => GetWait::NoWait,
            ms => GetWait::Wait(ms.min(i32::MAX as u128) as i32),
        };
        // Text is converted to UTF-8 by the queue manager; binary data is left as it is.
        let options = (
            constants::MQGMO_SYNCPOINT | constants::MQGMO_FAIL_IF_QUIESCING,
            wait,
            GetConvert::ConvertTo(CCSID(1208), constants::MQENC_NATIVE),
        );
        // The message is copied out before the buffer is touched again.
        let got = match session
            .queue
            .get_data_with::<structs::MQMD, u8>(&options, &mut buffer)
            .discard_warning()
        {
            Ok(Some((data, md))) => Ok(Some(Message {
                data: data.to_vec(),
                message_id: md.MsgId,
                correlation_id: md.CorrelId,
                backout_count: md.BackoutCount,
                put_time: format!("{}{}", chars(&md.PutDate), chars(&md.PutTime)),
            })),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        };
        match got {
            Ok(Some(message)) => {
                bytes += message.data.len();
                messages.push(message);
            }
            Ok(None) => break,
            // The message is longer than the buffer and stays on the queue: read it again with
            // a buffer that fits.
            Err(error) if error.2.0 == 2080 && buffer.len() < MAX_MESSAGE_BYTES => {
                let len = (buffer.len() * 4).min(MAX_MESSAGE_BYTES);
                buffer.resize(len, 0);
            }
            Err(error) => return Err(MqError::new("reading", &error)),
        }
    }
    Ok(messages)
}

enum Request {
    Fetch {
        epoch: u64,
        limits: BatchLimits,
    },
    Commit {
        epoch: u64,
        reply: oneshot::Sender<Result<(), MqError>>,
    },
    Close,
}

enum Report {
    Connected {
        epoch: u64,
    },
    Fetched {
        epoch: u64,
        messages: Vec<Message>,
    },
    Lost {
        epoch: u64,
        error: MqError,
    },
    /// Connecting failed; the worker tries again, or stops when `retryable` is false.
    Unavailable {
        error: MqError,
    },
    Failed {
        error: MqError,
    },
}

/// Owns one connection on its own thread: MQ calls block, and a connection stays on the thread
/// that made it.
fn source_worker(
    partition: u32,
    settings: Settings,
    requests: std_mpsc::Receiver<Request>,
    reports: mpsc::UnboundedSender<(u32, Report)>,
) {
    let send = |report| reports.send((partition, report)).is_ok();
    let mut epoch = 0u64;
    let mut delay = RECONNECT_MIN;
    loop {
        let session = match Session::open(&settings, true) {
            Ok(session) => session,
            Err(error) => {
                let retryable = error.retryable();
                if !send(Report::Unavailable { error }) || !retryable {
                    return;
                }
                // Wait before trying again, unless the stream closes meanwhile.
                match requests.recv_timeout(delay) {
                    Ok(Request::Close) | Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
                    Ok(Request::Commit { reply, .. }) => {
                        let _ = reply.send(Err(MqError::local("IBM MQ: not connected")));
                    }
                    _ => {}
                }
                delay = (delay * 2).min(RECONNECT_MAX);
                continue;
            }
        };
        delay = RECONNECT_MIN;
        epoch += 1;
        if !send(Report::Connected { epoch }) {
            return;
        }
        loop {
            let request = match requests.recv() {
                Ok(request) => request,
                Err(_) => return,
            };
            match request {
                Request::Close => return,
                Request::Fetch { epoch: e, .. } if e != epoch => {}
                Request::Fetch { limits, .. } => match read_batch(&session, limits) {
                    Ok(messages) => {
                        if !send(Report::Fetched { epoch, messages }) {
                            return;
                        }
                    }
                    Err(error) if error.lost() => {
                        drop(session);
                        if !send(Report::Lost { epoch, error }) {
                            return;
                        }
                        break;
                    }
                    Err(error) => {
                        let _ = session.backout();
                        send(Report::Failed { error });
                        return;
                    }
                },
                Request::Commit { epoch: e, reply } if e != epoch => {
                    let _ = reply.send(Err(MqError::local("IBM MQ: the connection was replaced")));
                }
                Request::Commit { reply, .. } => match session.commit() {
                    Ok(()) => {
                        let _ = reply.send(Ok(()));
                    }
                    Err(error) => {
                        let lost = error.lost();
                        let _ = reply.send(Err(error.clone()));
                        drop(session);
                        if lost {
                            if !send(Report::Lost { epoch, error }) {
                                return;
                            }
                            break;
                        }
                        send(Report::Failed { error });
                        return;
                    }
                },
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Busy {
    Idle,
    Fetching,
    /// A batch is out; its last position.
    InFlight(u64),
}

struct Partition {
    requests: std_mpsc::Sender<Request>,
    thread: Option<thread::JoinHandle<()>>,
    /// 0 while not connected.
    epoch: u64,
    seq: u64,
    position: u64,
    busy: Busy,
}

pub struct MqSource {
    label: String,
    limits: BatchLimits,
    partitions: Vec<Partition>,
    reports: mpsc::UnboundedReceiver<(u32, Report)>,
    events: VecDeque<SourceEvent>,
    paused: BTreeSet<u32>,
    stopped: bool,
    /// Why the connections stopped, repeated to later calls instead of a vaguer message.
    fatal: Option<String>,
}

impl MqSource {
    pub fn new(config: &Value, limits: BatchLimits) -> Result<MqSource, ConnectorError> {
        let settings = Settings::new(config)?;
        library()?;
        let connections = config
            .get("connections")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .clamp(1, 64) as u32;
        let (report_tx, reports) = mpsc::unbounded_channel();
        let mut partitions = Vec::new();
        for partition in 0..connections {
            let (requests, receiver) = std_mpsc::channel();
            let settings = settings.clone();
            let report_tx = report_tx.clone();
            let thread = thread::Builder::new()
                .name(format!("ibm-mq-{partition}"))
                .spawn(move || source_worker(partition, settings, receiver, report_tx))
                .map_err(|e| ConnectorError::fatal(format!("IBM MQ worker thread: {e}")))?;
            partitions.push(Partition {
                requests,
                thread: Some(thread),
                epoch: 0,
                seq: 0,
                position: 0,
                busy: Busy::Idle,
            });
        }
        Ok(MqSource {
            label: settings.label(),
            limits,
            partitions,
            reports,
            events: VecDeque::new(),
            paused: BTreeSet::new(),
            stopped: false,
            fatal: None,
        })
    }

    fn batch(&mut self, index: u32, messages: Vec<Message>) -> Result<SourceEvent, ConnectorError> {
        let state = &mut self.partitions[index as usize];
        let mut builder = EnvelopeBuilder::new();
        let first = state.position + 1;
        for message in &messages {
            state.position += 1;
            let mut headers = vec![
                Header {
                    key: "mq-message-id".into(),
                    value: hex(&message.message_id).into_bytes(),
                },
                Header {
                    key: "mq-backout-count".into(),
                    value: message.backout_count.to_string().into_bytes(),
                },
            ];
            if message.correlation_id.iter().any(|b| *b != 0) {
                headers.push(Header {
                    key: "mq-correlation-id".into(),
                    value: hex(&message.correlation_id).into_bytes(),
                });
            }
            builder.push(&Envelope {
                key: None,
                value: message.data.clone(),
                headers,
                partition: index,
                position: Some(state.position as i64),
                timestamp: put_millis(&message.put_time),
            });
        }
        let records = builder.len() as u32;
        let bytes = builder.bytes() as u64;
        let batch = builder
            .finish()
            .map_err(|e| ConnectorError::fatal(e.to_string()))?;
        let meta = BatchMeta {
            id: BatchId {
                partition: PartitionId(index),
                epoch: AssignmentEpoch(state.epoch),
                seq: state.seq,
            },
            first: Position::Tag(first),
            last: Position::Tag(state.position),
            records,
            bytes,
        };
        state.seq += 1;
        state.busy = Busy::InFlight(state.position);
        Ok(SourceEvent::Batch { batch, meta })
    }

    fn handle(
        &mut self,
        index: u32,
        report: Report,
    ) -> Result<Option<SourceEvent>, ConnectorError> {
        let state = &mut self.partitions[index as usize];
        Ok(match report {
            Report::Connected { epoch } => {
                state.epoch = epoch;
                state.seq = 0;
                state.position = 0;
                state.busy = Busy::Idle;
                Some(SourceEvent::Assigned {
                    partition: PartitionId(index),
                    epoch: AssignmentEpoch(epoch),
                })
            }
            Report::Fetched { epoch, messages } if epoch == state.epoch => {
                if messages.is_empty() {
                    state.busy = Busy::Idle;
                    Some(SourceEvent::Idle)
                } else {
                    Some(self.batch(index, messages)?)
                }
            }
            Report::Fetched { .. } => None,
            Report::Lost { epoch, error } if epoch == state.epoch => {
                state.epoch = 0;
                state.busy = Busy::Idle;
                self.events.push_back(SourceEvent::Revoking {
                    partition: PartitionId(index),
                    epoch: AssignmentEpoch(epoch),
                    within: Duration::ZERO,
                });
                return Err(ConnectorError::retryable(format!(
                    "{} (the connection's messages go back to the queue)",
                    error.message
                )));
            }
            Report::Lost { .. } => None,
            Report::Unavailable { error } if error.retryable() => {
                return Err(error.into_connector());
            }
            Report::Unavailable { error } | Report::Failed { error } => {
                self.stopped = true;
                self.fatal = Some(error.message.clone());
                return Err(ConnectorError::fatal(error.message));
            }
        })
    }
}

impl Source for MqSource {
    fn next_event(&mut self) -> BoxFuture<'_, Result<SourceEvent, ConnectorError>> {
        Box::pin(async move {
            loop {
                if let Some(event) = self.events.pop_front() {
                    return Ok(event);
                }
                if let Some(reason) = &self.fatal {
                    return Err(ConnectorError::fatal(reason.clone()));
                }
                if !self.stopped {
                    for (index, state) in self.partitions.iter_mut().enumerate() {
                        if state.epoch > 0
                            && state.busy == Busy::Idle
                            && !self.paused.contains(&(index as u32))
                        {
                            state.busy = Busy::Fetching;
                            let _ = state.requests.send(Request::Fetch {
                                epoch: state.epoch,
                                limits: self.limits,
                            });
                        }
                    }
                }
                // Let the driver tick when nothing arrives for a while.
                let tick = self.limits.max_wait + Duration::from_millis(500);
                match tokio::time::timeout(tick, self.reports.recv()).await {
                    Ok(Some((index, report))) => {
                        if let Some(event) = self.handle(index, report)? {
                            return Ok(event);
                        }
                    }
                    Ok(None) => {
                        return Err(ConnectorError::fatal(format!(
                            "IBM MQ: the connections to {} stopped",
                            self.label
                        )));
                    }
                    Err(_) => return Ok(SourceEvent::Idle),
                }
            }
        })
    }

    fn commit<'a>(&'a mut self, commits: &'a [Commit]) -> BoxFuture<'a, CommitOutcome> {
        Box::pin(async move {
            for commit in commits {
                let index = commit.partition.0 as usize;
                let Some(state) = self.partitions.get_mut(index) else {
                    return CommitOutcome::Stale;
                };
                if commit.epoch != AssignmentEpoch(state.epoch) {
                    return CommitOutcome::Stale;
                }
                let Position::Tag(position) = commit.position else {
                    return CommitOutcome::Failed(ConnectorError::fatal(
                        "IBM MQ commits its own positions",
                    ));
                };
                // One batch per connection is in flight, and a commit takes all of it.
                if state.busy != Busy::InFlight(position) {
                    return CommitOutcome::Failed(ConnectorError::fatal(format!(
                        "IBM MQ: a commit through {position} does not end the batch in flight \
                         ({:?})",
                        state.busy
                    )));
                }
                let (reply, answer) = oneshot::channel();
                if state
                    .requests
                    .send(Request::Commit {
                        epoch: state.epoch,
                        reply,
                    })
                    .is_err()
                {
                    return CommitOutcome::Stale;
                }
                match answer.await {
                    Ok(Ok(())) => state.busy = Busy::Idle,
                    Ok(Err(error)) if error.lost() => return CommitOutcome::Stale,
                    Ok(Err(error)) => return CommitOutcome::Failed(error.into_connector()),
                    Err(_) => return CommitOutcome::Stale,
                }
            }
            CommitOutcome::Committed
        })
    }

    fn pause(&mut self, partition: PartitionId) {
        self.paused.insert(partition.0);
    }

    fn resume(&mut self, partition: PartitionId) {
        self.paused.remove(&partition.0);
    }

    fn failed(&mut self, _reason: &str) {
        // Nothing more is committed: the workers back out and disconnect.
        self.stopped = true;
        for state in &self.partitions {
            let _ = state.requests.send(Request::Close);
        }
    }

    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities {
            replayable: true,
            ordered: self.partitions.len() == 1,
            ack_kind: AckKind::Message,
            max_instances: None,
            needs_keepalive: true,
            durable_positions: false,
        }
    }

    fn close(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            for state in &self.partitions {
                let _ = state.requests.send(Request::Close);
            }
            let threads: Vec<_> = self
                .partitions
                .iter_mut()
                .filter_map(|p| p.thread.take())
                .collect();
            let _ = tokio::task::spawn_blocking(move || {
                for thread in threads {
                    let _ = thread.join();
                }
            })
            .await;
        })
    }
}

/// The MsgId of a written message: the same for the same input, so a replay is recognizable.
pub fn message_id(stream: &str, partition: u32, position: &str) -> [u8; 24] {
    let digest = ring::digest::digest(
        &ring::digest::SHA256,
        format!("{stream}:{partition}:{position}").as_bytes(),
    );
    let mut id = [0u8; 24];
    id.copy_from_slice(&digest.as_ref()[..24]);
    id
}

struct Put {
    messages: Vec<(Vec<u8>, [u8; 24])>,
    reply: oneshot::Sender<Result<(), MqError>>,
}

fn sink_worker(settings: Settings, requests: std_mpsc::Receiver<Put>) {
    let mut session: Option<Session> = None;
    while let Ok(put) = requests.recv() {
        if session.is_none() {
            match Session::open(&settings, false) {
                Ok(opened) => session = Some(opened),
                Err(error) => {
                    let _ = put.reply.send(Err(error));
                    continue;
                }
            }
        }
        let current = session.as_ref().expect("opened");
        let result = put_batch(current, &put.messages);
        if let Err(error) = &result {
            if error.lost() {
                session = None;
            } else {
                let _ = current.backout();
            }
        }
        let _ = put.reply.send(result);
    }
}

fn put_batch(session: &Session, messages: &[(Vec<u8>, [u8; 24])]) -> Result<(), MqError> {
    for (data, id) in messages {
        // Asynchronous put responses: a put does not wait for the queue manager's answer.
        // Failures of persistent puts in a unit of work fail its commit; MQSTAT below names
        // the reason first.
        let options = (
            constants::MQPMO_SYNCPOINT
                | constants::MQPMO_FAIL_IF_QUIESCING
                | constants::MQPMO_ASYNC_RESPONSE,
            constants::MQPER_PERSISTENT,
            MessageId(*id),
        );
        let result = if std::str::from_utf8(data).is_ok() {
            session
                .queue
                .put_message(&options, &(data.as_slice(), text_format()))
        } else {
            session
                .queue
                .put_message(&options, &(data.as_slice(), bytes_format()))
        };
        result
            .discard_warning()
            .map_err(|e| MqError::new("putting a message", &e))?;
    }
    let status = session
        .qm
        .stat_put()
        .discard_warning()
        .map_err(|e| MqError::new("checking the puts", &e))?;
    if status.put_failure_count > 0 {
        let reason = status.reason.0;
        return Err(MqError {
            reason,
            message: format!(
                "IBM MQ putting messages: {} of {} puts failed, reason {reason}{}",
                status.put_failure_count,
                messages.len(),
                hint(reason)
            ),
        });
    }
    session.commit()
}

fn text_format() -> MessageFormat {
    MessageFormat {
        ccsid: CCSID(1208),
        encoding: constants::MQENC_NATIVE,
        fmt: TextEnc::Ascii(fmt::MQFMT_STRING),
    }
}

fn bytes_format() -> MessageFormat {
    MessageFormat {
        ccsid: CCSID(1208),
        encoding: constants::MQENC_NATIVE,
        fmt: TextEnc::Ascii(fmt::MQFMT_NONE),
    }
}

pub struct MqSink {
    label: String,
    stream: String,
    requests: Mutex<std_mpsc::Sender<Put>>,
}

impl MqSink {
    /// `stream`: the stream's name, part of each message's MsgId.
    pub fn new(config: &Value, stream: &str) -> Result<MqSink, ConnectorError> {
        let settings = Settings::new(config)?;
        library()?;
        let (requests, receiver) = std_mpsc::channel();
        let label = settings.label();
        thread::Builder::new()
            .name("ibm-mq-sink".into())
            .spawn(move || sink_worker(settings, receiver))
            .map_err(|e| ConnectorError::fatal(format!("IBM MQ worker thread: {e}")))?;
        Ok(MqSink {
            label,
            stream: stream.to_string(),
            requests: Mutex::new(requests),
        })
    }
}

impl Sink for MqSink {
    fn write<'a>(
        &'a self,
        batch: &'a RecordBatch,
        meta: &'a BatchMeta,
    ) -> BoxFuture<'a, Result<(), ConnectorError>> {
        Box::pin(async move {
            let rows = read_envelope(batch).map_err(|e| ConnectorError::fatal(e.to_string()))?;
            let messages = rows
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    let key = meta.row_key(row.position, index);
                    let id = message_id(&self.stream, meta.id.partition.0, &key);
                    (row.value, id)
                })
                .collect();
            let (reply, answer) = oneshot::channel();
            self.requests
                .lock()
                .expect("not poisoned")
                .send(Put { messages, reply })
                .map_err(|_| ConnectorError::fatal("IBM MQ: the sink's connection stopped"))?;
            match answer.await {
                Ok(result) => result.map_err(|e| e.into_connector()),
                Err(_) => Err(ConnectorError::fatal(format!(
                    "IBM MQ: the connection to {} stopped",
                    self.label
                ))),
            }
        })
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn capabilities(&self) -> SinkCapabilities {
        SinkCapabilities {
            idempotent: false,
            transactional: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn settings_need_a_channel_a_queue_and_a_place_to_connect() {
        let settings = Settings::new(&json!({
            "host": "mq.example.com", "channel": "APP.SVRCONN", "queue": "ORDERS.IN",
            "queue_manager": "QM1", "user": "app", "password": "secret"
        }))
        .unwrap();
        assert_eq!(settings.connection_name, "mq.example.com(1414)");
        assert_eq!(
            settings.label(),
            "queue ORDERS.IN on mq.example.com(1414) (APP.SVRCONN, queue manager QM1)"
        );
        assert!(!settings.label().contains("secret"));

        let multi = Settings::new(&json!({
            "connection_name": "a(1414),b(1414)", "channel": "C", "queue": "Q"
        }))
        .unwrap();
        assert_eq!(multi.connection_name, "a(1414),b(1414)");

        for config in [
            json!({"channel": "C", "queue": "Q"}),
            json!({"host": "h", "queue": "Q"}),
            json!({"host": "h", "channel": "C"}),
            json!({"host": "h", "channel": "C".repeat(21), "queue": "Q"}),
            json!({"host": "h", "channel": "C", "queue": "Q", "cipher_spec": "ANY_TLS13"}),
        ] {
            assert!(Settings::new(&config).is_err(), "{config}");
        }
    }

    #[test]
    fn errors_are_lost_retryable_or_fatal_with_a_hint() {
        let lost = MqError {
            reason: 2009,
            message: String::new(),
        };
        assert!(lost.lost() && lost.retryable());
        let full = MqError {
            reason: 2053,
            message: String::new(),
        };
        assert!(!full.lost() && full.retryable());
        let denied = MqError {
            reason: 2035,
            message: String::new(),
        };
        assert!(!denied.retryable());
        assert!(hint(2035).contains("CHLAUTH"));
        assert!(hint(2024).contains("MAXUMSGS"));
    }

    #[test]
    fn message_ids_repeat_for_the_same_input_only() {
        let id = message_id("orders", 0, "17");
        assert_eq!(id, message_id("orders", 0, "17"));
        assert_ne!(id, message_id("orders", 0, "18"));
        assert_ne!(id, message_id("orders", 1, "17"));
        assert_ne!(id, message_id("refunds", 0, "17"));
    }

    #[test]
    fn put_times_become_epoch_milliseconds() {
        assert_eq!(put_millis("1970010100000000"), Some(0));
        assert_eq!(put_millis("2026101312304512"), Some(1_791_894_645_120));
        assert_eq!(put_millis("20261013"), None);
        assert_eq!(put_millis("2026131312304512"), None);
    }

    #[test]
    fn a_missing_library_is_explained() {
        // SAFETY: the variable is set before the library is first loaded in this test binary.
        unsafe { std::env::set_var(LIBRARY_VARIABLE, "/no/such/libmqm_r.so") };
        let Err(error) = library() else {
            panic!("no library should load from a missing path");
        };
        let error = error.to_string();
        assert!(
            error.contains("IBM MQ client library was not found"),
            "{error}"
        );
        assert!(error.contains(LIBRARY_VARIABLE));
    }
}
