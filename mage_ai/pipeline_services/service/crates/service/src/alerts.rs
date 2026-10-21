//! Alerts a service sends (manifest `alerts`): a failed run, a run running too long, a
//! stream that stopped or fell behind, a failed monitor. Each message says what happened,
//! where (pipeline, block, run), the error's first lines, a link when
//! MAGE_SERVICE_PUBLIC_URL is set, and what to do.
//!
//! A channel's URL comes from the environment variable it names, read when sending; it is
//! never logged. Sending happens off the run's path, with three attempts. The same alert
//! (kind and pipeline) again within the cooldown is held and counted, and the next one
//! sent says how many were held.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use mage_service_core::manifest::{AlertChannel, AlertKind, Alerts, ChannelKind};
use serde_json::{Value, json};

/// Lines of an error a message carries; the run's log has the rest.
const ERROR_LINES: usize = 6;
const ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, Default)]
pub struct Event {
    pub kind: Option<AlertKind>,
    pub pipeline: String,
    pub run_id: Option<String>,
    pub block: Option<String>,
    pub title: String,
    pub error: Option<String>,
    pub hint: String,
}

pub struct Alerter {
    config: Alerts,
    service: String,
    public_url: Option<String>,
    held: Mutex<HashMap<String, (Instant, u64)>>,
    /// Runs already reported late.
    late: Mutex<HashSet<String>>,
    /// Streams reported behind or stopped, until they recover.
    streams: Mutex<HashSet<String>>,
}

impl Alerter {
    pub fn new(config: Alerts, service: &str) -> Alerter {
        Alerter {
            config,
            service: service.to_string(),
            public_url: std::env::var("MAGE_SERVICE_PUBLIC_URL")
                .ok()
                .map(|url| url.trim_end_matches('/').to_string())
                .filter(|url| !url.is_empty()),
            held: Mutex::new(HashMap::new()),
            late: Mutex::new(HashSet::new()),
            streams: Mutex::new(HashSet::new()),
        }
    }

    fn wants(&self, kind: AlertKind) -> bool {
        self.config.on.contains(&kind)
    }

    fn cooldown(&self) -> Duration {
        Duration::from_secs(self.config.cooldown_minutes.unwrap_or(10) * 60)
    }

    /// Sends the alert unless the same one went out within the cooldown; returns whether
    /// it was sent.
    pub fn notify(&self, event: Event) -> bool {
        let Some(kind) = event.kind else {
            return false;
        };
        if !self.wants(kind) {
            return false;
        }
        let key = format!("{}|{}", kind.as_str(), event.pipeline);
        let held = {
            let mut held = self.held.lock().unwrap_or_else(|p| p.into_inner());
            match held.get_mut(&key) {
                Some((last, count)) if last.elapsed() < self.cooldown() => {
                    *count += 1;
                    return false;
                }
                Some((last, count)) => {
                    let earlier = *count;
                    *last = Instant::now();
                    *count = 0;
                    earlier
                }
                None => {
                    held.insert(key, (Instant::now(), 0));
                    0
                }
            }
        };
        let message = self.message(&event, held);
        for channel in self.config.channels.clone() {
            let payload = payload(channel.kind, &message);
            std::thread::spawn(move || deliver(&channel, &payload));
        }
        true
    }

    fn link(&self, run_id: &str) -> Option<String> {
        self.public_url
            .as_ref()
            .map(|url| format!("{url}/v1/runs/{run_id}"))
    }

    fn message(&self, event: &Event, held: u64) -> Message {
        let mut lines = vec![format!("Pipeline: {}", event.pipeline)];
        if let Some(block) = &event.block {
            lines.push(format!("Block: {block}"));
        }
        if let Some(run) = &event.run_id {
            match self.link(run) {
                Some(link) => lines.push(format!("Run: {link} (log: {link}/logs)")),
                None => lines.push(format!("Run: {run}")),
            }
        }
        if let Some(error) = &event.error {
            let error = crate::log::redact(error);
            let shown: Vec<&str> = error.lines().take(ERROR_LINES).collect();
            let more = error.lines().count().saturating_sub(ERROR_LINES);
            lines.push(format!("Error: {}", shown.join("\n")));
            if more > 0 {
                lines.push(format!("({more} more lines in the run's log)"));
            }
        }
        if held > 0 {
            lines.push(format!(
                "{held} more like this were held in the last {} minutes.",
                self.cooldown().as_secs() / 60
            ));
        }
        lines.push(format!("What to do: {}", event.hint));
        Message {
            kind: event.kind.map(AlertKind::as_str).unwrap_or_default(),
            title: format!("{}: {}", self.service, event.title),
            body: lines.join("\n"),
            service: self.service.clone(),
            pipeline: event.pipeline.clone(),
            run_id: event.run_id.clone(),
            block: event.block.clone(),
            link: event.run_id.as_deref().and_then(|r| self.link(r)),
            held,
        }
    }

    /// A run that failed; `block` is the block that failed, when one did.
    pub fn run_failed(&self, pipeline: &str, run_id: &str, block: Option<&str>, error: &str) {
        self.notify(Event {
            kind: Some(AlertKind::RunFailed),
            pipeline: pipeline.to_string(),
            run_id: Some(run_id.to_string()),
            block: block.map(str::to_string),
            title: format!("run of {pipeline} failed"),
            error: Some(error.to_string()),
            hint: "Read the run's log for the whole error. Fix the block, or what it reads, and \
                   run the pipeline again; a request with the same idempotency key retries it."
                .into(),
        });
    }

    /// Runs, streams and their limits, checked every few seconds by [`watch`].
    fn check(&self, service: &crate::service::Service) {
        if let Some(minutes) = self.config.run_minutes
            && self.wants(AlertKind::RunLate)
        {
            let limit = chrono::Duration::milliseconds((minutes * 60_000.0) as i64);
            let now = chrono::Utc::now();
            let running = service
                .ledger
                .list_runs(
                    None,
                    Some(mage_service_core::protocol::RunStatus::Running),
                    500,
                )
                .unwrap_or_default();
            for run in running {
                let started = run
                    .started_at
                    .as_deref()
                    .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok());
                let Some(started) = started else { continue };
                if now - started.with_timezone(&chrono::Utc) < limit {
                    continue;
                }
                if !self
                    .late
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(run.id.clone())
                {
                    continue;
                }
                self.notify(Event {
                    kind: Some(AlertKind::RunLate),
                    pipeline: run.pipeline.clone(),
                    run_id: Some(run.id.clone()),
                    block: None,
                    title: format!(
                        "run of {} still running after {minutes} minutes",
                        run.pipeline
                    ),
                    error: None,
                    hint: format!(
                        "Check the run's log for the block it is in. If it is stuck, cancel it \
                         with POST /v1/runs/{}/cancel.",
                        run.id
                    ),
                });
            }
        }
        let statuses: Vec<(String, crate::stream::StreamStatus)> = service
            .streams
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(|(name, status)| Some((name.clone(), status.lock().ok()?.clone())))
            .collect();
        for (pipeline, status) in statuses {
            let stopped = matches!(status.state, crate::stream::StreamState::Failed);
            let lag: i64 = status.lag.values().sum();
            let behind = self.config.stream_lag.is_some_and(|limit| lag > limit);
            let mut seen = self.streams.lock().unwrap_or_else(|p| p.into_inner());
            let stopped_key = format!("stopped|{pipeline}");
            let lag_key = format!("lag|{pipeline}");
            if stopped && seen.insert(stopped_key.clone()) {
                drop(seen);
                self.notify(Event {
                    kind: Some(AlertKind::StreamStopped),
                    pipeline: pipeline.clone(),
                    title: format!("stream {pipeline} stopped"),
                    error: status.last_error.clone(),
                    hint: "Check that its source and sinks answer with `mage-service \
                           check-connections`, fix what the error names, and restart the \
                           service."
                        .into(),
                    ..Event::default()
                });
                seen = self.streams.lock().unwrap_or_else(|p| p.into_inner());
            } else if !stopped {
                seen.remove(&stopped_key);
            }
            if behind && seen.insert(lag_key.clone()) {
                drop(seen);
                self.notify(Event {
                    kind: Some(AlertKind::StreamLag),
                    pipeline: pipeline.clone(),
                    title: format!("stream {pipeline} is {lag} messages behind"),
                    hint: "Find the slowest block in /v1/streams (p95 per block), then add \
                           replicas or speed the block up. The lag clears once it keeps up."
                        .into(),
                    ..Event::default()
                });
            } else if !behind {
                seen.remove(&lag_key);
            }
        }
    }

    /// A monitor that failed on a block's output (monitors run with the checks feature).
    #[cfg_attr(not(feature = "checks"), allow(dead_code))]
    pub fn monitor_failed(&self, pipeline: &str, block: &str, run_id: &str, summary: &str) {
        self.notify(Event {
            kind: Some(AlertKind::MonitorFailed),
            pipeline: pipeline.to_string(),
            run_id: Some(run_id.to_string()),
            block: Some(block.to_string()),
            title: format!("a monitor of {block} failed"),
            error: Some(summary.to_string()),
            hint: "Compare the output with the monitor's baseline (the records in \
                   <data>/monitors); if the change is expected, silence the monitor or move \
                   its baseline."
                .into(),
        });
    }
}

/// Checks runs and streams every 15 seconds while the service runs.
pub fn watch(service: std::sync::Arc<crate::service::Service>) {
    if service.alerts.is_none() {
        return;
    }
    tokio::spawn(async move {
        let every = std::env::var("MAGE_SERVICE_ALERT_CHECK_SECONDS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|s| *s > 0.0)
            .unwrap_or(15.0);
        loop {
            tokio::time::sleep(Duration::from_secs_f64(every)).await;
            if service.shutting_down() {
                break;
            }
            let checked = service.clone();
            let _ = tokio::task::spawn_blocking(move || {
                if let Some(alerts) = &checked.alerts {
                    alerts.check(&checked);
                }
            })
            .await;
        }
    });
}

#[derive(Debug, Clone)]
pub struct Message {
    kind: &'static str,
    title: String,
    body: String,
    service: String,
    pipeline: String,
    run_id: Option<String>,
    block: Option<String>,
    link: Option<String>,
    held: u64,
}

fn payload(kind: ChannelKind, message: &Message) -> Value {
    let text = format!("{}\n{}", message.title, message.body);
    match kind {
        ChannelKind::Slack => json!({
            "text": text,
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": truncate(&message.title, 150)}},
                {"type": "section", "text": {"type": "mrkdwn", "text": truncate(&message.body, 2900)}},
            ],
        }),
        // A Teams workflow's webhook takes an Adaptive Card.
        ChannelKind::Teams => json!({
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {"type": "TextBlock", "text": message.title, "weight": "Bolder", "wrap": true},
                        {"type": "TextBlock", "text": message.body, "wrap": true},
                    ],
                },
            }],
        }),
        ChannelKind::GoogleChat => json!({"text": truncate(&text, 4000)}),
        ChannelKind::Discord => json!({"content": truncate(&text, 1990)}),
        ChannelKind::Webhook => json!({
            "kind": message.kind,
            "service": message.service,
            "pipeline": message.pipeline,
            "run_id": message.run_id,
            "block": message.block,
            "title": message.title,
            "text": message.body,
            "link": message.link,
            "held": message.held,
            "at": chrono::Utc::now().to_rfc3339(),
        }),
    }
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let kept: String = text.chars().take(limit - 1).collect();
    format!("{kept}…")
}

/// Posts the alert, three times at most; the URL never reaches the log.
fn deliver(channel: &AlertChannel, payload: &Value) {
    let Ok(url) = std::env::var(&channel.url_env) else {
        crate::log::warn(&format!(
            "An alert was not sent: {} is not set.",
            channel.url_env
        ));
        return;
    };
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .http_status_as_error(false)
        .build()
        .into();
    let mut last = String::new();
    for attempt in 0..ATTEMPTS {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs(2u64.pow(attempt)));
        }
        match agent.post(&url).send_json(payload) {
            Ok(response) if response.status().is_success() => return,
            Ok(response) => last = format!("the channel answered {}", response.status()),
            Err(error) => last = crate::log::redact(&error.to_string()).replace(&url, "<url>"),
        }
    }
    crate::log::warn(&format!(
        "An alert to the channel in {} was not delivered after {ATTEMPTS} attempts: {last}",
        channel.url_env
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alerter(cooldown: u64) -> Alerter {
        Alerter::new(
            Alerts {
                on: vec![AlertKind::RunFailed],
                channels: vec![],
                cooldown_minutes: Some(cooldown),
                run_minutes: None,
                stream_lag: None,
            },
            "scoring",
        )
    }

    #[test]
    fn a_repeated_alert_is_held_and_counted() {
        let alerts = alerter(10);
        let event = || Event {
            kind: Some(AlertKind::RunFailed),
            pipeline: "etl".into(),
            ..Event::default()
        };
        assert!(alerts.notify(event()));
        assert!(!alerts.notify(event()));
        assert!(!alerts.notify(event()));
        // Another pipeline is another alert; a kind not asked for is never sent.
        assert!(alerts.notify(Event {
            pipeline: "other".into(),
            ..event()
        }));
        assert!(!alerts.notify(Event {
            kind: Some(AlertKind::StreamLag),
            ..event()
        }));
        let zero = alerter(0);
        assert!(zero.notify(event()));
        assert!(zero.notify(event()));
    }

    #[test]
    fn a_message_names_the_block_the_error_and_what_to_do() {
        let alerts = alerter(10);
        let error = (1..=9)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let message = alerts.message(
            &Event {
                kind: Some(AlertKind::RunFailed),
                pipeline: "etl".into(),
                run_id: Some("r1".into()),
                block: Some("load".into()),
                title: "run of etl failed".into(),
                error: Some(error),
                hint: "Fix it.".into(),
            },
            2,
        );
        assert_eq!(message.title, "scoring: run of etl failed");
        assert!(message.body.contains("Block: load"));
        assert!(message.body.contains("line 6") && !message.body.contains("line 7"));
        assert!(message.body.contains("(3 more lines in the run's log)"));
        assert!(message.body.contains("2 more like this were held"));
        assert!(message.body.ends_with("What to do: Fix it."));
        let slack = payload(ChannelKind::Slack, &message);
        assert_eq!(
            slack["blocks"][0]["text"]["text"],
            "scoring: run of etl failed"
        );
        let teams = payload(ChannelKind::Teams, &message);
        assert_eq!(
            teams["attachments"][0]["contentType"],
            "application/vnd.microsoft.card.adaptive"
        );
        let hook = payload(ChannelKind::Webhook, &message);
        assert_eq!(
            (hook["kind"].as_str(), hook["block"].as_str()),
            (Some("run_failed"), Some("load"))
        );
        assert!(
            payload(ChannelKind::Discord, &message)["content"]
                .as_str()
                .unwrap()
                .len()
                < 2000
        );
    }
}
