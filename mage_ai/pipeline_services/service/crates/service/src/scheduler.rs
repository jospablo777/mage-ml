//! Schedules: each active time trigger starts a run at each occurrence of its cron
//! expression, in its timezone.
//!
//! One service owns the schedules: it holds a lease in the run history, so replicas that
//! share the data volume do not fire twice. Each occurrence has a key, trigger and instant,
//! that the run history accepts once, so a restart in the middle of a tick fires nothing
//! twice. Occurrences missed while the service was stopped are not run; the log says how
//! many there were.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use croner::Cron;
use mage_service_core::manifest::{Pipeline, Trigger, TriggerKind, cron_expression};
use mage_service_core::protocol::RunStatus;
use serde_json::{Map, Value, json};

use crate::ledger::{NewRun, timestamp};
use crate::service::Service;

const LEASE: &str = "scheduler";
const LEASE_SECONDS: i64 = 30;

/// A trigger that can fire: its cron expression parsed, or `@once`.
pub struct Schedule {
    pub pipeline: String,
    pub trigger: Trigger,
    cron: Option<Cron>,
    timezone: Tz,
    start: Option<DateTime<Utc>>,
}

#[derive(Debug, thiserror::Error)]
#[error("pipeline {pipeline}, trigger {trigger}: {reason}")]
pub struct ScheduleError {
    pub pipeline: String,
    pub trigger: String,
    pub reason: String,
}

impl Schedule {
    pub fn new(pipeline: &Pipeline, trigger: &Trigger) -> Result<Schedule, ScheduleError> {
        let error = |reason: String| ScheduleError {
            pipeline: pipeline.uuid.clone(),
            trigger: trigger.name.clone(),
            reason,
        };
        let expression = trigger.schedule.as_deref().unwrap_or("");
        let cron = match cron_expression(expression) {
            Some(expression) => Some(
                Cron::from_str(&expression)
                    .map_err(|e| error(format!("invalid schedule {expression:?}: {e}")))?,
            ),
            None => None,
        };
        let timezone = match trigger.timezone.as_deref() {
            None | Some("") => Tz::UTC,
            Some(name) => name
                .parse::<Tz>()
                .map_err(|_| error(format!("unknown timezone {name:?}")))?,
        };
        let start = match trigger.start_time.as_deref() {
            None | Some("") => None,
            Some(text) => Some(
                DateTime::parse_from_rfc3339(text)
                    .map_err(|e| error(format!("invalid start_time {text:?}: {e}")))?
                    .with_timezone(&Utc),
            ),
        };
        Ok(Schedule {
            pipeline: pipeline.uuid.clone(),
            trigger: trigger.clone(),
            cron,
            timezone,
            start,
        })
    }

    pub fn is_once(&self) -> bool {
        self.cron.is_none()
    }

    /// The first occurrence strictly after `after`, not before the start time.
    pub fn next_after(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let cron = self.cron.as_ref()?;
        // Whole seconds: croner keeps the fraction of its input, and occurrence keys must
        // not depend on when in a second the scheduler looked.
        let mut from = after.with_nanosecond(0)?;
        if let Some(start) = self.start
            && start > from
        {
            // An occurrence exactly at the start time counts.
            from = start.with_nanosecond(0)? - chrono::Duration::seconds(1);
        }
        let local = self.timezone.from_utc_datetime(&from.naive_utc());
        cron.find_next_occurrence(&local, false)
            .ok()
            .and_then(|t| t.with_timezone(&Utc).with_nanosecond(0))
    }

    fn key(&self, occurrence: Option<DateTime<Utc>>) -> String {
        match occurrence {
            Some(at) => format!("{}/{}/{}", self.pipeline, self.trigger.name, timestamp(at)),
            None => format!("{}/{}/once", self.pipeline, self.trigger.name),
        }
    }
}

/// The schedules of the active time triggers; problems are logged and skip the trigger.
pub fn schedules(service: &Service) -> Vec<Schedule> {
    let mut schedules = Vec::new();
    for pipeline in &service.manifest.pipelines {
        for trigger in &pipeline.triggers {
            if trigger.kind != TriggerKind::Time || !trigger.active {
                continue;
            }
            match Schedule::new(pipeline, trigger) {
                Ok(schedule) => schedules.push(schedule),
                Err(error) => crate::log::error(&format!("The schedule is skipped: {error}")),
            }
        }
    }
    schedules
}

pub async fn run(service: Arc<Service>, owner: String) {
    let schedules = schedules(&service);
    if schedules.is_empty() {
        return;
    }
    let started = Utc::now();
    let mut last_checked: HashMap<String, DateTime<Utc>> = schedules
        .iter()
        .map(|s| (s.trigger.name.clone() + "/" + &s.pipeline, started))
        .collect();
    let mut holding = false;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if service.shutting_down() {
            return;
        }
        match service.ledger.acquire_lease(LEASE, &owner, LEASE_SECONDS) {
            Ok(true) => {
                if !holding {
                    crate::log::info("This service runs the schedules.");
                    holding = true;
                }
            }
            Ok(false) => {
                if holding {
                    crate::log::warn("Another service took over the schedules.");
                    holding = false;
                }
                continue;
            }
            Err(error) => {
                crate::log::error(&format!("The schedule lease failed: {error}"));
                continue;
            }
        }
        let now = Utc::now();
        for schedule in &schedules {
            let id = schedule.trigger.name.clone() + "/" + &schedule.pipeline;
            if schedule.is_once() {
                if schedule.start.is_some_and(|start| start > now) {
                    continue;
                }
                fire(&service, schedule, None, now);
                continue;
            }
            let checked = last_checked.get(&id).copied().unwrap_or(now);
            let mut cursor = checked;
            let mut due = Vec::new();
            while let Some(next) = schedule.next_after(cursor) {
                if next > now {
                    break;
                }
                due.push(next);
                cursor = next;
                if due.len() > 1000 {
                    break;
                }
            }
            if let Some(latest) = due.last().copied() {
                if due.len() > 1 {
                    crate::log::warn(&format!(
                        "{}: {} occurrences of trigger {} passed together; running the latest.",
                        schedule.pipeline,
                        due.len(),
                        schedule.trigger.name
                    ));
                }
                fire(&service, schedule, Some(latest), now);
                last_checked.insert(id, latest);
            }
        }
    }
}

fn fire(
    service: &Arc<Service>,
    schedule: &Schedule,
    occurrence: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) {
    let key = schedule.key(occurrence);
    if occurrence.is_none() && service.ledger.has_occurrence(&key).unwrap_or(true) {
        return;
    }
    let Some(pipeline) = service.manifest.pipeline(&schedule.pipeline) else {
        return;
    };
    let source = match occurrence {
        Some(_) => format!("schedule:{}", schedule.trigger.name),
        None => format!("once:{}", schedule.trigger.name),
    };
    if schedule.trigger.skip_if_previous_running {
        let busy = [RunStatus::Running, RunStatus::Queued]
            .iter()
            .any(|status| {
                service
                    .ledger
                    .list_runs(Some(&pipeline.uuid), Some(*status), 50)
                    .map(|runs| runs.iter().any(|r| r.source == source))
                    .unwrap_or(false)
            });
        if busy {
            crate::log::info(&format!(
                "{}: trigger {} skipped this occurrence; its previous run has not finished.",
                pipeline.uuid, schedule.trigger.name
            ));
            return;
        }
    }
    let mut variables: Map<String, Value> = schedule.trigger.variables.clone();
    let at = occurrence.unwrap_or(now);
    variables.insert("execution_date".into(), json!(timestamp(at)));
    if let Some(cron_at) = occurrence
        && let Some(next) = schedule.next_after(cron_at)
    {
        variables.insert("interval_start_datetime".into(), json!(timestamp(cron_at)));
        variables.insert("interval_end_datetime".into(), json!(timestamp(next)));
        variables.insert(
            "interval_seconds".into(),
            json!((next - cron_at).num_seconds()),
        );
    }
    let blocks: Vec<String> = pipeline.blocks.iter().map(|b| b.uuid.clone()).collect();
    let created = service.ledger.create_occurrence_run(
        &key,
        NewRun {
            pipeline: &pipeline.uuid,
            source: &source,
            variables: &variables,
            idempotency_key: None,
            blocks: &blocks,
        },
    );
    match created {
        Ok(Some(run_id)) => {
            crate::log::info(&format!(
                "{}: trigger {} started run {run_id}.",
                pipeline.uuid, schedule.trigger.name
            ));
            service.wake();
        }
        Ok(None) => {}
        Err(error) => crate::log::error(&format!("{}: {error}", pipeline.uuid)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schedule(expression: &str, timezone: Option<&str>, start: Option<&str>) -> Schedule {
        let pipeline: Pipeline = serde_json::from_value(json!({
            "uuid": "etl", "name": "etl", "max_concurrent_runs": 1, "blocks": []
        }))
        .unwrap();
        let trigger: Trigger = serde_json::from_value(json!({
            "name": "t", "kind": "time", "schedule": expression, "active": true,
            "timezone": timezone, "start_time": start
        }))
        .unwrap();
        Schedule::new(&pipeline, &trigger).unwrap()
    }

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn daily_runs_at_midnight_of_its_timezone() {
        let utc = schedule("@daily", None, None);
        assert_eq!(
            utc.next_after(at("2026-10-10T05:00:00Z")),
            Some(at("2026-10-11T00:00:00Z"))
        );
        let costa_rica = schedule("@daily", Some("America/Costa_Rica"), None);
        assert_eq!(
            costa_rica.next_after(at("2026-10-10T05:00:00Z")),
            Some(at("2026-10-10T06:00:00Z"))
        );
    }

    #[test]
    fn nothing_fires_before_the_start_time() {
        let s = schedule("0 * * * *", None, Some("2026-12-01T00:00:00Z"));
        assert_eq!(
            s.next_after(at("2026-10-10T05:30:00Z")),
            Some(at("2026-12-01T00:00:00Z"))
        );
    }

    #[test]
    fn invalid_schedules_and_timezones_are_named() {
        let pipeline: Pipeline = serde_json::from_value(json!({
            "uuid": "etl", "name": "etl", "max_concurrent_runs": 1, "blocks": []
        }))
        .unwrap();
        let trigger: Trigger = serde_json::from_value(json!({
            "name": "t", "kind": "time", "schedule": "every day", "active": true
        }))
        .unwrap();
        let error = Schedule::new(&pipeline, &trigger)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("invalid schedule"), "{error}");
        let trigger: Trigger = serde_json::from_value(json!({
            "name": "t", "kind": "time", "schedule": "@daily", "active": true,
            "timezone": "Mars/Olympus"
        }))
        .unwrap();
        let error = Schedule::new(&pipeline, &trigger)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("unknown timezone"), "{error}");
    }

    #[test]
    fn once_has_no_cron_and_a_fixed_key() {
        let s = schedule("@once", None, None);
        assert!(s.is_once());
        assert_eq!(s.key(None), "etl/t/once");
    }
}
