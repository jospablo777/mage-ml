//! The scheduler's decisions in shadow (ROADMAP P3, scheduler ownership): every few seconds
//! the gateway works out, for each active trigger of the project, the execution date the
//! scheduler should have a run for (PipelineSchedule.should_schedule's clock rules), and
//! checks that the run appears. A run that has not appeared after the grace period counts as
//! missing. The scheduler keeps its authority; this only counts and reports, at
//! /__gateway/shadow under `scheduler`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::gateway_schedules::{UNSUPPORTED, clock_execution_date, truthy};
use crate::gateway_shadow::{Metadata, Raw, read_rows};

const RECENT: usize = 20;

#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub(crate) struct Counts {
    /// Execution dates whose run appeared.
    pub on_time: u64,
    /// Execution dates whose run had not appeared after the grace period.
    pub missing: u64,
    /// Ticks that could not read the database.
    pub failed: u64,
}

struct Pending {
    schedule: i64,
    pipeline: String,
    execution: DateTime<Utc>,
    seen: DateTime<Utc>,
}

#[derive(Default)]
struct State {
    /// The last execution date taken in, per trigger: each is checked once.
    last: BTreeMap<i64, DateTime<Utc>>,
    pending: Vec<Pending>,
    counts: Counts,
    /// Triggers whose schedule the gateway does not evaluate (landing times, cron forms
    /// it does not read).
    unsupported: BTreeSet<i64>,
    recent: VecDeque<Value>,
}

pub(crate) struct SchedulerShadow {
    grace: chrono::Duration,
    state: Mutex<State>,
}

/// A trigger as the scheduler reads it.
#[derive(Debug, Clone)]
pub(crate) struct Trigger {
    pub id: i64,
    pub pipeline_uuid: String,
    /// Stored by member name: TIME, API, EVENT, AFTER_RUN.
    pub schedule_type: String,
    pub interval: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub settings: Value,
    pub last_enabled_at: Option<DateTime<Utc>>,
}

/// The execution date the scheduler should have a run for at `now`, None when it starts
/// none (should_schedule without the pipeline's state); an error for a trigger the gateway
/// does not evaluate.
pub(crate) fn due(trigger: &Trigger, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, String> {
    // After-run triggers start runs when another pipeline's run ends.
    if trigger.schedule_type == "AFTER_RUN" {
        return Ok(None);
    }
    let interval = trigger.interval.as_deref().unwrap_or_default();
    let presets = ["@hourly", "@daily", "@weekly", "@monthly"];
    if trigger.schedule_type == "TIME"
        && presets.contains(&interval)
        && truthy(
            trigger
                .settings
                .get("landing_time_enabled")
                .unwrap_or(&Value::Null),
        )
    {
        // Runs start ahead of the landing time by the runs' average duration.
        return Err(format!("{UNSUPPORTED}a landing time"));
    }
    if let Some(start) = trigger.start_time
        && now < start
    {
        return Ok(None);
    }
    let Some(execution) = clock_execution_date(trigger.interval.as_deref(), now)? else {
        return Ok(None);
    };
    if let Some(start) = trigger.start_time
        && execution < start
    {
        return Ok(None);
    }
    // A trigger enabled after the execution date starts no run for it, unless its settings
    // ask for an initial run.
    let initial = truthy(
        trigger
            .settings
            .get("create_initial_pipeline_run")
            .unwrap_or(&Value::Null),
    );
    if let Some(enabled) = trigger.last_enabled_at
        && !initial
        && execution < enabled
    {
        return Ok(None);
    }
    Ok(Some(execution))
}

fn time(raw: &Raw) -> Option<DateTime<Utc>> {
    match raw {
        Raw::Time(at) => Some(*at),
        Raw::Text(text) => crate::gateway_auth::naive_utc(text),
        _ => None,
    }
}

/// An execution date as a query parameter, as SQLAlchemy stores it.
fn execution_param(metadata: &Metadata, at: DateTime<Utc>) -> Raw {
    match metadata {
        Metadata::Sqlite(_) => Raw::Text(at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        Metadata::Postgres(_) => Raw::Time(at),
    }
}

impl SchedulerShadow {
    pub(crate) fn new(grace: chrono::Duration) -> SchedulerShadow {
        SchedulerShadow {
            grace,
            state: Mutex::new(State::default()),
        }
    }

    /// One look at the triggers and their runs.
    pub(crate) async fn tick(
        &self,
        metadata: &Metadata,
        repo_path: &Path,
        now: DateTime<Utc>,
    ) -> Result<(), String> {
        let (status, kind, settings) = match metadata {
            Metadata::Postgres(_) => (
                "CAST(status AS TEXT)",
                "CAST(schedule_type AS TEXT)",
                "CAST(settings AS TEXT)",
            ),
            Metadata::Sqlite(_) => ("status", "schedule_type", "settings"),
        };
        let rows = read_rows(
            metadata,
            &format!(
                "SELECT id, pipeline_uuid, {kind}, schedule_interval, start_time, {settings}, \
                 last_enabled_at FROM pipeline_schedule WHERE {status} = 'ACTIVE' \
                 AND (repo_path = {{1}} OR repo_path IS NULL)"
            ),
            &[Raw::Text(repo_path.display().to_string())],
        )
        .await?;
        // A trigger with a backfill is left out of the scheduler's loop.
        let backfilled: BTreeSet<i64> = read_rows(
            metadata,
            "SELECT DISTINCT pipeline_schedule_id FROM backfill \
             WHERE pipeline_schedule_id IS NOT NULL",
            &[],
        )
        .await?
        .iter()
        .filter_map(|r| match r.first() {
            Some(Raw::Int(id)) => Some(*id),
            _ => None,
        })
        .collect();
        let triggers: Vec<Trigger> = rows
            .into_iter()
            .filter_map(|r| {
                let Raw::Int(id) = r[0] else { return None };
                let text = |raw: &Raw| match raw {
                    Raw::Text(v) => Some(v.clone()),
                    _ => None,
                };
                Some(Trigger {
                    id,
                    pipeline_uuid: text(&r[1]).unwrap_or_default(),
                    schedule_type: text(&r[2]).unwrap_or_default().to_uppercase(),
                    interval: text(&r[3]),
                    start_time: time(&r[4]),
                    settings: text(&r[5])
                        .and_then(|t| serde_json::from_str(&t).ok())
                        .unwrap_or(Value::Null),
                    last_enabled_at: time(&r[6]),
                })
            })
            .collect();

        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            for trigger in &triggers {
                // The scheduler runs the project's pipelines only.
                let defined = repo_path
                    .join("pipelines")
                    .join(&trigger.pipeline_uuid)
                    .join("metadata.yaml")
                    .is_file();
                if !defined || backfilled.contains(&trigger.id) {
                    continue;
                }
                match due(trigger, now) {
                    Err(_) => {
                        state.unsupported.insert(trigger.id);
                    }
                    Ok(None) => {}
                    Ok(Some(execution)) => {
                        state.unsupported.remove(&trigger.id);
                        if state.last.get(&trigger.id) != Some(&execution) {
                            state.last.insert(trigger.id, execution);
                            state.pending.push(Pending {
                                schedule: trigger.id,
                                pipeline: trigger.pipeline_uuid.clone(),
                                execution,
                                seen: now,
                            });
                        }
                    }
                }
            }
        }

        let pending: Vec<(i64, DateTime<Utc>)> = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state
                .pending
                .iter()
                .map(|p| (p.schedule, p.execution))
                .collect()
        };
        let mut found = BTreeSet::new();
        for (schedule, execution) in pending {
            let rows = read_rows(
                metadata,
                "SELECT id FROM pipeline_run WHERE pipeline_schedule_id = {1} \
                 AND execution_date = {2} LIMIT 1",
                &[Raw::Int(schedule), execution_param(metadata, execution)],
            )
            .await?;
            if !rows.is_empty() {
                found.insert((schedule, execution));
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let grace = self.grace;
        let mut missing = Vec::new();
        let mut on_time = 0;
        state.pending.retain(|p| {
            if found.contains(&(p.schedule, p.execution)) {
                on_time += 1;
                false
            } else if now - p.seen > grace {
                missing.push(json!({
                    "pipeline_schedule_id": p.schedule,
                    "pipeline_uuid": p.pipeline,
                    "execution_date": p.execution.to_rfc3339(),
                }));
                false
            } else {
                true
            }
        });
        state.counts.on_time += on_time;
        state.counts.missing += missing.len() as u64;
        for difference in missing {
            crate::log::warn(&format!(
                "Gateway scheduler shadow: no run for {difference} after {} s",
                grace.num_seconds()
            ));
            if state.recent.len() == RECENT {
                state.recent.pop_front();
            }
            state.recent.push_back(difference);
        }
        Ok(())
    }

    pub(crate) fn failed(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.counts.failed += 1;
    }

    pub(crate) fn report(&self) -> Value {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        json!({
            "counts": state.counts,
            "pending": state.pending.len(),
            "unsupported_triggers": state.unsupported.len(),
            "grace_seconds": self.grace.num_seconds(),
            "recent_missing": state.recent.iter().cloned().collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().to_utc()
    }

    fn trigger(interval: &str) -> Trigger {
        Trigger {
            id: 1,
            pipeline_uuid: "p".into(),
            schedule_type: "TIME".into(),
            interval: Some(interval.into()),
            start_time: None,
            settings: Value::Null,
            last_enabled_at: None,
        }
    }

    #[test]
    fn the_execution_date_due_follows_should_schedule() {
        let now = utc("2026-10-22T06:10:15Z");
        assert_eq!(
            due(&trigger("@hourly"), now),
            Ok(Some(utc("2026-10-22T06:00:00Z")))
        );
        assert_eq!(
            due(&trigger("*/15 * * * *"), now),
            Ok(Some(utc("2026-10-22T06:00:00Z")))
        );
        assert_eq!(due(&trigger("@once"), now), Ok(None));
        assert_eq!(due(&trigger(""), now), Ok(None));
        // API triggers with an interval are scheduled too; after-run ones never.
        let api = Trigger {
            schedule_type: "API".into(),
            ..trigger("@daily")
        };
        assert_eq!(due(&api, now), Ok(Some(utc("2026-10-22T00:00:00Z"))));
        let after = Trigger {
            schedule_type: "AFTER_RUN".into(),
            ..trigger("@daily")
        };
        assert_eq!(due(&after, now), Ok(None));
        // Not before the start time, nor an execution date before it.
        let ahead = Trigger {
            start_time: Some(utc("2026-10-22T07:00:00Z")),
            ..trigger("@hourly")
        };
        assert_eq!(due(&ahead, now), Ok(None));
        let mid = Trigger {
            start_time: Some(utc("2026-10-22T06:05:00Z")),
            ..trigger("@hourly")
        };
        assert_eq!(due(&mid, now), Ok(None));
        // Enabled after the execution date: no initial run unless asked for.
        let enabled = Trigger {
            last_enabled_at: Some(utc("2026-10-22T06:01:00Z")),
            ..trigger("@hourly")
        };
        assert_eq!(due(&enabled, now), Ok(None));
        let initial = Trigger {
            settings: json!({"create_initial_pipeline_run": true}),
            ..enabled
        };
        assert_eq!(due(&initial, now), Ok(Some(utc("2026-10-22T06:00:00Z"))));
        // Landing times and cron forms the gateway does not read.
        let landing = Trigger {
            settings: json!({"landing_time_enabled": true}),
            ..trigger("@daily")
        };
        assert!(due(&landing, now).is_err());
        assert!(due(&trigger("0 0 L * *"), now).is_err());
    }

    #[tokio::test]
    async fn runs_are_on_time_or_missing_after_the_grace_period() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("mage.db");
        let repo = dir.path().join("repo");
        for p in ["on_time", "forgotten"] {
            std::fs::create_dir_all(repo.join("pipelines").join(p)).unwrap();
            std::fs::write(
                repo.join("pipelines").join(p).join("metadata.yaml"),
                "blocks: []\n",
            )
            .unwrap();
        }
        let connection = rusqlite::Connection::open(&db).unwrap();
        connection
            .execute_batch(&format!(
                "CREATE TABLE pipeline_schedule (id INTEGER, pipeline_uuid TEXT, schedule_type TEXT, \
                 schedule_interval TEXT, start_time TEXT, settings TEXT, last_enabled_at TEXT, \
                 status TEXT, repo_path TEXT);
                 CREATE TABLE backfill (pipeline_schedule_id INTEGER);
                 CREATE TABLE pipeline_run (id INTEGER, pipeline_schedule_id INTEGER, execution_date TEXT);
                 INSERT INTO pipeline_schedule VALUES (1, 'on_time', 'TIME', '@hourly', NULL, '{{}}', NULL, 'ACTIVE', '{repo}');
                 INSERT INTO pipeline_schedule VALUES (2, 'forgotten', 'TIME', '@hourly', NULL, '{{}}', NULL, 'ACTIVE', NULL);
                 INSERT INTO pipeline_schedule VALUES (3, 'on_time', 'TIME', '@hourly', NULL, '{{}}', NULL, 'INACTIVE', NULL);
                 INSERT INTO pipeline_schedule VALUES (4, 'gone', 'TIME', '@hourly', NULL, '{{}}', NULL, 'ACTIVE', NULL);
                 INSERT INTO pipeline_run VALUES (1, 1, '2026-10-22 06:00:00.000000');",
                repo = repo.display()
            ))
            .unwrap();
        let metadata = Metadata::Sqlite(db.clone());
        let shadow = SchedulerShadow::new(chrono::Duration::seconds(120));
        let now = utc("2026-10-22T06:10:15Z");
        shadow.tick(&metadata, &repo, now).await.unwrap();
        let report = shadow.report();
        assert_eq!(report["counts"]["on_time"], 1);
        assert_eq!(report["pending"], 1);
        // Within the grace period nothing is missing; after it, the forgotten trigger is.
        shadow
            .tick(&metadata, &repo, now + chrono::Duration::seconds(60))
            .await
            .unwrap();
        assert_eq!(shadow.report()["counts"]["missing"], 0);
        shadow
            .tick(&metadata, &repo, now + chrono::Duration::seconds(150))
            .await
            .unwrap();
        let report = shadow.report();
        assert_eq!(report["counts"]["missing"], 1);
        assert_eq!(report["recent_missing"][0]["pipeline_uuid"], "forgotten");
        // The same execution date is not checked twice; the next hour's is.
        shadow
            .tick(&metadata, &repo, utc("2026-10-22T07:00:30Z"))
            .await
            .unwrap();
        assert_eq!(shadow.report()["pending"], 2);
    }
}
