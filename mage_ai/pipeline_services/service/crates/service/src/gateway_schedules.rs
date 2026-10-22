//! GET /api/pipeline_schedules and /api/pipelines/<uuid>/pipeline_schedules, the triggers
//! pages' list, as PipelineScheduleResource.collection and its LIST presenter answer it,
//! with each trigger's next run date as PipelineSchedule.next_execution_date gives it.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use serde_json::{Map, Value, json};

use crate::gateway_shadow::{
    Kind, Metadata, Query, Raw, field_value, object, read_rows, str_time_utc,
};

/// The start of an answer's error that says the gateway leaves the query to the server.
pub(crate) const UNSUPPORTED: &str = "unsupported: ";

const DEFAULT_LIMIT: i64 = 40;

/// PipelineSchedulePresenter's default attributes.
const SCHEDULE_FIELDS: &[(&str, Kind)] = &[
    ("created_at", Kind::Time),
    ("description", Kind::Text),
    ("global_data_product_uuid", Kind::Text),
    ("id", Kind::Int),
    ("last_enabled_at", Kind::Time),
    ("name", Kind::Text),
    ("pipeline_uuid", Kind::Text),
    ("repo_path", Kind::Text),
    ("schedule_interval", Kind::Text),
    ("schedule_type", Kind::Enum),
    ("settings", Kind::Json),
    ("sla", Kind::Int),
    ("start_time", Kind::Time),
    ("status", Kind::Enum),
    ("token", Kind::Text),
    ("updated_at", Kind::Time),
    ("variables", Kind::Json),
];
const START_TIME: usize = 12;

/// Every column of event_matcher, as BaseModel.to_dict gives them.
const EVENT_MATCHER_FIELDS: &[(&str, Kind)] = &[
    ("id", Kind::Int),
    ("created_at", Kind::Time),
    ("updated_at", Kind::Time),
    ("event_type", Kind::Enum),
    ("name", Kind::Text),
    ("pattern", Kind::Json),
];

const SCHEDULE_TYPES: &[&str] = &["API", "EVENT", "TIME", "AFTER_RUN"];
const SCHEDULE_STATUSES: &[&str] = &["ACTIVE", "INACTIVE"];

/// What the list needs besides the query: where it is read and who reads it.
pub(crate) struct Lister<'a> {
    pub metadata: &'a Metadata,
    pub repo_path: &'a std::path::Path,
    /// The pipeline of /api/pipelines/<uuid>/pipeline_schedules.
    pub pipeline: Option<&'a str>,
    /// HIDE_API_TRIGGER_TOKEN.
    pub hide_token: bool,
    pub now: DateTime<Utc>,
}

fn unsupported(why: &str) -> String {
    format!("{UNSUPPORTED}{why}")
}

/// Every value of a name, as Tornado keeps them: blank ones dropped.
fn all_values(query: &str, name: &str) -> Vec<String> {
    query
        .split('&')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = crate::gateway_shadow::percent_decode(value);
            (crate::gateway_shadow::percent_decode(key) == name && !value.is_empty())
                .then_some(value)
        })
        .collect()
}

/// An Enum column filter's values as stored: SQLAlchemy takes a member's value or its name
/// and refuses anything else, which fails the server's request.
fn enum_names(text: &str, names: &[&str]) -> Result<Vec<String>, String> {
    text.split(',')
        .map(|value| {
            names
                .iter()
                .find(|name| **name == value || name.to_lowercase() == value)
                .map(|name| name.to_string())
                .ok_or_else(|| unsupported("a value the server's Enum refuses"))
        })
        .collect()
}

pub(crate) async fn pipeline_schedules(
    lister: &Lister<'_>,
    raw_query: &str,
) -> Result<Value, String> {
    let query = Query::parse(raw_query);
    let metadata = lister.metadata;
    let number = |name: &str, default: i64| -> Result<i64, String> {
        match query.get(name) {
            Some(text) => match text.parse::<i64>() {
                Ok(n) if n >= 0 => Ok(n),
                _ => Err(unsupported("a _limit or _offset Python slices otherwise")),
            },
            None => Ok(default),
        }
    };
    let limit = number("_limit", DEFAULT_LIMIT)?;
    let offset = number("_offset", 0)?;

    let mut filters = Vec::new();
    let mut params: Vec<Raw> = Vec::new();
    let mut param = |raw: Raw| {
        params.push(raw);
        format!("{{{}}}", params.len())
    };
    // PipelineSchedule.repo_query.
    let p = param(Raw::Text(lister.repo_path.display().to_string()));
    filters.push(format!("(s.repo_path = {p} OR s.repo_path IS NULL)"));

    let tags = all_values(raw_query, "tag[]");
    if !tags.is_empty() {
        if tags.iter().any(|t| t == "true" || t == "false") {
            return Err(unsupported("a tag the operation reads as a bool"));
        }
        let ps: Vec<String> = tags.into_iter().map(|t| param(Raw::Text(t))).collect();
        filters.push(format!(
            "s.id IN (SELECT ta.taggable_id FROM tag_association ta JOIN tag t \
             ON t.id = ta.tag_id WHERE t.name IN ({}) AND ta.taggable_type = 'PipelineSchedule')",
            ps.join(", ")
        ));
    }

    // Triggers whose name or pipeline contains the text, ignoring case.
    if query.get("search") == Some("true") {
        return Err(unsupported("search=true, which the server fails on"));
    }
    let search = query.get("search").unwrap_or_default().trim();
    if !search.is_empty() {
        let p = param(Raw::Text(format!("%{search}%")));
        filters.push(match metadata {
            Metadata::Sqlite(_) => {
                format!("(lower(s.name) LIKE lower({p}) OR lower(s.pipeline_uuid) LIKE lower({p}))")
            }
            Metadata::Postgres(_) => format!("(s.name ILIKE {p} OR s.pipeline_uuid ILIKE {p})"),
        });
    }

    let text = |column: &str| match metadata {
        Metadata::Postgres(_) => format!("CAST(s.{column} AS TEXT)"),
        Metadata::Sqlite(_) => format!("s.{column}"),
    };
    for (name, column, names) in [
        ("schedule_type[]", "schedule_type", Some(SCHEDULE_TYPES)),
        ("schedule_interval[]", "schedule_interval", None),
        ("status[]", "status", Some(SCHEDULE_STATUSES)),
    ] {
        let Some(value) = query.get(name) else {
            continue;
        };
        if value == "true" {
            return Err(unsupported("a filter the operation reads as a bool"));
        }
        let values = match names {
            Some(names) => enum_names(value, names)?,
            None => value.split(',').map(str::to_string).collect(),
        };
        let ps: Vec<String> = values.into_iter().map(|v| param(Raw::Text(v))).collect();
        filters.push(format!("{} IN ({})", text(column), ps.join(", ")));
    }

    let order = if let Some(product) = query.get("global_data_product_uuid") {
        let p = param(Raw::Text(product.to_string()));
        filters.push(format!("s.global_data_product_uuid = {p}"));
        "s.id DESC".to_string()
    } else if let Some(pipeline) = lister.pipeline {
        let p = param(Raw::Text(pipeline.to_string()));
        filters.push(format!(
            "s.global_data_product_uuid IS NULL AND s.pipeline_uuid = {p}"
        ));
        "s.id DESC".to_string()
    } else {
        // Ties, and no order at all, by id: pages are slices of this list.
        match query.get("order_by") {
            Some("created_at") => "s.created_at DESC, s.id DESC".to_string(),
            Some(column @ ("name" | "pipeline_uuid" | "status" | "schedule_type")) => {
                format!("s.{column} ASC, s.id ASC")
            }
            _ => "s.id ASC".to_string(),
        }
    };

    let from = format!("FROM pipeline_schedule s WHERE {}", filters.join(" AND "));
    let count = match read_rows(metadata, &format!("SELECT count(*) {from}"), &params)
        .await?
        .first()
        .and_then(|r| r.first())
    {
        Some(Raw::Int(n)) => *n,
        _ => 0,
    };
    // The server works out every trigger's next run date, not only the page's: one it
    // cannot (croniter raising) fails its whole answer.
    for row in read_rows(
        metadata,
        &format!("SELECT DISTINCT s.schedule_interval {from}"),
        &params,
    )
    .await?
    {
        if let Some(Raw::Text(interval)) = row.first() {
            Interval::parse(interval)?;
        }
    }

    let columns: Vec<String> = SCHEDULE_FIELDS
        .iter()
        .map(|(name, kind)| match (metadata, kind) {
            (Metadata::Postgres(_), Kind::Enum | Kind::Json) => format!("CAST(s.{name} AS TEXT)"),
            _ => format!("s.{name}"),
        })
        .collect();
    let rows = read_rows(
        metadata,
        &format!(
            "SELECT {} {from} ORDER BY {order} LIMIT {} OFFSET {offset}",
            columns.join(", "),
            limit + 1
        ),
        &params,
    )
    .await?;
    let has_next = rows.len() as i64 > limit;
    let rows: Vec<Vec<Raw>> = rows.into_iter().take(limit as usize).collect();
    let ids: Vec<Raw> = rows.iter().map(|r| r[3].clone()).collect();

    let mut runs: BTreeMap<i64, (Value, Value, i64)> = BTreeMap::new();
    let mut tags_by_id: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    let mut matchers: BTreeMap<i64, Vec<Value>> = BTreeMap::new();
    let mut last_status: BTreeMap<i64, Value> = BTreeMap::new();
    if !ids.is_empty() {
        let list = (1..=ids.len())
            .map(|n| format!("{{{n}}}"))
            .collect::<Vec<_>>()
            .join(", ");
        let status = match metadata {
            Metadata::Postgres(_) => "CAST(status AS TEXT)",
            Metadata::Sqlite(_) => "status",
        };
        for row in read_rows(
            metadata,
            &format!(
                "SELECT pipeline_schedule_id, count(pipeline_schedule_id), \
                 sum(CASE WHEN {status} IN ('INITIAL', 'RUNNING') THEN 1 ELSE 0 END), max(id) \
                 FROM pipeline_run WHERE pipeline_schedule_id IN ({list}) \
                 GROUP BY pipeline_schedule_id"
            ),
            &ids,
        )
        .await?
        {
            if let [Raw::Int(id), Raw::Int(count), in_progress, last] = row.as_slice() {
                let in_progress = match in_progress {
                    Raw::Int(n) => Value::from(*n),
                    // sum() of PostgreSQL integers is bigint; of no rows, null.
                    Raw::Text(n) => Value::from(n.parse::<i64>().unwrap_or_default()),
                    _ => Value::Null,
                };
                let last = match last {
                    Raw::Int(last) => *last,
                    _ => 0,
                };
                runs.insert(*id, (Value::from(*count), in_progress, last));
            }
        }
        let last_ids: Vec<Raw> = runs
            .values()
            .filter(|(_, _, last)| *last != 0)
            .map(|(_, _, last)| Raw::Int(*last))
            .collect();
        if !last_ids.is_empty() {
            let list = (1..=last_ids.len())
                .map(|n| format!("{{{n}}}"))
                .collect::<Vec<_>>()
                .join(", ");
            for row in read_rows(
                metadata,
                &format!("SELECT id, {status} FROM pipeline_run WHERE id IN ({list})"),
                &last_ids,
            )
            .await?
            {
                if let [Raw::Int(id), status] = row.as_slice() {
                    last_status.insert(*id, field_value(Kind::Enum, status.clone())?);
                }
            }
        }
        for row in read_rows(
            metadata,
            &format!(
                "SELECT ta.taggable_id, t.name FROM tag_association ta JOIN tag t \
                 ON t.id = ta.tag_id WHERE ta.taggable_id IN ({list}) \
                 AND ta.taggable_type = 'PipelineSchedule'"
            ),
            &ids,
        )
        .await?
        {
            if let [Raw::Int(id), Raw::Text(name)] = row.as_slice() {
                tags_by_id.entry(*id).or_default().push(name.clone());
            }
        }
        let matcher_columns: Vec<String> = EVENT_MATCHER_FIELDS
            .iter()
            .map(|(name, kind)| match (metadata, kind) {
                (Metadata::Postgres(_), Kind::Enum | Kind::Json) => {
                    format!("CAST(e.{name} AS TEXT)")
                }
                _ => format!("e.{name}"),
            })
            .collect();
        for mut row in read_rows(
            metadata,
            &format!(
                "SELECT a.pipeline_schedule_id, {} FROM pipeline_schedule_event_matcher_association a \
                 JOIN event_matcher e ON e.id = a.event_matcher_id \
                 WHERE a.pipeline_schedule_id IN ({list}) ORDER BY e.id",
                matcher_columns.join(", ")
            ),
            &ids,
        )
        .await?
        {
            let Raw::Int(id) = row.remove(0) else {
                continue;
            };
            matchers
                .entry(id)
                .or_default()
                .push(Value::Object(object(EVENT_MATCHER_FIELDS, row)?));
        }
    }

    let mut schedules = Vec::new();
    for raws in rows {
        let start_time = match &raws[START_TIME] {
            Raw::Time(at) => Some(*at),
            Raw::Text(text) => Some(
                crate::gateway_auth::naive_utc(text).ok_or_else(|| format!("start_time {text}"))?,
            ),
            _ => None,
        };
        let mut schedule = object(SCHEDULE_FIELDS, raws)?;
        let id = schedule["id"].as_i64().unwrap_or_default();
        let next = next_execution_date(
            schedule["schedule_interval"].as_str(),
            schedule["schedule_type"].as_str(),
            &schedule["settings"],
            start_time,
            lister.now,
        )?;
        let (count, in_progress, last) = match runs.get(&id) {
            Some((count, in_progress, last)) => (
                count.clone(),
                in_progress.clone(),
                last_status.get(last).cloned().unwrap_or(Value::Null),
            ),
            None => (Value::Null, Value::Null, Value::Null),
        };
        let mut tags = tags_by_id.remove(&id).unwrap_or_default();
        tags.sort();
        schedule.insert(
            "event_matchers".into(),
            Value::Array(matchers.remove(&id).unwrap_or_default()),
        );
        schedule.insert("last_pipeline_run_status".into(), last);
        schedule.insert(
            "next_pipeline_run_date".into(),
            next.map(str_time_utc).unwrap_or(Value::Null),
        );
        schedule.insert("pipeline_in_progress_runs_count".into(), in_progress);
        schedule.insert("pipeline_runs_count".into(), count);
        schedule.insert("tags".into(), json!(tags));
        if lister.hide_token {
            schedule.insert("token".into(), json!("[API_TOKEN_PLACEHOLDER]"));
        }
        schedules.push(Value::Object(schedule));
    }
    let mut meta = Map::new();
    meta.insert("count".into(), Value::from(count));
    meta.insert("next".into(), Value::Bool(has_next));
    Ok(json!({ "pipeline_schedules": schedules, "metadata": meta }))
}

/// A schedule_interval as PipelineSchedule reads it: MageML's presets, or a cron
/// expression for croniter.
enum Interval {
    Once,
    AlwaysOn,
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Cron(Box<croner::Cron>),
}

impl Interval {
    /// None for no interval ("" or null). The gateway reads the cron expressions on which
    /// croner and croniter agree: five fields of digits, `*`, `/`, `-` and `,`, and not
    /// both a day of the month and a day of the week, which croniter joins with OR.
    fn parse(text: &str) -> Result<Option<Interval>, String> {
        Ok(Some(match text {
            "" => return Ok(None),
            "@once" => Interval::Once,
            "@always_on" => Interval::AlwaysOn,
            "@hourly" => Interval::Hourly,
            "@daily" => Interval::Daily,
            "@weekly" => Interval::Weekly,
            "@monthly" => Interval::Monthly,
            cron => {
                let fields: Vec<&str> = cron.split_whitespace().collect();
                let simple = fields.len() == 5
                    && fields.iter().all(|f| {
                        f.bytes()
                            .all(|b| b.is_ascii_digit() || b"*/,-".contains(&b))
                    })
                    && (fields[2] == "*" || fields[4] == "*");
                if !simple {
                    return Err(unsupported("a schedule interval the gateway does not read"));
                }
                Interval::Cron(Box::new(
                    croner::Cron::from_str(cron)
                        .map_err(|_| unsupported("a cron expression croner refuses"))?,
                ))
            }
        }))
    }
}

/// The last occurrence strictly before `at`, as croniter's get_prev.
fn cron_before(cron: &croner::Cron, at: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let whole = at.with_nanosecond(0).unwrap_or(at);
    cron.find_previous_occurrence(&whole, whole != at)
        .map_err(|e| unsupported(&format!("croner: {e}")))
}

/// The first occurrence strictly after `at`, as croniter's get_next.
fn cron_after(cron: &croner::Cron, at: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let whole = at.with_nanosecond(0).unwrap_or(at);
    cron.find_next_occurrence(&whole, false)
        .map_err(|e| unsupported(&format!("croner: {e}")))
}

fn midnight(at: DateTime<Utc>) -> DateTime<Utc> {
    at.date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap_or_default()
        .and_utc()
}

fn with_clock(at: DateTime<Utc>, hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    at.date_naive()
        .and_hms_opt(hour, minute, second)
        .unwrap_or_default()
        .and_utc()
        + Duration::nanoseconds(at.nanosecond() as i64)
}

/// Python's truth of a JSON value.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

struct Schedule<'a> {
    interval: &'a Interval,
    landing: Option<DateTime<Utc>>,
}

impl Schedule<'_> {
    /// PipelineSchedule.current_execution_date(now).
    fn current(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
        let current = match self.interval {
            Interval::Once | Interval::AlwaysOn => now,
            Interval::Daily => midnight(now),
            Interval::Hourly => {
                with_clock(now, now.hour(), 0, 0) - Duration::nanoseconds(now.nanosecond() as i64)
            }
            Interval::Weekly => {
                midnight(now) - Duration::days(now.weekday().num_days_from_monday() as i64)
            }
            Interval::Monthly => midnight(now.with_day(1).unwrap_or(now)),
            Interval::Cron(cron) => cron_before(cron, now)?,
        };
        let Some(start) = self.landing else {
            return Ok(current);
        };
        Ok(match self.interval {
            Interval::Hourly => with_clock(current, current.hour(), start.minute(), start.second()),
            Interval::Daily => with_clock(current, start.hour(), start.minute(), start.second()),
            Interval::Weekly => {
                with_clock(current, start.hour(), start.minute(), start.second())
                    + Duration::days(start.weekday().num_days_from_monday() as i64)
            }
            Interval::Monthly => {
                with_clock(current, start.hour(), start.minute(), start.second())
                    + Duration::days(start.day() as i64 - 1)
            }
            _ => current,
        })
    }

    /// The execution date after this one.
    fn after(&self, at: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
        Ok(match self.interval {
            Interval::Daily => at + Duration::days(1),
            Interval::Hourly => at + Duration::hours(1),
            Interval::Weekly => at + Duration::weeks(1),
            Interval::Monthly => {
                let next = at.with_day(1).unwrap_or(at) + Duration::days(32);
                let first = next.with_day(1).unwrap_or(next);
                if self.landing.is_some() {
                    // The same day of the next month, as current_execution_date counts it.
                    self.current(first)?
                } else {
                    first
                }
            }
            Interval::Cron(cron) => cron_after(cron, at)?,
            Interval::Once | Interval::AlwaysOn => at,
        })
    }
}

/// PipelineSchedule.next_execution_date at `now`.
fn next_execution_date(
    interval: Option<&str>,
    schedule_type: Option<&str>,
    settings: &Value,
    start_time: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    let Some(interval) = Interval::parse(interval.unwrap_or_default())? else {
        return Ok(None);
    };
    if matches!(interval, Interval::Once | Interval::AlwaysOn) {
        return Ok(None);
    }
    // landing_time_enabled.
    let landing = schedule_type == Some("time")
        && matches!(
            interval,
            Interval::Hourly | Interval::Daily | Interval::Weekly | Interval::Monthly
        )
        && truthy(settings.get("landing_time_enabled").unwrap_or(&Value::Null));
    let schedule = Schedule {
        interval: &interval,
        landing: start_time.filter(|_| landing),
    };
    let current = schedule.current(now)?;
    if let Some(start) = schedule.landing {
        if now < start {
            return Ok(Some(start));
        }
        if current > now {
            return Ok(Some(current));
        }
        return schedule.after(current).map(Some);
    }
    if let Some(start) = start_time
        && now < start
    {
        let first = schedule.current(start)?;
        return Ok(Some(if first < start {
            schedule.after(first)?
        } else {
            first
        }));
    }
    schedule.after(current).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().to_utc()
    }

    /// From croniter: croniter(e, croniter(e, now).get_prev()).get_next() at now.
    #[test]
    fn cron_next_dates_are_croniters() {
        let now = utc("2023-08-19T20:10:15.25Z");
        for (expression, expected) in [
            ("*/15 * * * *", "2023-08-19T20:15:00+00:00"),
            ("0 0 * * 7", "2023-08-20T00:00:00+00:00"),
            ("0 0 * * 0", "2023-08-20T00:00:00+00:00"),
            ("30 9 1-5 * *", "2023-09-01T09:30:00+00:00"),
            ("0 12 * * 1-5", "2023-08-21T12:00:00+00:00"),
            ("5 4 * 2 *", "2024-02-01T04:05:00+00:00"),
            ("0 0 29 2 *", "2024-02-29T00:00:00+00:00"),
            ("0 */6 * * *", "2023-08-20T00:00:00+00:00"),
            ("10 20 * * *", "2023-08-20T20:10:00+00:00"),
            ("0-30/10 20 19 8 *", "2023-08-19T20:20:00+00:00"),
        ] {
            let next = next_execution_date(Some(expression), Some("time"), &Value::Null, None, now);
            assert_eq!(next, Ok(Some(utc(expected))), "{expression}");
        }
        // croniter's get_prev is strictly before, the fraction counting.
        let cron = croner::Cron::from_str("30 9 * * *").unwrap();
        assert_eq!(
            cron_before(&cron, utc("2023-01-01T09:30:00.5Z")),
            Ok(utc("2023-01-01T09:30:00Z"))
        );
        assert_eq!(
            cron_before(&cron, utc("2023-01-01T09:30:00Z")),
            Ok(utc("2022-12-31T09:30:00Z"))
        );
    }

    /// The cases of test_next_execution_date and
    /// test_next_execution_date_waits_for_start_and_landing_time (test_schedules.py).
    #[test]
    fn next_dates_are_pythons() {
        let now = utc("2023-08-19T20:10:15Z");
        let landing = json!({"landing_time_enabled": true});
        let off = json!({"landing_time_enabled": false});
        let past = Some(utc("2023-08-19T19:14:15Z"));
        for (interval, start, settings, expected) in [
            ("@once", past, &off, None),
            ("@always_on", past, &off, None),
            ("", past, &off, None),
            ("@hourly", past, &off, Some("2023-08-19T21:00:00Z")),
            ("@daily", past, &off, Some("2023-08-20T00:00:00Z")),
            ("@weekly", past, &off, Some("2023-08-21T00:00:00Z")),
            ("@monthly", past, &off, Some("2023-09-01T00:00:00Z")),
            ("30 9 * 8 *", past, &off, Some("2023-08-20T09:30:00Z")),
            (
                "@daily",
                Some(utc("2023-08-01T22:25:00Z")),
                &landing,
                Some("2023-08-19T22:25:00Z"),
            ),
            (
                "@hourly",
                Some(utc("2023-08-01T05:30:00Z")),
                &landing,
                Some("2023-08-19T20:30:00Z"),
            ),
            (
                "@monthly",
                Some(utc("2023-07-25T06:00:00Z")),
                &landing,
                Some("2023-08-25T06:00:00Z"),
            ),
            (
                "@daily",
                Some(utc("2023-08-01T19:00:00Z")),
                &landing,
                Some("2023-08-20T19:00:00Z"),
            ),
            (
                "@weekly",
                Some(utc("2023-08-04T09:00:00Z")),
                &landing,
                Some("2023-08-25T09:00:00Z"),
            ),
            (
                "@monthly",
                Some(utc("2023-07-10T06:00:00Z")),
                &landing,
                Some("2023-09-10T06:00:00Z"),
            ),
            (
                "@daily",
                Some(utc("2023-09-01T22:25:00Z")),
                &landing,
                Some("2023-09-01T22:25:00Z"),
            ),
            (
                "@daily",
                Some(utc("2023-09-01T10:00:00Z")),
                &off,
                Some("2023-09-02T00:00:00Z"),
            ),
            (
                "@daily",
                Some(utc("2023-09-01T00:00:00Z")),
                &off,
                Some("2023-09-01T00:00:00Z"),
            ),
            (
                "@monthly",
                Some(utc("2023-09-01T10:00:00Z")),
                &off,
                Some("2023-10-01T00:00:00Z"),
            ),
            (
                "30 9 * * *",
                Some(utc("2023-09-01T09:30:00Z")),
                &off,
                Some("2023-09-01T09:30:00Z"),
            ),
            (
                "30 9 * * *",
                Some(utc("2023-09-01T09:31:00Z")),
                &off,
                Some("2023-09-02T09:30:00Z"),
            ),
        ] {
            assert_eq!(
                next_execution_date(Some(interval), Some("time"), settings, start, now),
                Ok(expected.map(utc)),
                "{interval} {start:?} {settings}"
            );
        }
        // Landing time is for time triggers only.
        assert_eq!(
            next_execution_date(
                Some("@daily"),
                Some("api"),
                &landing,
                Some(utc("2023-08-01T22:25:00Z")),
                now
            ),
            Ok(Some(utc("2023-08-20T00:00:00Z")))
        );
    }

    #[test]
    fn intervals_the_gateway_leaves_to_the_server() {
        for interval in [
            "@yearly",
            "0 0 1 * MON",
            "0 0 1 * 1",
            "0 0 L * *",
            "* * * * * *",
            "H * * * *",
        ] {
            assert!(
                Interval::parse(interval).is_err_and(|e| e.starts_with(UNSUPPORTED)),
                "{interval}"
            );
        }
        assert!(matches!(
            Interval::parse("0 0 1 * *"),
            Ok(Some(Interval::Cron(_)))
        ));
    }

    #[test]
    fn filters_take_enum_values_or_names() {
        assert_eq!(
            enum_names("after_run,TIME", SCHEDULE_TYPES),
            Ok(vec!["AFTER_RUN".to_string(), "TIME".to_string()])
        );
        assert!(enum_names("After_Run", SCHEDULE_TYPES).is_err());
        assert!(enum_names("active,", SCHEDULE_STATUSES).is_err());
        assert_eq!(
            all_values("tag[]=a&tag[]=b%20c&tag[]=&x=1", "tag[]"),
            vec!["a".to_string(), "b c".to_string()]
        );
    }
}
