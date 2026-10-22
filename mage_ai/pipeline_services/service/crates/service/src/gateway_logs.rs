//! GET /api/pipelines/<uuid>/logs, the logs page, as LogResource answers it for logs kept
//! on the server's disk: the logs of the pipeline's runs and block runs, its blocks' editor
//! logs, a text search over them, and the lines of one log before those the page shows.
//! Each log is read as one text over its rotated segments (mage-log-window, as
//! log_windows.py reads it through column_atlas_native).

use std::path::{Component, Path, PathBuf};

use serde_json::{Map, Value, json};

use crate::gateway_schedules::UNSUPPORTED;
use crate::gateway_shadow::{Metadata, Query, Raw, read_rows};

const MAX_LOG_FILES: usize = 20;
const MAX_SEARCH_LOG_FILES: usize = 200;
const LOG_PARTITION_EDIT_PIPELINE: &str = "edit_pipeline";
const LOGS_SUBDIR: &str = ".logs";

pub(crate) struct LogLister<'a> {
    pub metadata: &'a Metadata,
    pub repo_path: &'a Path,
    /// LOGS_DIR_PATH, or the project's variables_dir (MAGE_GATEWAY_LOGS_DIR).
    pub logs_dir: &'a str,
    pub pipeline: &'a str,
}

fn unsupported(why: &str) -> String {
    format!("{UNSUPPORTED}{why}")
}

/// `int(os.getenv(name) or default)`, as log_windows.py reads its sizes.
fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// posixpath.join.
fn py_join(parts: &[&str]) -> String {
    let mut path = String::new();
    for (i, part) in parts.iter().enumerate() {
        if part.starts_with('/') || i == 0 {
            path = part.to_string();
        } else if path.is_empty() || path.ends_with('/') {
            path.push_str(part);
        } else {
            path.push('/');
            path.push_str(part);
        }
    }
    path
}

/// clean_name for the ASCII names the gateway reads: `"`, `$` and whitespace controls
/// removed, every other non-word character an underscore, `letter_` before a leading
/// digit, lower case.
fn clean_name(name: &str) -> Result<String, String> {
    if !name.is_ascii() {
        return Err(unsupported("a block uuid that is not ASCII"));
    }
    let mut cleaned: String = name
        .chars()
        .filter(|c| !matches!(c, '"' | '$' | '\n' | '\r' | '\t'))
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.starts_with(|c: char| c.is_ascii_digit()) {
        cleaned = format!("letter_{cleaned}");
    }
    Ok(cleaned.to_lowercase())
}

/// The path's segments as strings, oldest first.
fn segments(path: &str) -> Vec<String> {
    mage_log_window::segment_paths(Path::new(path))
        .iter()
        .map(|p| p.to_string_lossy().into_owned())
        .collect()
}

/// log_windows._base.
fn base(path: &str) -> Map<String, Value> {
    let file = mage_log_window::log_file(Path::new(path));
    let mut item = Map::new();
    item.insert(
        "name".into(),
        json!(path.rsplit('/').next().unwrap_or_default()),
    );
    item.insert("path".into(), json!(path));
    item.insert("size".into(), json!(file.size));
    item.insert("modified_timestamp".into(), json!(file.modified_timestamp));
    item.insert("segments".into(), json!(file.segments));
    item
}

/// log_windows.window.
fn window(path: &str, before_line: Option<u64>, lines: u64) -> Result<Value, String> {
    let mut item = base(path);
    let found = mage_log_window::window(&segments(path), before_line, lines)?;
    item.insert("content".into(), json!(found.text));
    item.insert("first_line".into(), json!(found.first_line));
    item.insert("total_lines".into(), json!(found.total_lines));
    Ok(Value::Object(item))
}

fn window_lines() -> u64 {
    env_number("MAGE_LOG_WINDOW_LINES", 5_000)
}

/// matching_lines: a local log searched over all its segments, or None without a match.
fn matching(item: Value, text: &str) -> Result<Option<Value>, String> {
    let path = item["path"].as_str().unwrap_or_default().to_string();
    if !Path::new(&path).is_file() {
        // The item's own lines, split as str.splitlines splits them: left to the server.
        if item["content"].as_str().is_some_and(|c| !c.is_empty()) {
            return Err(unsupported("a log whose newest segment is gone"));
        }
        return Ok(None);
    }
    let limit = env_number("MAGE_LOG_SEARCH_LINES", 20_000) as usize;
    let found = mage_log_window::search(&segments(&path), text, limit)?;
    if found.is_empty() {
        return Ok(None);
    }
    let mut item = base(&path);
    item.insert(
        "content".into(),
        json!(
            found
                .iter()
                .map(|m| m.text.as_str())
                .collect::<Vec<_>>()
                .join("\n")
        ),
    );
    item.insert(
        "matched_lines".into(),
        json!(found.iter().map(|m| m.line).collect::<Vec<_>>()),
    );
    Ok(Some(Value::Object(item)))
}

/// The ids of a `name[]` filter: its first value split on commas.
fn ids(query: &Query, name: &str) -> Result<Vec<i64>, String> {
    match query.get(name) {
        None => Ok(Vec::new()),
        Some(text) => text
            .split(',')
            .map(|id| {
                id.trim()
                    .parse::<i64>()
                    .map_err(|_| unsupported("an id the server's query fails on"))
            })
            .collect(),
    }
}

/// get_query_timestamps: Unix seconds as a UTC time without a zone.
fn timestamp(query: &Query, name: &str) -> Result<Option<chrono::NaiveDateTime>, String> {
    let Some(text) = query.get(name) else {
        return Ok(None);
    };
    let seconds = text
        .trim()
        .parse::<i64>()
        .map_err(|_| unsupported("a timestamp the server refuses"))?;
    chrono::DateTime::from_timestamp(seconds, 0)
        .map(|at| Some(at.naive_utc()))
        .ok_or_else(|| unsupported("a timestamp the server refuses"))
}

/// PipelineRun.execution_partition.
fn execution_partition(
    variables: &Raw,
    schedule_id: &Raw,
    execution_date: &Raw,
) -> Result<String, String> {
    if let Raw::Text(text) = variables
        && let Ok(Value::Object(variables)) = serde_json::from_str::<Value>(text)
    {
        match variables.get("execution_partition") {
            Some(Value::String(partition)) if !partition.is_empty() => {
                return Ok(partition.clone());
            }
            None | Some(Value::Null) | Some(Value::String(_)) | Some(Value::Bool(false)) => {}
            Some(_) => return Err(unsupported("an execution_partition that is not text")),
        }
    }
    let schedule = match schedule_id {
        Raw::Int(id) => id.to_string(),
        _ => "None".to_string(),
    };
    let at = match execution_date {
        Raw::Null => return Ok(schedule),
        Raw::Time(at) => *at,
        Raw::Text(text) => {
            crate::gateway_auth::naive_utc(text).ok_or_else(|| format!("execution_date {text}"))?
        }
        _ => return Err("execution_date of another type".into()),
    };
    Ok(format!("{schedule}/{}", at.format("%Y%m%dT%H%M%S")))
}

/// Path::resolve(strict=False) of an absolute, lexically normal path: the part that exists
/// with its symlinks resolved, then the rest.
fn resolve(path: &Path) -> Option<PathBuf> {
    let mut existing = path.to_path_buf();
    let mut rest = Vec::new();
    loop {
        if let Ok(found) = std::fs::canonicalize(&existing) {
            return Some(rest.iter().rev().fold(found, |p, part| p.join(part)));
        }
        rest.push(existing.file_name()?.to_os_string());
        existing = existing.parent()?.to_path_buf();
    }
}

/// os.path.normpath of an absolute path.
fn normal(path: &str) -> Option<PathBuf> {
    let mut out = PathBuf::from("/");
    for component in Path::new(path).components() {
        match component {
            Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            Component::Normal(part) => out.push(part),
            Component::Prefix(_) => return None,
        }
    }
    Some(out)
}

/// ensure_path_is_in_directory: the path resolved, or None when it is outside.
fn inside(path: &str, directory: &str) -> Result<Option<PathBuf>, String> {
    // expandvars, expanduser and a relative path (the server's working directory): left
    // to the server.
    if path.contains('$') || path.starts_with('~') || !path.starts_with('/') {
        return Err(unsupported("a log_path the gateway does not resolve"));
    }
    let (Some(full), Some(dir)) = (
        normal(path).and_then(|p| resolve(&p)),
        normal(directory).and_then(|p| resolve(&p)),
    ) else {
        return Err(unsupported("a log_path the gateway does not resolve"));
    };
    Ok(full.starts_with(&dir).then_some(full))
}

pub(crate) async fn pipeline_logs(
    lister: &LogLister<'_>,
    raw_query: &str,
) -> Result<Value, String> {
    let query = Query::parse(raw_query);
    let uuid = lister.pipeline;
    let config = crate::gateway_shadow::pipeline_config(lister.repo_path, uuid)
        .ok_or_else(|| unsupported("a pipeline the server cannot load"))?;
    let blocks = match (&config.blocks, config.execution_framework) {
        (Some(blocks), false) => {
            // blocks_by_uuid: one per uuid, where it first appears.
            let mut unique: Vec<String> = Vec::new();
            for block in blocks {
                if !unique.contains(block) {
                    unique.push(block.clone());
                }
            }
            unique
        }
        _ => return Err(unsupported("a pipeline the gateway does not read")),
    };
    let logs_root = py_join(&[lister.logs_dir, "pipelines", uuid, LOGS_SUBDIR]);

    // The lines of one log before a line the page shows.
    let log_path = query.get("log_path").unwrap_or_default().trim().to_string();
    if !log_path.is_empty() {
        let number = |name: &str| -> Result<Option<u64>, String> {
            match query.get(name) {
                None => Ok(None),
                Some(text) => text
                    .trim()
                    .parse::<u64>()
                    .map(Some)
                    .map_err(|_| unsupported("a number the server reads otherwise")),
            }
        };
        let Some(path) = inside(&log_path, &logs_root)? else {
            return Err(unsupported("a log_path outside the pipeline's logs"));
        };
        let item = window(
            &path.to_string_lossy(),
            number("before_line")?,
            number("lines")?.unwrap_or_else(window_lines),
        )?;
        return Ok(answer(vec![item], Vec::new(), 1, 0));
    }

    let text = query.get("text").unwrap_or_default().trim().to_string();
    let max_files = if text.is_empty() {
        MAX_LOG_FILES
    } else {
        MAX_SEARCH_LOG_FILES
    };
    let keep = |item: Value| -> Result<Option<Value>, String> {
        if text.is_empty() {
            Ok(Some(item))
        } else {
            matching(item, &text)
        }
    };
    let start = timestamp(&query, "start_timestamp")?;
    let end = timestamp(&query, "end_timestamp")?;
    let schedule_ids = ids(&query, "pipeline_schedule_id[]")?;
    let run_ids = ids(&query, "pipeline_run_id[]")?;
    let block_run_ids = ids(&query, "block_run_id[]")?;
    if query.get("block_uuid[]") == Some("true") {
        return Err(unsupported("block_uuid[]=true, which the server fails on"));
    }
    let block_uuids: Vec<String> = query
        .get("block_uuid[]")
        .map(|t| t.split(',').map(str::to_string).collect())
        .unwrap_or_default();
    let limit = match query.get("_limit") {
        None => None,
        Some(text) => Some(
            text.trim()
                .parse::<u64>()
                .map_err(|_| unsupported("a _limit the server reads otherwise"))?,
        ),
    };

    let metadata = lister.metadata;
    // The filters both queries share: the pipeline's triggers, schedules, runs, times.
    let shared = |params: &mut Vec<Raw>| -> Vec<String> {
        let mut param = |raw: Raw| {
            params.push(raw);
            format!("{{{}}}", params.len())
        };
        let mut filters = vec![format!(
            "b.pipeline_uuid = {}",
            param(Raw::Text(uuid.into()))
        )];
        if !schedule_ids.is_empty() {
            let ps: Vec<String> = schedule_ids.iter().map(|id| param(Raw::Int(*id))).collect();
            filters.push(format!("a.pipeline_schedule_id IN ({})", ps.join(", ")));
        }
        if !run_ids.is_empty() {
            let ps: Vec<String> = run_ids.iter().map(|id| param(Raw::Int(*id))).collect();
            filters.push(format!("a.id IN ({})", ps.join(", ")));
        }
        for (at, operator) in [(start, ">="), (end, "<=")] {
            if let Some(at) = at {
                let raw = match metadata {
                    Metadata::Sqlite(_) => {
                        Raw::Text(at.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
                    }
                    Metadata::Postgres(_) => Raw::Time(at.and_utc()),
                };
                filters.push(format!("a.execution_date {operator} {}", param(raw)));
            }
        }
        filters
    };
    let limit_sql = limit.map(|n| format!(" LIMIT {n}")).unwrap_or_default();
    let run_path = |run_pipeline: &Raw, partition: &str, file: &str| -> Result<String, String> {
        match run_pipeline {
            Raw::Text(p) if p == uuid => Ok(py_join(&[
                lister.logs_dir,
                "pipelines",
                uuid,
                LOGS_SUBDIR,
                partition,
                "",
                file,
            ])),
            // Python loads the run's own pipeline, which may be another or gone.
            _ => Err(unsupported("a run of another pipeline")),
        }
    };

    let mut total_pipeline_run_log_count = 0;
    let mut pipeline_run_logs = Vec::new();
    if block_uuids.is_empty() && block_run_ids.is_empty() {
        let mut params = Vec::new();
        let filters = shared(&mut params);
        let from = format!(
            "FROM pipeline_run a JOIN pipeline_schedule b ON a.pipeline_schedule_id = b.id \
             WHERE {}",
            filters.join(" AND ")
        );
        total_pipeline_run_log_count =
            match read_rows(metadata, &format!("SELECT count(*) {from}"), &params)
                .await?
                .first()
                .and_then(|r| r.first())
            {
                Some(Raw::Int(n)) => *n,
                _ => 0,
            };
        let variables = match metadata {
            Metadata::Postgres(_) => "CAST(a.variables AS TEXT)",
            Metadata::Sqlite(_) => "a.variables",
        };
        let rows = read_rows(
            metadata,
            &format!(
                "SELECT a.execution_date, a.pipeline_schedule_id, a.pipeline_uuid, {variables} \
                 {from} ORDER BY a.created_at DESC, a.id DESC{limit_sql}"
            ),
            &params,
        )
        .await?;
        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let partition = execution_partition(&row[3], &row[1], &row[0])?;
            for file in ["pipeline.log", "scheduler.log"] {
                let path = run_path(&row[2], &partition, file)?;
                if seen.insert(path.clone())
                    && let Some(item) = keep(window(&path, None, window_lines())?)?
                {
                    pipeline_run_logs.push(item);
                }
            }
            if pipeline_run_logs.len() >= max_files {
                break;
            }
        }
    }

    let mut params = Vec::new();
    let mut filters = shared(&mut params);
    if !block_uuids.is_empty() {
        let ors: Vec<String> = block_uuids
            .iter()
            .map(|block| {
                params.push(Raw::Text(format!("{block}%")));
                format!("c.block_uuid LIKE {{{}}}", params.len())
            })
            .collect();
        filters.push(format!("({})", ors.join(" OR ")));
    }
    if !block_run_ids.is_empty() {
        let ps: Vec<String> = block_run_ids
            .iter()
            .map(|id| {
                params.push(Raw::Int(*id));
                format!("{{{}}}", params.len())
            })
            .collect();
        filters.push(format!("c.id IN ({})", ps.join(", ")));
    }
    let variables = match metadata {
        Metadata::Postgres(_) => "CAST(a.variables AS TEXT)",
        Metadata::Sqlite(_) => "a.variables",
    };
    let from = format!(
        "FROM block_run c JOIN pipeline_run a ON a.id = c.pipeline_run_id \
         JOIN pipeline_schedule b ON a.pipeline_schedule_id = b.id WHERE {}",
        filters.join(" AND ")
    );
    let total_block_run_log_count =
        match read_rows(metadata, &format!("SELECT count(*) {from}"), &params)
            .await?
            .first()
            .and_then(|r| r.first())
        {
            Some(Raw::Int(n)) => *n,
            _ => 0,
        };
    let rows = read_rows(
        metadata,
        &format!(
            "SELECT a.execution_date, a.pipeline_schedule_id, a.pipeline_uuid, {variables}, \
             c.block_uuid {from} ORDER BY c.started_at DESC, c.id DESC{limit_sql}"
        ),
        &params,
    )
    .await?;
    let mut block_run_logs = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in rows {
        let partition = execution_partition(&row[3], &row[1], &row[0])?;
        let Raw::Text(block_uuid) = &row[4] else {
            return Err(unsupported("a block run without a block uuid"));
        };
        let path = run_path(
            &row[2],
            &partition,
            &format!("{}.log", clean_name(block_uuid)?),
        )?;
        if seen.insert(path.clone())
            && let Some(item) = keep(window(&path, None, window_lines())?)?
        {
            block_run_logs.push(item);
        }
        if block_run_logs.len() >= max_files {
            break;
        }
    }

    // The editor's logs of each block, a file per run named by its time.
    for block in &blocks {
        let base = py_join(&[
            lister.logs_dir,
            "pipelines",
            uuid,
            LOGS_SUBDIR,
            LOG_PARTITION_EDIT_PIPELINE,
            block,
            "",
        ]);
        let Ok(entries) = std::fs::read_dir(&base) else {
            continue;
        };
        let folder = base.trim_end_matches('/').to_string();
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !entry.path().is_file() || is_segment(&name) {
                continue;
            }
            if !edit_log_in_range(&name, start, end)? {
                continue;
            }
            let path = format!("{folder}/{name}");
            if let Some(item) = keep(window(&path, None, window_lines())?)? {
                block_run_logs.push(item);
            }
        }
    }

    Ok(answer(
        block_run_logs,
        pipeline_run_logs,
        total_block_run_log_count,
        total_pipeline_run_log_count,
    ))
}

/// A rotated segment (`name.log.3`), read with its log.
fn is_segment(name: &str) -> bool {
    match name.rsplit_once(".log.") {
        Some((_, n)) => !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// LogResource's filter of editor logs: the name before its first dot as
/// `%Y%m%dT%H%M%S`, within the range; a name that is not a time is left out.
fn edit_log_in_range(
    name: &str,
    start: Option<chrono::NaiveDateTime>,
    end: Option<chrono::NaiveDateTime>,
) -> Result<bool, String> {
    let stem = name.split('.').next().unwrap_or_default();
    let strict = stem.len() == 15
        && stem.as_bytes()[8] == b'T'
        && stem
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 8 || b.is_ascii_digit());
    let at = if strict {
        chrono::NaiveDateTime::parse_from_str(stem, "%Y%m%dT%H%M%S").ok()
    } else {
        None
    };
    let Some(at) = at else {
        // strptime reads some shorter numbers too: those are left to the server.
        if !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit() || b == b'T') {
            return Err(unsupported("an editor log name strptime may read"));
        }
        return Ok(false);
    };
    Ok(start.is_none_or(|s| s <= at) && end.is_none_or(|e| at <= e))
}

fn answer(
    block_run_logs: Vec<Value>,
    pipeline_run_logs: Vec<Value>,
    total_block_run_log_count: i64,
    total_pipeline_run_log_count: i64,
) -> Value {
    json!({
        "logs": [{
            "block_run_logs": block_run_logs,
            "pipeline_run_logs": pipeline_run_logs,
            "total_block_run_log_count": total_block_run_log_count,
            "total_pipeline_run_log_count": total_pipeline_run_log_count,
        }],
        "metadata": {},
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_join_as_posixpath() {
        assert_eq!(
            py_join(&[
                "/data",
                "pipelines",
                "p",
                ".logs",
                "3/20260101T000000",
                "",
                "pipeline.log"
            ]),
            "/data/pipelines/p/.logs/3/20260101T000000/pipeline.log"
        );
        assert_eq!(
            py_join(&["/data/", "pipelines", "p", ".logs", "", "", "b.log"]),
            "/data/pipelines/p/.logs/b.log"
        );
        // An absolute part starts over, as in Python.
        assert_eq!(
            py_join(&["/data", "pipelines", "/elsewhere", "x"]),
            "/elsewhere/x"
        );
    }

    #[test]
    fn block_uuids_clean_as_clean_name() {
        assert_eq!(clean_name("Load Data").unwrap(), "load_data");
        assert_eq!(clean_name("1st-block").unwrap(), "letter_1st_block");
        assert_eq!(clean_name("a\"b$c\td").unwrap(), "abcd");
        assert_eq!(clean_name("dbt/model.sql").unwrap(), "dbt_model_sql");
        assert!(clean_name("café").is_err());
    }

    #[test]
    fn editor_logs_are_named_by_their_time() {
        let at = |t| chrono::NaiveDateTime::parse_from_str(t, "%Y%m%dT%H%M%S").ok();
        assert_eq!(
            edit_log_in_range("20260101T120000.log", None, None),
            Ok(true)
        );
        assert_eq!(
            edit_log_in_range("20260101T120000.log", at("20260102T000000"), None),
            Ok(false)
        );
        assert_eq!(
            edit_log_in_range("20260101T120000.log", None, at("20260101T120000")),
            Ok(true)
        );
        assert_eq!(edit_log_in_range("notes.log", None, None), Ok(false));
        assert!(edit_log_in_range("2026111T1200.log", None, None).is_err());
        assert!(is_segment("20260101T120000.log.3"));
        assert!(!is_segment("20260101T120000.log"));
    }

    #[test]
    fn partitions_are_pythons() {
        let none = Raw::Null;
        assert_eq!(
            execution_partition(
                &none,
                &Raw::Int(3),
                &Raw::Text("2026-01-01 01:02:03.000000".into())
            ),
            Ok("3/20260101T010203".into())
        );
        assert_eq!(
            execution_partition(&none, &Raw::Int(3), &Raw::Null),
            Ok("3".into())
        );
        assert_eq!(
            execution_partition(
                &Raw::Text(r#"{"execution_partition": "custom/1"}"#.into()),
                &Raw::Int(3),
                &Raw::Null
            ),
            Ok("custom/1".into())
        );
        assert!(
            execution_partition(
                &Raw::Text(r#"{"execution_partition": 5}"#.into()),
                &Raw::Int(3),
                &Raw::Null
            )
            .is_err()
        );
    }

    #[test]
    fn earlier_lines_stay_in_the_pipelines_logs() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("pipelines/p/.logs");
        std::fs::create_dir_all(root.join("1")).unwrap();
        std::fs::write(root.join("1/b.log"), "a\n").unwrap();
        let root = root.to_string_lossy().to_string();
        let found = inside(&format!("{root}/1/b.log"), &root).unwrap();
        assert!(found.is_some_and(|p| p.ends_with("1/b.log")));
        assert_eq!(
            inside(&format!("{root}/../../q/.logs/x.log"), &root),
            Ok(None)
        );
        assert!(inside("relative.log", &root).is_err());
        assert!(inside("$HOME/x.log", &root).is_err());
    }
}
