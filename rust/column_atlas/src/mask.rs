//! Masking a table for development copies (`mage data sample`): each column keeps its
//! values, becomes null, is masked, replaced by a keyed hash, a deterministic fake, or its
//! dates are shifted. Hashes and fakes come from HMAC-SHA-256 with the copy's key, so the
//! same value gives the same output in every table and every run with that key, and joins
//! on hashed keys still match; without the key a value cannot be checked against them.
//! Missing values stay missing.

use std::collections::BTreeMap;

use polars::prelude::*;
use serde::{Deserialize, Serialize};

use crate::error::AtlasError;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "rule")]
pub enum Rule {
    Keep,
    Null,
    /// `***` for text; missing for other types.
    Mask,
    /// A keyed hash: an integer for integer columns (keys stay integers), hex text otherwise.
    Hash,
    /// A made-up value of a kind (`name`, `email`, `phone`, `text`), the same for the same
    /// input value.
    Fake {
        kind: String,
    },
    /// Dates and times moved by up to `days` days either way; the same row moves the same
    /// way in every column shifted `by` the same column (its key), or by its own value.
    ShiftDays {
        days: i64,
        #[serde(default)]
        by: Option<String>,
    },
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub rows: usize,
    pub columns: BTreeMap<String, String>,
}

const FIRST: &[&str] = &[
    "Alex", "Ana", "Ari", "Bea", "Cam", "Dani", "Eli", "Femi", "Gabi", "Hana", "Ira", "Jo", "Kai",
    "Lee", "Mika", "Noa", "Oli", "Pat", "Quin", "Rae", "Sam", "Tali", "Uri", "Val", "Wren", "Yael",
    "Zoe", "Ash", "Bo", "Cruz",
];
const LAST: &[&str] = &[
    "Abara",
    "Berg",
    "Costa",
    "Dahl",
    "Ekberg",
    "Fujita",
    "Garza",
    "Haddad",
    "Ito",
    "Jensen",
    "Kowal",
    "Lindqvist",
    "Mora",
    "Nakamura",
    "Okafor",
    "Petrov",
    "Quispe",
    "Rossi",
    "Silva",
    "Tanaka",
    "Ueda",
    "Varga",
    "Weber",
    "Xu",
    "Yilmaz",
    "Zamora",
];

struct Keyed(ring::hmac::Key);

impl Keyed {
    fn new(key: &str) -> Keyed {
        Keyed(ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            key.as_bytes(),
        ))
    }

    fn digest(&self, text: &str) -> [u8; 32] {
        let tag = ring::hmac::sign(&self.0, text.as_bytes());
        let mut out = [0u8; 32];
        out.copy_from_slice(tag.as_ref());
        out
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn word(bytes: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(bytes[at..at + 8].try_into().expect("8 bytes"))
}

fn fake(kind: &str, digest: &[u8; 32]) -> Result<String, AtlasError> {
    let n = word(digest, 0);
    Ok(match kind {
        "name" => format!(
            "{} {}",
            FIRST[(n % FIRST.len() as u64) as usize],
            LAST[(word(digest, 8) % LAST.len() as u64) as usize]
        ),
        "email" => format!("user-{}@example.com", hex(&digest[..6])),
        "phone" => format!("+1 555 01{:02} {:04}", n % 100, word(digest, 8) % 10_000),
        "text" => format!("text-{}", hex(&digest[..6])),
        other => {
            return Err(AtlasError::invalid(format!(
                "fake {other}: the kinds are name, email, phone and text"
            )));
        }
    })
}

/// The text a value is hashed from: the same for an integer key and its text form, so a
/// key stored as an integer in one table and as text in another hashes the same.
fn texts(series: &Series) -> Result<Vec<Option<String>>, AtlasError> {
    let as_text = series
        .cast(&DataType::String)
        .map_err(|e| AtlasError::engine(format!("column {}: {e}", series.name())))?;
    Ok(as_text
        .str()
        .map_err(|e| AtlasError::engine(e.to_string()))?
        .iter()
        .map(|v| v.map(str::to_string))
        .collect())
}

fn apply(
    frame: &DataFrame,
    column: &str,
    rule: &Rule,
    keyed: &Keyed,
) -> Result<Series, AtlasError> {
    let series = frame
        .column(column)
        .map_err(|_| AtlasError::invalid(format!("the table has no column {column}")))?
        .as_materialized_series()
        .clone();
    let name = series.name().clone();
    let dtype = series.dtype().clone();
    Ok(match rule {
        Rule::Keep => series,
        Rule::Null => Series::full_null(name, series.len(), &dtype),
        Rule::Mask => {
            if dtype == DataType::String {
                let values: Vec<Option<&str>> = series
                    .str()
                    .map_err(|e| AtlasError::engine(e.to_string()))?
                    .iter()
                    .map(|v| v.map(|_| "***"))
                    .collect();
                Series::new(name, values)
            } else {
                Series::full_null(name, series.len(), &dtype)
            }
        }
        Rule::Hash => {
            let values = texts(&series)?;
            if dtype.is_integer() {
                let hashed: Vec<Option<i64>> = values
                    .iter()
                    .map(|v| {
                        v.as_deref()
                            .map(|t| (word(&keyed.digest(t), 0) & i64::MAX as u64) as i64)
                    })
                    .collect();
                Series::new(name, hashed)
            } else {
                let hashed: Vec<Option<String>> = values
                    .iter()
                    .map(|v| v.as_deref().map(|t| hex(&keyed.digest(t)[..16])))
                    .collect();
                Series::new(name, hashed)
            }
        }
        Rule::Fake { kind } => {
            let values = texts(&series)?;
            let faked: Vec<Option<String>> = values
                .iter()
                .map(|v| {
                    v.as_deref()
                        .map(|t| fake(kind, &keyed.digest(t)))
                        .transpose()
                })
                .collect::<Result<_, _>>()?;
            Series::new(name, faked)
        }
        Rule::ShiftDays { days, by } => {
            if !matches!(dtype, DataType::Date | DataType::Datetime(_, _)) {
                return Err(AtlasError::invalid(format!(
                    "column {column} is {dtype}, not a date or time: shift_days moves dates"
                )));
            }
            let source = match by {
                Some(other) => frame
                    .column(other)
                    .map_err(|_| {
                        AtlasError::invalid(format!("shift_days by {other}: no such column"))
                    })?
                    .as_materialized_series()
                    .clone(),
                None => series.clone(),
            };
            let span = 2 * days.max(&0) + 1;
            let offsets: Vec<Option<i64>> = texts(&source)?
                .iter()
                .map(|v| {
                    // A row without its key keeps its date.
                    Some(v.as_deref().map_or(0, |t| {
                        (word(&keyed.digest(t), 0) % span as u64) as i64 - days
                    }))
                })
                .collect();
            let offsets = Series::new("offsets".into(), offsets);
            let shifted = match &dtype {
                DataType::Date => {
                    let days_since = series
                        .cast(&DataType::Int32)
                        .map_err(|e| AtlasError::engine(e.to_string()))?
                        .cast(&DataType::Int64)
                        .map_err(|e| AtlasError::engine(e.to_string()))?;
                    (&days_since + &offsets)
                        .map_err(|e| AtlasError::engine(e.to_string()))?
                        .cast(&DataType::Int32)
                        .and_then(|s| s.cast(&DataType::Date))
                }
                DataType::Datetime(unit, _) => {
                    let per_day: i64 = match unit {
                        TimeUnit::Nanoseconds => 86_400_000_000_000,
                        TimeUnit::Microseconds => 86_400_000_000,
                        TimeUnit::Milliseconds => 86_400_000,
                    };
                    let since = series
                        .cast(&DataType::Int64)
                        .map_err(|e| AtlasError::engine(e.to_string()))?;
                    let moved = &offsets * per_day;
                    (&since + &moved)
                        .map_err(|e| AtlasError::engine(e.to_string()))?
                        .cast(&dtype)
                }
                _ => unreachable!(),
            }
            .map_err(|e| AtlasError::engine(e.to_string()))?;
            shifted.with_name(name)
        }
    })
}

/// Reads `input` (Parquet or Arrow IPC), applies `rules` (JSON: column -> rule), writes
/// Parquet to `output`. A column without a rule is kept; the caller decides which columns
/// must have one.
pub fn mask(input: &str, output: &str, rules: &str, key: &str) -> Result<Report, AtlasError> {
    let rules: BTreeMap<String, Rule> = serde_json::from_str(rules)
        .map_err(|e| AtlasError::invalid(format!("the masking rules: {e}")))?;
    if key.len() < 16 {
        return Err(AtlasError::invalid(
            "the masking key is shorter than 16 characters: hashes from it could be guessed",
        ));
    }
    let lazy =
        if input.ends_with(".arrow") || input.ends_with(".ipc") || input.ends_with(".feather") {
            LazyFrame::scan_ipc(
                PlRefPath::from(input),
                Default::default(),
                Default::default(),
            )
        } else {
            LazyFrame::scan_parquet(PlRefPath::from(input), ScanArgsParquet::default())
        }
        .map_err(|e| AtlasError::engine(format!("reading {input}: {e}")))?;
    let frame = lazy
        .collect()
        .map_err(|e| AtlasError::engine(format!("reading {input}: {e}")))?;
    let keyed = Keyed::new(key);
    let mut columns = Vec::with_capacity(frame.width());
    let mut report = Report {
        rows: frame.height(),
        columns: BTreeMap::new(),
    };
    for name in frame.get_column_names() {
        let rule = rules.get(name.as_str()).cloned().unwrap_or(Rule::Keep);
        report.columns.insert(
            name.to_string(),
            serde_json::to_value(&rule)
                .ok()
                .and_then(|v| v["rule"].as_str().map(str::to_string))
                .unwrap_or_default(),
        );
        columns.push(apply(&frame, name.as_str(), &rule, &keyed)?.into_column());
    }
    for name in rules.keys() {
        if frame.column(name).is_err() {
            return Err(AtlasError::invalid(format!(
                "a rule names column {name}, which the table does not have"
            )));
        }
    }
    let mut masked =
        DataFrame::new(frame.height(), columns).map_err(|e| AtlasError::engine(e.to_string()))?;
    let file = std::fs::File::create(output)
        .map_err(|e| AtlasError::engine(format!("writing {output}: {e}")))?;
    ParquetWriter::new(file)
        .finish(&mut masked)
        .map_err(|e| AtlasError::engine(format!("writing {output}: {e}")))?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "a-development-key-0123";

    fn masked(frame: &mut DataFrame, rules: &str) -> DataFrame {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.parquet");
        let output = dir.path().join("out.parquet");
        ParquetWriter::new(std::fs::File::create(&input).unwrap())
            .finish(frame)
            .unwrap();
        mask(
            input.to_str().unwrap(),
            output.to_str().unwrap(),
            rules,
            KEY,
        )
        .unwrap();
        LazyFrame::scan_parquet(
            PlRefPath::from(output.to_str().unwrap()),
            ScanArgsParquet::default(),
        )
        .unwrap()
        .collect()
        .unwrap()
    }

    #[test]
    fn hashed_keys_still_join_and_never_equal_the_originals() {
        let mut customers = df!(
            "id" => [17i64, 42, 99],
            "email" => [Some("ana@corp.org"), None, Some("bo@corp.org")],
            "name" => ["Ana Real", "Bo Real", "Cy Real"],
            "ssn" => ["111", "222", "333"],
        )
        .unwrap();
        let mut orders =
            df!("customer_id" => ["42", "17", "42"], "total" => [1.0, 2.0, 3.0]).unwrap();
        let rules = r#"{"id": {"rule": "hash"}, "email": {"rule": "fake", "kind": "email"},
                        "name": {"rule": "fake", "kind": "name"}, "ssn": {"rule": "mask"}}"#;
        let customers = masked(&mut customers, rules);
        let orders = masked(&mut orders, r#"{"customer_id": {"rule": "hash"}}"#);

        let ids: Vec<i64> = customers
            .column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(ids.iter().all(|id| ![17, 42, 99].contains(id)), "{ids:?}");
        // The integer key and its text form in another table hash alike, as an integer
        // for the integer column and hex for the text one: both come from the same digest.
        let keyed = Keyed::new(KEY);
        assert_eq!(
            ids[1],
            (word(&keyed.digest("42"), 0) & i64::MAX as u64) as i64
        );
        let order_keys: Vec<String> = orders
            .column("customer_id")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .flatten()
            .map(str::to_string)
            .collect();
        assert_eq!(order_keys[0], order_keys[2]);
        assert_eq!(order_keys[0], hex(&keyed.digest("42")[..16]));

        let emails: Vec<Option<&str>> = customers
            .column("email")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert!(emails[0].unwrap().ends_with("@example.com"));
        assert_eq!(emails[1], None);
        let names: Vec<&str> = customers
            .column("name")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert!(names.iter().all(|n| !n.contains("Real")));
        let ssn: Vec<&str> = customers
            .column("ssn")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .flatten()
            .collect();
        assert_eq!(ssn, ["***", "***", "***"]);
    }

    #[test]
    fn dates_shift_by_the_rows_key_within_the_range() {
        let mut frame = df!(
            "id" => [1i64, 2, 3],
            "signed_up" => [chrono::NaiveDate::from_ymd_opt(2026, 1, 10).unwrap(); 3],
        )
        .unwrap();
        let shifted = masked(
            &mut frame,
            r#"{"signed_up": {"rule": "shift_days", "days": 30, "by": "id"}}"#,
        );
        let days: Vec<i32> = shifted
            .column("signed_up")
            .unwrap()
            .cast(&DataType::Int32)
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let base = 20463; // 2026-01-10
        assert!(days.iter().all(|d| (d - base).abs() <= 30), "{days:?}");
        let again = masked(
            &mut frame,
            r#"{"signed_up": {"rule": "shift_days", "days": 30, "by": "id"}}"#,
        );
        assert_eq!(shifted, again, "the same key moves the same way");
    }

    #[test]
    fn a_short_key_and_an_unknown_column_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.parquet");
        let mut frame = df!("a" => [1i64]).unwrap();
        ParquetWriter::new(std::fs::File::create(&input).unwrap())
            .finish(&mut frame)
            .unwrap();
        let out = dir.path().join("out.parquet");
        let (input, out) = (input.to_str().unwrap(), out.to_str().unwrap());
        assert!(mask(input, out, "{}", "short").is_err());
        let error = mask(input, out, r#"{"b": {"rule": "null"}}"#, KEY)
            .err()
            .unwrap();
        assert!(format!("{error:?}").contains("column b"));
    }
}
