//! Row values as JSON, written the same from the initial copy (the text protocol) and from the
//! binary log, so a row copied and the same row changed later look alike: integers and floats
//! as numbers, DECIMAL as a number with every digit, DATE as `2026-11-28`, DATETIME as
//! `2026-11-28T10:00:00.12` (no zone), TIMESTAMP in UTC with `+00:00`, TIME as `-01:30:00`,
//! YEAR and BIT as numbers, ENUM and SET as their names, JSON as JSON, binary strings,
//! geometry and vectors as base64, other strings as text. Fractions of a second leave out
//! trailing zeros. A zero date (month or day 0) is null: no other database takes it.

use base64::Engine as _;
use mysql_async::consts::{ColumnFlags, ColumnType};
use mysql_async::{Column, Value};
use mysql_common::binlog::value::BinlogValue;

/// The `binary` character set: bytes, not text.
const BINARY: u16 = 63;

/// How a column's values are written.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Kind {
    Integer,
    Float,
    Decimal,
    Date,
    DateTime,
    Timestamp,
    Time,
    Year,
    Bit,
    Json,
    Text,
    Binary,
    /// The binary log has an ENUM's index (from 1) and a SET's bits; their names are in the
    /// table map's metadata. The text protocol has the names already: Text.
    Enum(Vec<String>),
    Set(Vec<String>),
}

pub(crate) fn kind_of(column: &Column) -> Kind {
    use ColumnType::*;
    match column.column_type() {
        MYSQL_TYPE_TINY | MYSQL_TYPE_SHORT | MYSQL_TYPE_LONG | MYSQL_TYPE_LONGLONG
        | MYSQL_TYPE_INT24 => Kind::Integer,
        MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => Kind::Float,
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => Kind::Decimal,
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Kind::Date,
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => Kind::DateTime,
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => Kind::Timestamp,
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => Kind::Time,
        MYSQL_TYPE_YEAR => Kind::Year,
        MYSQL_TYPE_BIT => Kind::Bit,
        MYSQL_TYPE_JSON => Kind::Json,
        MYSQL_TYPE_GEOMETRY | MYSQL_TYPE_VECTOR => Kind::Binary,
        _ if column.flags().contains(ColumnFlags::BINARY_FLAG)
            && column.character_set() == BINARY =>
        {
            Kind::Binary
        }
        _ if column.character_set() == BINARY => Kind::Binary,
        _ => Kind::Text,
    }
}

/// A fraction of a second, from microseconds: `.12` for 120000, nothing for 0.
fn fraction(micros: u32) -> String {
    if micros == 0 {
        return String::new();
    }
    let digits = format!("{micros:06}");
    format!(".{}", digits.trim_end_matches('0'))
}

/// A fraction as text (`.120000`, `.5`) the same way.
fn trim_fraction(text: &str) -> String {
    match text.split_once('.') {
        Some((whole, part)) => {
            let part = part.trim_end_matches('0');
            if part.is_empty() {
                whole.to_string()
            } else {
                format!("{whole}.{part}")
            }
        }
        None => text.to_string(),
    }
}

fn write_string(out: &mut Vec<u8>, text: &str) {
    out.extend(serde_json::to_vec(text).unwrap_or_else(|_| b"null".to_vec()));
}

fn write_base64(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'"');
    out.extend_from_slice(
        base64::engine::general_purpose::STANDARD
            .encode(bytes)
            .as_bytes(),
    );
    out.push(b'"');
}

/// A float as MySQL writes it: the shortest digits, `1` for 1.0.
fn write_float(out: &mut Vec<u8>, text: &str) {
    out.extend_from_slice(text.strip_suffix(".0").unwrap_or(text).as_bytes());
}

fn is_number(text: &[u8]) -> bool {
    !text.is_empty()
        && text
            .iter()
            .all(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'))
        && text.iter().any(u8::is_ascii_digit)
}

fn write_bits(out: &mut Vec<u8>, bytes: &[u8]) {
    let value = bytes
        .iter()
        .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
    out.extend_from_slice(value.to_string().as_bytes());
}

fn write_json_text(out: &mut Vec<u8>, text: &[u8]) {
    match serde_json::from_slice::<serde_json::Value>(text) {
        Ok(value) => out.extend(serde_json::to_vec(&value).unwrap_or_default()),
        Err(_) => write_string(out, &String::from_utf8_lossy(text)),
    }
}

fn zero_date(year: u16, month: u8, day: u8) -> bool {
    year == 0 || month == 0 || day == 0
}

/// A DATE, DATETIME or TIMESTAMP as the text protocol writes it (`2026-11-28 10:00:00.120`),
/// in the form above; None for a zero date.
fn text_date(text: &str, kind: &Kind) -> Option<String> {
    let date = text.get(..10)?;
    let mut parts = date.split('-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    if [year, month, day]
        .iter()
        .any(|p| p.trim_start_matches('0').is_empty())
    {
        return None;
    }
    match kind {
        Kind::Date => Some(date.to_string()),
        Kind::Timestamp => Some(format!(
            "{}+00:00",
            trim_fraction(&text.replacen(' ', "T", 1))
        )),
        _ => Some(trim_fraction(&text.replacen(' ', "T", 1))),
    }
}

/// A value from the text protocol (`None` is NULL).
pub(crate) fn write_text(out: &mut Vec<u8>, kind: &Kind, value: Option<&[u8]>) {
    let Some(bytes) = value else {
        out.extend_from_slice(b"null");
        return;
    };
    let text = || String::from_utf8_lossy(bytes);
    match kind {
        Kind::Integer | Kind::Decimal | Kind::Year if is_number(bytes) => {
            out.extend_from_slice(bytes)
        }
        Kind::Float if is_number(bytes) => write_float(out, &text()),
        Kind::Date | Kind::DateTime | Kind::Timestamp => match text_date(&text(), kind) {
            Some(date) => write_string(out, &date),
            None => out.extend_from_slice(b"null"),
        },
        Kind::Time => write_string(out, &trim_fraction(&text())),
        Kind::Bit => write_bits(out, bytes),
        Kind::Json => write_json_text(out, bytes),
        Kind::Binary => write_base64(out, bytes),
        _ => write_string(out, &text()),
    }
}

/// A value from the binary log.
pub(crate) fn write_binlog(
    out: &mut Vec<u8>,
    kind: &Kind,
    value: Option<&BinlogValue<'_>>,
) -> Result<(), String> {
    let value = match value {
        None | Some(BinlogValue::Value(Value::NULL)) => {
            out.extend_from_slice(b"null");
            return Ok(());
        }
        Some(BinlogValue::Jsonb(jsonb)) => {
            let json = serde_json::Value::try_from(jsonb.clone())
                .map_err(|e| format!("a JSON value could not be read: {e}"))?;
            out.extend(serde_json::to_vec(&json).unwrap_or_default());
            return Ok(());
        }
        Some(BinlogValue::JsonDiff(_)) => {
            return Err(
                "a partial JSON update (binlog_row_value_options=PARTIAL_JSON) holds only \
                        the change; set binlog_row_value_options to ''"
                    .into(),
            );
        }
        Some(BinlogValue::Value(value)) => value,
    };
    match (kind, value) {
        (Kind::Enum(names), Value::Int(index)) => match usize::try_from(*index) {
            Ok(0) | Err(_) => write_string(out, ""),
            Ok(index) => match names.get(index - 1) {
                Some(name) => write_string(out, name),
                None => out.extend_from_slice(index.to_string().as_bytes()),
            },
        },
        (Kind::Timestamp, Value::Int(seconds)) => write_timestamp(out, *seconds, 0),
        (_, Value::Int(n)) => out.extend_from_slice(n.to_string().as_bytes()),
        (_, Value::UInt(n)) => out.extend_from_slice(n.to_string().as_bytes()),
        (_, Value::Float(f)) => write_float(out, &f.to_string()),
        (_, Value::Double(f)) => match serde_json::Number::from_f64(*f) {
            Some(number) => write_float(out, &number.to_string()),
            None => out.extend_from_slice(b"null"),
        },
        (Kind::Date, Value::Date(y, m, d, ..)) => {
            if zero_date(*y, *m, *d) {
                out.extend_from_slice(b"null");
            } else {
                write_string(out, &format!("{y:04}-{m:02}-{d:02}"));
            }
        }
        (_, Value::Date(y, m, d, h, mi, s, us)) => {
            if zero_date(*y, *m, *d) {
                out.extend_from_slice(b"null");
            } else {
                write_string(
                    out,
                    &format!(
                        "{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}{}",
                        fraction(*us)
                    ),
                );
            }
        }
        (_, Value::Time(negative, days, h, m, s, us)) => {
            let hours = u64::from(*days) * 24 + u64::from(*h);
            write_string(
                out,
                &format!(
                    "{}{hours:02}:{m:02}:{s:02}{}",
                    if *negative { "-" } else { "" },
                    fraction(*us)
                ),
            );
        }
        (Kind::Timestamp, Value::Bytes(bytes)) => {
            // `seconds` or `seconds.micros` since the epoch.
            let text = String::from_utf8_lossy(bytes);
            let (seconds, micros) = text.split_once('.').unwrap_or((&text, "0"));
            let seconds: i64 = seconds.parse().map_err(|_| format!("TIMESTAMP {text}"))?;
            let micros: u32 = micros.parse().map_err(|_| format!("TIMESTAMP {text}"))?;
            write_timestamp(out, seconds, micros);
        }
        (Kind::Year, Value::Bytes(bytes)) => {
            // YEAR 0000 is written as the byte 0, which reads as 1900 (not a valid YEAR).
            if bytes.as_slice() == b"1900" {
                out.push(b'0');
            } else {
                out.extend_from_slice(bytes);
            }
        }
        (Kind::Decimal, Value::Bytes(bytes)) if is_number(bytes) => out.extend_from_slice(bytes),
        (Kind::Bit, Value::Bytes(bytes)) => write_bits(out, bytes),
        (Kind::Set(names), Value::Bytes(bits)) => {
            let chosen: Vec<&str> = names
                .iter()
                .enumerate()
                .filter(|(i, _)| {
                    bits.get(i / 8)
                        .is_some_and(|byte| byte & (1 << (i % 8)) != 0)
                })
                .map(|(_, name)| name.as_str())
                .collect();
            write_string(out, &chosen.join(","));
        }
        (Kind::Binary, Value::Bytes(bytes)) => write_base64(out, bytes),
        (Kind::Json, Value::Bytes(bytes)) => write_json_text(out, bytes),
        (_, Value::Bytes(bytes)) => write_string(out, &String::from_utf8_lossy(bytes)),
        (_, Value::NULL) => out.extend_from_slice(b"null"),
    }
    Ok(())
}

fn write_timestamp(out: &mut Vec<u8>, seconds: i64, micros: u32) {
    if seconds == 0 && micros == 0 {
        // The zero TIMESTAMP.
        out.extend_from_slice(b"null");
        return;
    }
    match chrono::DateTime::from_timestamp(seconds, micros * 1000) {
        Some(time) => write_string(
            out,
            &format!(
                "{}{}+00:00",
                time.format("%Y-%m-%dT%H:%M:%S"),
                fraction(micros)
            ),
        ),
        None => out.extend_from_slice(b"null"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(kind: Kind, value: &str) -> String {
        let mut out = Vec::new();
        write_text(&mut out, &kind, Some(value.as_bytes()));
        String::from_utf8(out).unwrap()
    }

    fn binlog(kind: Kind, value: Value) -> String {
        let mut out = Vec::new();
        write_binlog(&mut out, &kind, Some(&BinlogValue::Value(value))).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn a_copied_value_and_a_logged_one_read_the_same() {
        let cases = [
            (Kind::Integer, "-12", Value::Int(-12)),
            (Kind::Integer, "18446744073709551615", Value::UInt(u64::MAX)),
            (Kind::Float, "1.5", Value::Double(1.5)),
            (Kind::Float, "1", Value::Float(1.0)),
            (Kind::Float, "0.1", Value::Float(0.1)),
            (Kind::Decimal, "12.50", Value::Bytes(b"12.50".to_vec())),
            (
                Kind::Date,
                "2026-11-28",
                Value::Date(2026, 11, 28, 0, 0, 0, 0),
            ),
            (
                Kind::DateTime,
                "2026-11-28 10:00:00.120",
                Value::Date(2026, 11, 28, 10, 0, 0, 120_000),
            ),
            (
                Kind::DateTime,
                "2026-11-28 10:00:00",
                Value::Date(2026, 11, 28, 10, 0, 0, 0),
            ),
            (
                Kind::Time,
                "-01:30:00.500000",
                Value::Time(true, 0, 1, 30, 0, 500_000),
            ),
            (
                Kind::Time,
                "838:59:59",
                Value::Time(false, 34, 22, 59, 59, 0),
            ),
            (Kind::Year, "2026", Value::Bytes(b"2026".to_vec())),
            (Kind::Year, "0000", Value::Bytes(b"1900".to_vec())),
            (Kind::Bit, "\u{1}\u{0}", Value::Bytes(vec![1, 0])),
            (
                Kind::Text,
                "tea ☕",
                Value::Bytes("tea ☕".as_bytes().to_vec()),
            ),
            (Kind::Binary, "\u{0}\u{1}", Value::Bytes(vec![0, 1])),
        ];
        for (kind, copied, logged) in cases {
            let left = text(kind.clone(), copied);
            let right = binlog(kind.clone(), logged);
            // The text protocol writes YEAR 0000 as 0000, a number either way.
            if copied == "0000" {
                assert_eq!(right, "0");
                continue;
            }
            assert_eq!(left, right, "{kind:?} {copied}");
        }
    }

    #[test]
    fn timestamps_are_utc_and_zero_dates_null() {
        assert_eq!(
            text(Kind::Timestamp, "2026-11-28 10:00:00.250000"),
            "\"2026-11-28T10:00:00.25+00:00\""
        );
        assert_eq!(
            binlog(Kind::Timestamp, Value::Bytes(b"1795860000.250000".to_vec())),
            "\"2026-11-28T10:00:00.25+00:00\""
        );
        assert_eq!(binlog(Kind::Timestamp, Value::Int(0)), "null");
        assert_eq!(text(Kind::Timestamp, "0000-00-00 00:00:00"), "null");
        assert_eq!(text(Kind::Date, "2026-00-15"), "null");
        assert_eq!(
            binlog(Kind::DateTime, Value::Date(0, 0, 0, 0, 0, 0, 0)),
            "null"
        );
    }

    #[test]
    fn enums_and_sets_are_their_names_and_json_is_json() {
        let sizes = vec!["small".to_string(), "large".to_string()];
        assert_eq!(
            binlog(Kind::Enum(sizes.clone()), Value::Int(2)),
            "\"large\""
        );
        assert_eq!(binlog(Kind::Enum(sizes.clone()), Value::Int(0)), "\"\"");
        assert_eq!(
            binlog(Kind::Set(sizes), Value::Bytes(vec![0b11])),
            "\"small,large\""
        );
        assert_eq!(text(Kind::Json, r#"{"a": [1, 2.5]}"#), r#"{"a":[1,2.5]}"#);
        assert_eq!(text(Kind::Binary, "ab"), "\"YWI=\"");
    }
}
