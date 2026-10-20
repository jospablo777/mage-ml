//! Redis for Rust blocks (feature `redis`): frames as hashes, one per row, for features a
//! service reads by key in a millisecond.
//!
//! ```ignore
//! use mage::prelude::*;
//! use mage::redis::Redis;
//! use std::time::Duration;
//!
//! fn export_data(features: DataFrame) -> Result<()> {
//!     // customer:42 -> {spend_7d: "120.5", orders_7d: "3", last_seen: "2024-06-01T..."}
//!     Redis::connect()?.write_hashes(&features, "customer", &["customer_id"],
//!                                    Some(Duration::from_secs(86_400)))?;
//!     Ok(())
//! }
//!
//! fn load_data(customers: DataFrame) -> Result<DataFrame> {
//!     Redis::connect()?.read_hashes(&customers, "customer",
//!         &[("spend_7d", DataType::Float64), ("orders_7d", DataType::Int64)])
//! }
//! ```
//!
//! The profile gives REDIS_URL (`redis://`, or `rediss://` for TLS checked against
//! Mozilla's roots), and REDIS_PASSWORD when the URL has none.
//!
//! Each row's hash is replaced as a whole, in a transaction: a field that is null in the
//! row is not left over from an earlier write. Values are stored as exact text: integers
//! as digits, floats in the shortest form that reads back to the same number, booleans
//! as `true`/`false`, dates and datetimes in ISO 8601 (with the time zone), decimals with
//! every digit, lists and structs as JSON, and binary columns as their bytes. Reading
//! converts each field to the type it is asked for; a missing key or field is null.
use std::collections::HashMap;
use std::time::Duration;

pub use redis;
use redis::{Client, Connection};
use serde_json::{Map, Value as Json};

use crate::prelude::*;

/// Rows per transaction.
pub const BATCH_ROWS: usize = 1_000;

pub struct Redis {
    connection: Connection,
}

impl Redis {
    /// Connects with the block's io_config profile.
    pub fn connect() -> Result<Self> {
        Self::connect_settings(&crate::profile::settings()?)
    }

    /// Connects with REDIS_URL and REDIS_PASSWORD.
    pub fn connect_settings(settings: &HashMap<String, String>) -> Result<Self> {
        let get = |key: &str| {
            settings
                .get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty() && v != "None")
        };
        let url = get("REDIS_URL")
            .ok_or_else(|| anyhow!("The profile has no REDIS_URL, such as redis://host:6379/0"))?;
        Self::open(&url, get("REDIS_PASSWORD").as_deref())
    }

    /// Connects to a URL; the password, if given, replaces the URL's.
    pub fn open(url: &str, password: Option<&str>) -> Result<Self> {
        // redis' rustls brings no crypto provider; ring is the SDK's.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut info: redis::ConnectionInfo = url
            .into_connection_info()
            .with_context(|| format!("REDIS_URL {}", without_password(url)))?;
        if let Some(password) = password {
            let settings = info.redis_settings().clone().set_password(password);
            info = info.set_redis_settings(settings);
        }
        let client = Client::open(info)?;
        let connection = client
            .get_connection_with_timeout(Duration::from_secs(30))
            .with_context(|| format!("Connecting to {}", without_password(url)))?;
        Ok(Redis { connection })
    }

    /// The connection, for any other command.
    pub fn connection(&mut self) -> &mut Connection {
        &mut self.connection
    }

    /// Writes each row as the hash `prefix:key`, replacing what the key held; the key joins
    /// the values of `key_columns` with ':'. Returns the rows written.
    pub fn write_hashes(
        &mut self,
        frame: &DataFrame,
        prefix: &str,
        key_columns: &[&str],
        ttl: Option<Duration>,
    ) -> Result<usize> {
        ensure!(
            !key_columns.is_empty(),
            "write_hashes needs at least one key column"
        );
        let keys = keys_of(frame, prefix, key_columns)?;
        let fields: Vec<(String, Vec<Option<Vec<u8>>>)> = frame
            .columns()
            .iter()
            .filter(|c| !key_columns.contains(&c.name().as_str()))
            .map(|column| {
                let series = column.as_materialized_series();
                let name = series.name().to_string();
                let values = series
                    .iter()
                    .map(|value| encode(value, &name))
                    .collect::<Result<Vec<_>>>()?;
                Ok((name, values))
            })
            .collect::<Result<_>>()?;
        for start in (0..keys.len()).step_by(BATCH_ROWS) {
            let end = (start + BATCH_ROWS).min(keys.len());
            let mut pipe = redis::pipe();
            pipe.atomic();
            for (row, key) in keys.iter().enumerate().take(end).skip(start) {
                pipe.cmd("DEL").arg(key).ignore();
                let mut hset = redis::cmd("HSET");
                hset.arg(key);
                let mut any = false;
                for (name, values) in &fields {
                    if let Some(value) = &values[row] {
                        hset.arg(name).arg(value.as_slice());
                        any = true;
                    }
                }
                if any {
                    pipe.add_command(hset).ignore();
                    if let Some(ttl) = ttl {
                        pipe.cmd("PEXPIRE")
                            .arg(key)
                            .arg(ttl.as_millis() as u64)
                            .ignore();
                    }
                }
            }
            pipe.query::<()>(&mut self.connection)
                .with_context(|| format!("Writing rows {start} to {end} under {prefix}:*"))?;
        }
        Ok(keys.len())
    }

    /// The fields of the hashes `prefix:key` of each row of `keys`, as columns of the given
    /// types after the key columns. A missing key or field is null.
    pub fn read_hashes(
        &mut self,
        keys: &DataFrame,
        prefix: &str,
        fields: &[(&str, DataType)],
    ) -> Result<DataFrame> {
        let key_columns: Vec<&str> = keys.get_column_names().iter().map(|n| n.as_str()).collect();
        let names = keys_of(keys, prefix, &key_columns)?;
        let mut values: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(names.len());
        for chunk in names.chunks(BATCH_ROWS) {
            let mut pipe = redis::pipe();
            for key in chunk {
                let mut hmget = redis::cmd("HMGET");
                hmget.arg(key);
                for (field, _) in fields {
                    hmget.arg(*field);
                }
                pipe.add_command(hmget);
            }
            let rows: Vec<Vec<Option<Vec<u8>>>> = pipe
                .query(&mut self.connection)
                .with_context(|| format!("Reading hashes under {prefix}:*"))?;
            values.extend(rows);
        }
        let mut frame = keys.clone();
        for (index, (field, dtype)) in fields.iter().enumerate() {
            let column: Vec<Option<&[u8]>> = values
                .iter()
                .map(|row| row.get(index).and_then(|v| v.as_deref()))
                .collect();
            frame.with_column(decode(field, &column, dtype)?.into_column())?;
        }
        Ok(frame)
    }
}

fn without_password(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme), Some(at)) if at > scheme => {
            format!("{}://***@{}", &url[..scheme], &url[at + 1..])
        }
        _ => url.to_string(),
    }
}

use redis::IntoConnectionInfo;

/// `prefix:k1:k2` for each row.
fn keys_of(frame: &DataFrame, prefix: &str, key_columns: &[&str]) -> Result<Vec<String>> {
    let mut keys: Vec<Vec<String>> = vec![Vec::new(); frame.height()];
    for name in key_columns {
        let series = frame
            .column(name)
            .with_context(|| format!("Key column {name}"))?
            .as_materialized_series()
            .clone();
        for (row, value) in series.iter().enumerate() {
            let text = encode(value, name)?
                .ok_or_else(|| anyhow!("Key column {name} is null in row {row}"))?;
            keys[row].push(String::from_utf8_lossy(&text).into_owned());
        }
    }
    Ok(keys
        .into_iter()
        .map(|parts| {
            let key = parts.join(":");
            if prefix.is_empty() {
                key
            } else {
                format!("{prefix}:{key}")
            }
        })
        .collect())
}

fn decimal_text(value: i128, scale: usize) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let text = if scale == 0 {
        digits
    } else if digits.len() > scale {
        format!(
            "{}.{}",
            &digits[..digits.len() - scale],
            &digits[digits.len() - scale..]
        )
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if negative { format!("-{text}") } else { text }
}

fn datetime_text(value: i64, unit: TimeUnit, zone: Option<&TimeZone>) -> Result<String> {
    let (seconds, nanos) = match unit {
        TimeUnit::Milliseconds => (value.div_euclid(1_000), value.rem_euclid(1_000) * 1_000_000),
        TimeUnit::Microseconds => (
            value.div_euclid(1_000_000),
            value.rem_euclid(1_000_000) * 1_000,
        ),
        TimeUnit::Nanoseconds => (
            value.div_euclid(1_000_000_000),
            value.rem_euclid(1_000_000_000),
        ),
    };
    let moment = chrono::DateTime::from_timestamp(seconds, nanos as u32)
        .ok_or_else(|| anyhow!("Datetime {value} is out of range"))?;
    Ok(match zone {
        Some(zone) => {
            let zone: chrono_tz_free::Zone = zone.as_str().parse()?;
            zone.format(moment)
        }
        None => moment
            .naive_utc()
            .format("%Y-%m-%dT%H:%M:%S%.f")
            .to_string(),
    })
}

/// Time zones: UTC and fixed offsets are written as RFC 3339; named zones as UTC with the
/// zone's name, since the SDK carries no zone database.
mod chrono_tz_free {
    use anyhow::Result;

    pub struct Zone(String);

    impl std::str::FromStr for Zone {
        type Err = anyhow::Error;
        fn from_str(text: &str) -> Result<Self> {
            Ok(Zone(text.to_string()))
        }
    }

    impl Zone {
        pub fn format(&self, moment: chrono::DateTime<chrono::Utc>) -> String {
            let utc = moment.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string();
            if self.0 == "UTC" || self.0 == "Etc/UTC" {
                utc
            } else {
                format!("{utc}[{}]", self.0)
            }
        }
    }
}

fn to_json(value: AnyValue, column: &str) -> Result<Json> {
    Ok(match value {
        AnyValue::Null => Json::Null,
        AnyValue::Boolean(v) => Json::Bool(v),
        AnyValue::Float32(v) => serde_json::Number::from_f64(v as f64)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        AnyValue::Float64(v) => serde_json::Number::from_f64(v)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        AnyValue::List(series) => Json::Array(
            series
                .iter()
                .map(|v| to_json(v, column))
                .collect::<Result<_>>()?,
        ),
        value @ (AnyValue::Struct(..) | AnyValue::StructOwned(_)) => {
            let fields = match &value {
                AnyValue::Struct(_, _, fields) => fields.to_vec(),
                AnyValue::StructOwned(payload) => payload.1.clone(),
                _ => Vec::new(),
            };
            let mut object = Map::new();
            for (field, v) in fields.iter().zip(value._iter_struct_av()) {
                object.insert(field.name().to_string(), to_json(v, column)?);
            }
            Json::Object(object)
        }
        other => match encode(other, column)? {
            None => Json::Null,
            Some(bytes) => {
                let text = String::from_utf8_lossy(&bytes).into_owned();
                // Integers stay numbers, so a reader gets every digit back.
                match text.parse::<i64>() {
                    Ok(number) => Json::from(number),
                    Err(_) => Json::String(text),
                }
            }
        },
    })
}

/// A value as the bytes stored in Redis; None for null.
fn encode(value: AnyValue, column: &str) -> Result<Option<Vec<u8>>> {
    let text = match value {
        AnyValue::Null => return Ok(None),
        AnyValue::Boolean(v) => v.to_string(),
        AnyValue::Int8(v) => v.to_string(),
        AnyValue::Int16(v) => v.to_string(),
        AnyValue::Int32(v) => v.to_string(),
        AnyValue::Int64(v) => v.to_string(),
        AnyValue::Int128(v) => v.to_string(),
        AnyValue::UInt8(v) => v.to_string(),
        AnyValue::UInt16(v) => v.to_string(),
        AnyValue::UInt32(v) => v.to_string(),
        AnyValue::UInt64(v) => v.to_string(),
        // The shortest text that reads back to the same float, with an exponent when
        // shorter: 0.1, 1e-300.
        AnyValue::Float32(v) => format!("{v:?}"),
        AnyValue::Float64(v) => format!("{v:?}"),
        AnyValue::String(v) => v.to_string(),
        AnyValue::StringOwned(v) => v.to_string(),
        AnyValue::Binary(v) => return Ok(Some(v.to_vec())),
        AnyValue::BinaryOwned(v) => return Ok(Some(v)),
        AnyValue::Date(days) => (chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
            + chrono::Duration::days(days as i64))
        .to_string(),
        AnyValue::Datetime(v, unit, zone) => datetime_text(v, unit, zone)?,
        AnyValue::DatetimeOwned(v, unit, zone) => datetime_text(v, unit, zone.as_deref())?,
        AnyValue::Time(ns) => chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (ns / 1_000_000_000) as u32,
            (ns % 1_000_000_000) as u32,
        )
        .ok_or_else(|| anyhow!("Column {column}: invalid time"))?
        .format("%H:%M:%S%.f")
        .to_string(),
        AnyValue::Decimal(v, _, scale) => decimal_text(v, scale),
        value @ (AnyValue::List(_) | AnyValue::Struct(..) | AnyValue::StructOwned(_)) => {
            to_json(value, column)?.to_string()
        }
        AnyValue::Categorical(..)
        | AnyValue::CategoricalOwned(..)
        | AnyValue::Enum(..)
        | AnyValue::EnumOwned(..) => value.str_value().to_string(),
        other => bail!(
            "Column {column}: {} cannot be written to Redis",
            other.dtype()
        ),
    };
    Ok(Some(text.into_bytes()))
}

/// A field's stored values as a column of the type asked for.
fn decode(name: &str, values: &[Option<&[u8]>], dtype: &DataType) -> Result<Series> {
    let text = |v: &[u8]| -> Result<String> {
        String::from_utf8(v.to_vec()).with_context(|| format!("Field {name} is not UTF-8"))
    };
    let parse_all =
        |parse: &dyn Fn(&str) -> Result<AnyValue<'static>>| -> Result<Vec<AnyValue<'static>>> {
            values
                .iter()
                .map(|v| match v {
                    None => Ok(AnyValue::Null),
                    Some(bytes) => parse(&text(bytes)?),
                })
                .collect()
        };
    let named = |values: Vec<AnyValue<'static>>, dtype: &DataType| -> Result<Series> {
        Ok(Series::from_any_values_and_dtype(
            name.into(),
            &values,
            dtype,
            true,
        )?)
    };
    match dtype {
        DataType::Binary => Ok(Series::new(
            name.into(),
            values
                .iter()
                .map(|v| v.map(|b| b.to_vec()))
                .collect::<Vec<_>>(),
        )),
        DataType::String => {
            let texts: Vec<Option<String>> = values
                .iter()
                .map(|v| v.map(text).transpose())
                .collect::<Result<_>>()?;
            Ok(Series::new(name.into(), texts))
        }
        DataType::Boolean => named(
            parse_all(&|t| match t {
                "true" | "1" => Ok(AnyValue::Boolean(true)),
                "false" | "0" => Ok(AnyValue::Boolean(false)),
                other => bail!("Field {name}: {other:?} is not a boolean"),
            })?,
            dtype,
        ),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => named(
            parse_all(&|t| {
                Ok(AnyValue::Int64(t.parse().with_context(|| {
                    format!("Field {name}: {t:?} is not an integer")
                })?))
            })?,
            dtype,
        ),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => named(
            parse_all(&|t| {
                Ok(AnyValue::UInt64(t.parse().with_context(|| {
                    format!("Field {name}: {t:?} is not an unsigned integer")
                })?))
            })?,
            dtype,
        ),
        DataType::Float32 | DataType::Float64 => named(
            parse_all(&|t| {
                Ok(AnyValue::Float64(t.parse().with_context(|| {
                    format!("Field {name}: {t:?} is not a number")
                })?))
            })?,
            dtype,
        ),
        other => {
            // Dates, datetimes, decimals and the rest: Polars reads them from the text.
            let texts: Vec<Option<String>> = values
                .iter()
                .map(|v| v.map(text).transpose())
                .collect::<Result<_>>()?;
            let series = Series::new(name.into(), texts.clone());
            let column = match other {
                // Stored as UTC text ('...Z', with '[zone]' for named zones): read as UTC,
                // then given the zone, which Polars does by casting.
                DataType::Datetime(unit, _) => {
                    let utc: Vec<Option<String>> = texts
                        .iter()
                        .map(|t| {
                            t.as_ref().map(|t| {
                                t.split('[')
                                    .next()
                                    .unwrap_or(t)
                                    .trim_end_matches('Z')
                                    .to_string()
                            })
                        })
                        .collect();
                    Series::new(name.into(), utc)
                        .str()?
                        .as_datetime(
                            None,
                            *unit,
                            false,
                            false,
                            None,
                            &StringChunked::from_iter([Some("raise")]),
                        )
                        .and_then(|c| c.into_series().strict_cast(other))
                }
                _ => series.strict_cast(other),
            };
            column.with_context(|| format!("Field {name} as {other}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_stored_as_exact_text() {
        let encoded =
            |value: AnyValue| String::from_utf8(encode(value, "c").unwrap().unwrap()).unwrap();
        assert_eq!(encoded(AnyValue::Int64(i64::MAX)), "9223372036854775807");
        assert_eq!(encoded(AnyValue::Float64(0.1)), "0.1");
        assert_eq!(encoded(AnyValue::Float64(1e-300)), "1e-300");
        assert_eq!(encoded(AnyValue::Float64(100.0)), "100.0");
        assert_eq!(encoded(AnyValue::Boolean(true)), "true");
        assert_eq!(encoded(AnyValue::Date(19_875)), "2024-06-01");
        assert_eq!(
            encoded(AnyValue::Datetime(
                1_717_236_000_123_456,
                TimeUnit::Microseconds,
                None
            )),
            "2024-06-01T10:00:00.123456"
        );
        assert_eq!(
            encoded(AnyValue::Datetime(
                1_717_236_000_123,
                TimeUnit::Milliseconds,
                Some(&TimeZone::UTC)
            )),
            "2024-06-01T10:00:00.123Z"
        );
        assert_eq!(encoded(AnyValue::Decimal(-1205, 38, 2)), "-12.05");
        assert_eq!(encoded(AnyValue::Decimal(5, 38, 3)), "0.005");
        assert!(encode(AnyValue::Null, "c").unwrap().is_none());
        assert_eq!(
            encode(AnyValue::Binary(&[0, 255]), "c").unwrap().unwrap(),
            vec![0, 255]
        );
    }

    #[test]
    fn stored_text_reads_back_as_the_type_asked_for() {
        let values: Vec<Option<&[u8]>> = vec![Some(b"9007199254740993"), None];
        let ints = decode("n", &values, &DataType::Int64).unwrap();
        assert_eq!(ints.i64().unwrap().get(0), Some(9_007_199_254_740_993));
        assert_eq!(ints.null_count(), 1);
        let floats = decode("x", &[Some(b"0.1".as_slice())], &DataType::Float64).unwrap();
        assert_eq!(floats.f64().unwrap().get(0), Some(0.1));
        let dates = decode("d", &[Some(b"2024-06-01".as_slice())], &DataType::Date).unwrap();
        assert_eq!(dates.dtype(), &DataType::Date);
        let at = decode(
            "at",
            &[Some(b"2024-06-01T10:00:00.123456".as_slice())],
            &DataType::Datetime(TimeUnit::Microseconds, None),
        )
        .unwrap();
        assert_eq!(
            at.get(0).unwrap(),
            AnyValue::Datetime(1_717_236_000_123_456, TimeUnit::Microseconds, None)
        );
        let error = decode("n", &[Some(b"abc".as_slice())], &DataType::Int64).unwrap_err();
        assert!(format!("{error:#}").contains("not an integer"), "{error:#}");
    }

    #[test]
    fn keys_join_the_key_columns_under_the_prefix() {
        let frame = df!("region" => ["eu", "us"], "id" => [7i64, 8]).unwrap();
        assert_eq!(
            keys_of(&frame, "customer", &["region", "id"]).unwrap(),
            ["customer:eu:7", "customer:us:8"]
        );
        let nulls = df!("id" => [Some(1i64), None]).unwrap();
        assert!(keys_of(&nulls, "c", &["id"]).is_err());
        assert_eq!(
            without_password("redis://user:secret@host:6379/0"),
            "redis://***@host:6379/0"
        );
    }
}
