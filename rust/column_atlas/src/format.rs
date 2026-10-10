//! Cell text for the grid. Integers and decimals keep every digit, floats use the shortest
//! text that reads back as the same number, text is shown as is, and nested values as JSON.

use polars::prelude::*;

pub const MAX_CELL_CHARS: usize = 256;
const ELLIPSIS: char = '…';

pub fn truncate(text: String) -> String {
    let mut chars = text.char_indices();
    match chars.nth(MAX_CELL_CHARS - 1) {
        // The text has more than MAX_CELL_CHARS characters only if one follows this one.
        Some((index, _)) if chars.next().is_some() => {
            let mut short = text[..index].to_string();
            short.push(ELLIPSIS);
            short
        }
        _ => text,
    }
}

pub fn float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if value == value.trunc() && value.abs() < 1e16 {
        // 3.0 rather than 3, so whole floats are not taken for integers.
        format!("{value:.1}")
    } else {
        // Debug writes the shortest text that reads back as the same float, with an
        // exponent for very large or small values.
        format!("{value:?}")
    }
}

fn hex(bytes: &[u8]) -> String {
    let shown = bytes.len().min(MAX_CELL_CHARS / 2);
    let mut text = String::with_capacity(2 + shown * 2);
    text.push_str("0x");
    for byte in &bytes[..shown] {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// A value as JSON, for nested values: lists and structs keep their shape.
fn json(value: &AnyValue<'_>) -> serde_json::Value {
    use serde_json::Value;
    match value {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(v) => Value::Bool(*v),
        AnyValue::Int8(v) => Value::from(*v),
        AnyValue::Int16(v) => Value::from(*v),
        AnyValue::Int32(v) => Value::from(*v),
        AnyValue::Int64(v) => Value::from(*v),
        AnyValue::UInt8(v) => Value::from(*v),
        AnyValue::UInt16(v) => Value::from(*v),
        AnyValue::UInt32(v) => Value::from(*v),
        AnyValue::UInt64(v) => Value::from(*v),
        AnyValue::Float32(v) => serde_json::Number::from_f64(*v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::Float64(v) => serde_json::Number::from_f64(*v)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        AnyValue::String(v) => Value::String(v.to_string()),
        AnyValue::StringOwned(v) => Value::String(v.to_string()),
        AnyValue::List(series) => Value::Array(series.iter().map(|item| json(&item)).collect()),
        AnyValue::Array(series, _) => Value::Array(series.iter().map(|item| json(&item)).collect()),
        AnyValue::Struct(_, _, fields) => {
            let values: Vec<AnyValue> = value._iter_struct_av().collect();
            let mut map = serde_json::Map::new();
            for (field, item) in fields.iter().zip(values.iter()) {
                map.insert(field.name().to_string(), json(item));
            }
            Value::Object(map)
        }
        AnyValue::StructOwned(payload) => {
            let (values, fields) = payload.as_ref();
            let mut map = serde_json::Map::new();
            for (field, item) in fields.iter().zip(values.iter()) {
                map.insert(field.name().to_string(), json(item));
            }
            Value::Object(map)
        }
        other => match cell(other) {
            Some(text) => Value::String(text),
            None => Value::Null,
        },
    }
}

/// The text of a value; None is a missing value.
/// A geometry cell: WKT of its WKB bytes, or hex when they are not WKB.
pub fn geometry(value: &AnyValue<'_>) -> Option<String> {
    let bytes = match value {
        AnyValue::Binary(bytes) => *bytes,
        AnyValue::BinaryOwned(bytes) => bytes.as_slice(),
        _ => return cell(value),
    };
    match crate::wkb::to_wkt(bytes, MAX_CELL_CHARS * 4) {
        Some(text) => Some(truncate(text)),
        None => cell(value),
    }
}

pub fn cell(value: &AnyValue<'_>) -> Option<String> {
    let text = match value {
        AnyValue::Null => return None,
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
        AnyValue::Float32(v) => float(*v as f64),
        AnyValue::Float64(v) => float(*v),
        AnyValue::String(v) => v.to_string(),
        AnyValue::StringOwned(v) => v.to_string(),
        AnyValue::Categorical(_, _)
        | AnyValue::Enum(_, _)
        | AnyValue::CategoricalOwned(_, _)
        | AnyValue::EnumOwned(_, _) => value.str_value().to_string(),
        AnyValue::Binary(v) => hex(v),
        AnyValue::BinaryOwned(v) => hex(v),
        AnyValue::List(_)
        | AnyValue::Array(_, _)
        | AnyValue::Struct(_, _, _)
        | AnyValue::StructOwned(_) => json(value).to_string(),
        // Dates, times, durations and decimals: Polars writes them as ISO text and exact
        // decimals.
        other => other.to_string(),
    };
    Some(truncate(text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_text_is_cut_to_the_limit() {
        let text = "ñ".repeat(400);
        let short = cell(&AnyValue::String(&text)).unwrap();
        assert_eq!(short.chars().count(), MAX_CELL_CHARS);
        assert!(short.ends_with(ELLIPSIS));
        let exact = "z".repeat(MAX_CELL_CHARS);
        assert_eq!(cell(&AnyValue::String(&exact)).unwrap(), exact);
    }

    #[test]
    fn numbers_keep_their_value() {
        assert_eq!(
            cell(&AnyValue::UInt64(u64::MAX)).unwrap(),
            "18446744073709551615"
        );
        assert_eq!(
            cell(&AnyValue::Int64(i64::MIN)).unwrap(),
            "-9223372036854775808"
        );
        assert_eq!(float(0.1), "0.1");
        assert_eq!(float(3.0), "3.0");
        assert_eq!(float(1e300), "1e300");
        assert_eq!(float(f64::NAN), "NaN");
        assert_eq!(float(f64::NEG_INFINITY), "-inf");
        assert_eq!(float(-0.000123), "-0.000123");
    }

    #[test]
    fn text_has_no_quotes() {
        assert_eq!(cell(&AnyValue::String("it's \"x\"")).unwrap(), "it's \"x\"");
        assert_eq!(cell(&AnyValue::Null), None);
    }

    #[test]
    fn binary_is_hex() {
        assert_eq!(cell(&AnyValue::Binary(&[0, 255, 16])).unwrap(), "0x00ff10");
    }
}
