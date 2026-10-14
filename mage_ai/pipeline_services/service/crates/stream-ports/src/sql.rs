//! Naming and typing columns from JSON messages, shared by the SQL sinks: names as MageML's
//! Python sinks make them, and the kind of value each field holds.

pub use crate::sql_reserved::RESERVED_WORDS;

/// A column name as MageML's Python sink makes it (mage_ai.shared.utils.clean_name, then
/// reserved words prefixed with `_`).
pub fn clean_column(name: &str, case_sensitive: bool, allow_reserved: bool) -> String {
    let mut out: String = name
        .chars()
        .filter(|c| !matches!(c, '\u{feff}' | '"' | '$' | '\n' | '\r' | '\t'))
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.chars().next().is_some_and(|c| c.is_numeric()) {
        out = format!("letter_{out}");
    }
    if !case_sensitive {
        out = out.to_lowercase();
    }
    if !allow_reserved
        && RESERVED_WORDS
            .binary_search(&out.to_uppercase().as_str())
            .is_ok()
    {
        out = format!("_{out}");
    }
    out
}

/// The type a column gets from the values in a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    Boolean,
    BigInt,
    Double,
    Numeric,
    Json,
    Text,
}

impl Kind {
    pub fn of(raw: &str) -> Option<Kind> {
        let raw = raw.trim();
        Some(match raw.as_bytes().first()? {
            b'"' => Kind::Text,
            b'{' | b'[' => Kind::Json,
            b't' | b'f' => Kind::Boolean,
            b'n' => return None,
            _ if raw.contains(['.', 'e', 'E']) => Kind::Double,
            _ if raw.parse::<i64>().is_ok() => Kind::BigInt,
            _ => Kind::Numeric,
        })
    }

    /// Two values of one column: text takes anything, numbers widen, JSON takes scalars.
    pub fn merge(self, other: Kind) -> Kind {
        use Kind::*;
        match (self, other) {
            (a, b) if a == b => a,
            (Text, _) | (_, Text) => Text,
            (Json, _) | (_, Json) => Json,
            (Numeric, Double) | (Double, Numeric) | (Double, BigInt) | (BigInt, Double) => Double,
            (Numeric, BigInt) | (BigInt, Numeric) => Numeric,
            _ => Text,
        }
    }
}
