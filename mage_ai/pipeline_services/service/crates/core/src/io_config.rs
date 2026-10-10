//! A Rust block's io_config profile, which the export compiles from io_config.yaml.
//!
//! Secrets never enter the manifest: where the profile reads `env_var('NAME')` or
//! `mage_secret_var('name')`, the value is `{"$parts": [...]}`, a list of text and
//! `{"env": "NAME", "default": ...}` items. The service fills them in from its
//! environment for each run and passes the profile to the block in `MAGE_IO_CONFIG`.

use serde_json::{Map, Value};

pub const PARTS: &str = "$parts";

/// The profile with its variables filled in by `lookup`. A value that is one unset
/// variable without a default becomes null, which blocks read as not set; an unset
/// variable inside a longer text is empty. Null settings are left out.
pub fn render(profile: &Value, lookup: &dyn Fn(&str) -> Option<String>) -> Value {
    match profile {
        Value::Object(map) => {
            if let Some(parts) = parts(map) {
                return render_parts(parts, lookup);
            }
            let mut rendered = Map::with_capacity(map.len());
            for (key, value) in map {
                let value = render(value, lookup);
                if !value.is_null() {
                    rendered.insert(key.clone(), value);
                }
            }
            Value::Object(rendered)
        }
        Value::Array(items) => Value::Array(items.iter().map(|v| render(v, lookup)).collect()),
        other => other.clone(),
    }
}

/// The environment variables the profile reads.
pub fn variables(profile: &Value) -> Vec<String> {
    let mut names = Vec::new();
    collect(profile, &mut names);
    names.sort();
    names.dedup();
    names
}

fn collect(value: &Value, names: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(parts) = parts(map) {
                names.extend(
                    parts
                        .iter()
                        .filter_map(|p| p.get("env")?.as_str().map(String::from)),
                );
            } else {
                map.values().for_each(|v| collect(v, names));
            }
        }
        Value::Array(items) => items.iter().for_each(|v| collect(v, names)),
        _ => {}
    }
}

fn parts(map: &Map<String, Value>) -> Option<&Vec<Value>> {
    if map.len() != 1 {
        return None;
    }
    map.get(PARTS)?.as_array()
}

fn variable(part: &Value, lookup: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let name = part.get("env")?.as_str()?;
    lookup(name).or_else(|| match part.get("default") {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    })
}

fn render_parts(parts: &[Value], lookup: &dyn Fn(&str) -> Option<String>) -> Value {
    if let [part @ Value::Object(_)] = parts {
        return variable(part, lookup).map_or(Value::Null, Value::String);
    }
    let mut text = String::new();
    for part in parts {
        match part {
            Value::String(literal) => text.push_str(literal),
            Value::Object(_) => text.push_str(&variable(part, lookup).unwrap_or_default()),
            _ => {}
        }
    }
    Value::String(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn lookup(name: &str) -> Option<String> {
        match name {
            "PG_HOST" => Some("db.internal".into()),
            "MAGE_SECRET_PG_PASSWORD" => Some("s3cret".into()),
            _ => None,
        }
    }

    #[test]
    fn fills_in_variables_and_keeps_literals() {
        let profile = json!({
            "POSTGRES_HOST": {"$parts": [{"env": "PG_HOST"}]},
            "POSTGRES_PASSWORD": {"$parts": [{"env": "MAGE_SECRET_PG_PASSWORD"}]},
            "POSTGRES_PORT": 5432,
            "POSTGRES_DBNAME": "shop",
            "POSTGRES_USER": {"$parts": [{"env": "PG_USER", "default": "mage"}]},
            "POSTGRES_SCHEMA": {"$parts": [{"env": "PG_SCHEMA"}]},
            "URL": {"$parts": ["postgresql://", {"env": "PG_HOST"}, ":", {"env": "PG_PORT"}, "/x"]},
            "nested": {"hosts": [{"$parts": [{"env": "PG_HOST"}]}]},
        });
        assert_eq!(
            render(&profile, &lookup),
            json!({
                "POSTGRES_HOST": "db.internal",
                "POSTGRES_PASSWORD": "s3cret",
                "POSTGRES_PORT": 5432,
                "POSTGRES_DBNAME": "shop",
                "POSTGRES_USER": "mage",
                "URL": "postgresql://db.internal:/x",
                "nested": {"hosts": ["db.internal"]},
            })
        );
    }

    #[test]
    fn a_non_text_default_is_its_json() {
        let profile = json!({"PORT": {"$parts": [{"env": "PG_PORT", "default": 5432}]}});
        assert_eq!(render(&profile, &lookup), json!({"PORT": "5432"}));
    }

    #[test]
    fn lists_the_variables() {
        let profile = json!({
            "A": {"$parts": [{"env": "PG_HOST"}]},
            "B": {"$parts": ["x", {"env": "MAGE_SECRET_PG_PASSWORD"}, {"env": "PG_HOST"}]},
            "C": "literal",
        });
        assert_eq!(
            variables(&profile),
            vec!["MAGE_SECRET_PG_PASSWORD", "PG_HOST"]
        );
    }
}
