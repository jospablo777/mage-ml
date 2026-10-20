//! The block's io_config profile (`data_provider_profile` in its settings), which MageML
//! passes in memory as MAGE_IO_CONFIG, with environment variables and secrets filled in.
use std::collections::HashMap;

use anyhow::{Context as _, Result, anyhow};

/// The profile's settings as text; nested values (a service account key) as JSON.
pub fn settings() -> Result<HashMap<String, String>> {
    let text = std::env::var("MAGE_IO_CONFIG").map_err(|_| {
        anyhow!(
            "The block has no io_config profile: set data_provider_profile in its \
             settings, or pass the settings to the client"
        )
    })?;
    parse(&text)
}

pub(crate) fn parse(text: &str) -> Result<HashMap<String, String>> {
    let value: serde_json::Value = serde_json::from_str(text).context("MAGE_IO_CONFIG")?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("MAGE_IO_CONFIG is not an object"))?;
    Ok(object
        .iter()
        .filter_map(|(key, value)| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(text) => Some((key.clone(), text.clone())),
            other => Some((key.clone(), other.to_string())),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_text_and_nested_values_json() {
        let settings =
            parse(r#"{"A": "x", "PORT": 5432, "KEY": {"type": "service_account"}, "N": null}"#)
                .unwrap();
        assert_eq!(settings["A"], "x");
        assert_eq!(settings["PORT"], "5432");
        assert_eq!(settings["KEY"], r#"{"type":"service_account"}"#);
        assert!(!settings.contains_key("N"));
        assert!(parse("[1]").is_err());
    }
}
