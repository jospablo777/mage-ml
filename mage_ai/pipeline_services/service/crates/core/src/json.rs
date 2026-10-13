//! JSON checks the service makes before it parses user values into `serde_json::Value`.
//!
//! `Value` holds integers in 64 bits; serde_json turns a larger integer into a float and loses
//! its last digits, with no error. Where user values enter the service (run variables,
//! manifest variables), such integers are refused with a clear message instead, so nothing is
//! changed quietly. Stream messages never become `Value`: they pass as raw JSON.

/// Integer literals in the JSON text that do not fit in 64 bits (below `i64::MIN` or above
/// `u64::MAX`), as written. Numbers with a fraction or an exponent are floats and round-trip
/// exactly; numbers inside strings are text.
pub fn oversized_integers(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += if bytes[i] == b'\\' { 2 } else { 1 };
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                while i < bytes.len()
                    && matches!(bytes[i], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    i += 1;
                }
                let token = &text[start..i];
                let integer = !token.contains(['.', 'e', 'E']);
                if integer && !fits_64_bits(token) {
                    found.push(token.to_string());
                }
            }
            _ => i += 1,
        }
    }
    found
}

fn fits_64_bits(token: &str) -> bool {
    token.parse::<i64>().is_ok() || token.parse::<u64>().is_ok()
}

/// An error message naming the oversized integers of `what`, or None.
pub fn check_integers(text: &str, what: &str) -> Option<String> {
    let found = oversized_integers(text);
    if found.is_empty() {
        return None;
    }
    let shown: Vec<&str> = found.iter().take(3).map(String::as_str).collect();
    Some(format!(
        "{what} holds integers too large for 64 bits ({}); they would lose digits. Send them \
         as strings.",
        shown.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_past_64_bits_are_found_and_nothing_else() {
        let text = r#"{"big": 123456789012345678901234567890, "max": 18446744073709551615,
            "min": -9223372036854775808, "under": -9223372036854775809, "float": 1e300,
            "fraction": 12345678901234567890123.5, "text": "99999999999999999999999",
            "escaped": "a \" 99999999999999999999999", "list": [1, 2, 18446744073709551616]}"#;
        assert_eq!(
            oversized_integers(text),
            [
                "123456789012345678901234567890",
                "-9223372036854775809",
                "18446744073709551616"
            ]
        );
    }

    #[test]
    fn the_message_names_them() {
        assert_eq!(check_integers(r#"{"a": 1}"#, "x"), None);
        let message = check_integers("[123456789012345678901234567890]", "The variables").unwrap();
        assert!(
            message.contains("123456789012345678901234567890"),
            "{message}"
        );
        assert!(message.contains("as strings"));
    }
}
