//! Secret values replaced in text the service writes (logs, errors, the run history).

pub fn redact(text: &str, values: &[String]) -> String {
    let mut out = text.to_string();
    for value in values {
        if out.contains(value.as_str()) {
            out = out.replace(value.as_str(), "***");
        }
    }
    out
}
