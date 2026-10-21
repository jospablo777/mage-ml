//! Times as the run history, the API and the logs write them: RFC 3339 in UTC with
//! milliseconds.

use chrono::{DateTime, SecondsFormat, Utc};

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

pub fn timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_utc_with_milliseconds() {
        let time = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.678+02:00")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(timestamp(time), "2026-01-02T01:04:05.678Z");
    }
}
