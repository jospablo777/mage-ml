//! What a connection check found, for `mage-service check-connections` and the editor's
//! connection report. A check only reads: it never joins a consumer group, takes a message,
//! or writes, so it is safe against production systems.

/// The outcome of a check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckResult {
    /// The system answered and allows what the stream does, as far as a read can tell.
    Passed,
    /// It answered, with something to look at (a missing topic, a lag, no committed position).
    Warning,
    /// It did not answer, or refused.
    Failed,
    /// This connector has no check.
    NotChecked,
}

impl CheckResult {
    pub fn as_str(self) -> &'static str {
        match self {
            CheckResult::Passed => "passed",
            CheckResult::Warning => "warning",
            CheckResult::Failed => "failed",
            CheckResult::NotChecked => "not_checked",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionReport {
    pub result: CheckResult,
    /// One line in plain words: what was reached, or why not.
    pub summary: String,
    /// What the system said, as label and value: partitions, retention, lag, columns.
    pub facts: Vec<(String, String)>,
    /// Each thing to fix or look at.
    pub problems: Vec<String>,
}

impl ConnectionReport {
    pub fn passed(summary: impl Into<String>) -> ConnectionReport {
        ConnectionReport {
            result: CheckResult::Passed,
            summary: summary.into(),
            facts: Vec::new(),
            problems: Vec::new(),
        }
    }

    pub fn failed(summary: impl Into<String>) -> ConnectionReport {
        ConnectionReport {
            result: CheckResult::Failed,
            ..ConnectionReport::passed(summary)
        }
    }

    pub fn not_checked(summary: impl Into<String>) -> ConnectionReport {
        ConnectionReport {
            result: CheckResult::NotChecked,
            ..ConnectionReport::passed(summary)
        }
    }

    pub fn fact(mut self, label: &str, value: impl ToString) -> ConnectionReport {
        self.facts.push((label.to_string(), value.to_string()));
        self
    }

    /// A problem that leaves the connection usable: the result becomes a warning.
    pub fn warn(mut self, problem: impl Into<String>) -> ConnectionReport {
        if self.result == CheckResult::Passed {
            self.result = CheckResult::Warning;
        }
        self.problems.push(problem.into());
        self
    }

    /// A problem the stream cannot run with: the result becomes a failure.
    pub fn fail(mut self, problem: impl Into<String>) -> ConnectionReport {
        self.result = CheckResult::Failed;
        self.problems.push(problem.into());
        self
    }
}

/// A duration in the largest whole unit: `7 days`, `36 hours`, `90 seconds`.
pub fn human_duration(milliseconds: i64) -> String {
    if milliseconds < 0 {
        return "forever".into();
    }
    let seconds = milliseconds / 1000;
    for (unit, size) in [("day", 86_400), ("hour", 3_600), ("minute", 60)] {
        if seconds >= size && seconds % size == 0 {
            let n = seconds / size;
            return format!("{n} {unit}{}", if n == 1 { "" } else { "s" });
        }
    }
    format!("{seconds} seconds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_report_turns_to_a_warning_or_a_failure_by_its_problems() {
        let report = ConnectionReport::passed("reached").fact("partitions", 3);
        assert_eq!(report.result, CheckResult::Passed);
        let report = report.warn("no committed position");
        assert_eq!(report.result, CheckResult::Warning);
        let report = report.fail("topic missing").warn("later");
        assert_eq!(report.result, CheckResult::Failed);
        assert_eq!(report.problems.len(), 3);
        assert_eq!(
            report.facts,
            vec![("partitions".to_string(), "3".to_string())]
        );
    }

    #[test]
    fn durations_read_in_their_unit() {
        assert_eq!(human_duration(604_800_000), "7 days");
        assert_eq!(human_duration(129_600_000), "36 hours");
        assert_eq!(human_duration(90_000), "90 seconds");
        assert_eq!(human_duration(-1), "forever");
    }
}
