//! A GTID set (`3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7,...`): the transactions a stream
//! has handled, which any server of the replication topology understands, so a restart
//! after a failover continues where the stream was (ADR-0048).

use std::collections::BTreeMap;
use std::str::FromStr;

use mysql_common::packets::Sid;

/// One transaction: its source (uuid, or uuid:tag on MySQL 8.4) and number.
pub(crate) type Gtid = (String, u64);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GtidSet {
    /// source -> sorted, merged intervals, both ends included.
    sources: BTreeMap<String, Vec<(u64, u64)>>,
}

fn interval(part: &str) -> Option<(u64, u64)> {
    match part.split_once('-') {
        Some((a, b)) => Some((a.trim().parse().ok()?, b.trim().parse().ok()?)),
        None => {
            let n = part.trim().parse().ok()?;
            Some((n, n))
        }
    }
}

impl GtidSet {
    /// The text MySQL writes (`@@gtid_executed`, `Executed_Gtid_Set`): sources separated by
    /// commas, each with its intervals after colons; line breaks and spaces are ignored.
    pub fn parse(text: &str) -> Result<GtidSet, String> {
        let mut set = GtidSet::default();
        for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let mut parts = entry.split(':').map(str::trim);
            let mut source = parts.next().unwrap_or_default().to_ascii_lowercase();
            if source.len() != 36 {
                return Err(format!("{entry} does not start with a server uuid"));
            }
            for part in parts {
                match interval(part) {
                    Some((start, end)) if start <= end && start > 0 => {
                        set.fill(&source, start, end);
                    }
                    // A tag (MySQL 8.4) names the intervals after it.
                    _ if part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') => {
                        source = format!("{}:{}", &source[..36], part.to_ascii_lowercase());
                    }
                    _ => return Err(format!("{entry} has an interval that is not numbers")),
                }
            }
        }
        Ok(set)
    }

    fn fill(&mut self, source: &str, start: u64, end: u64) {
        let intervals = self.sources.entry(source.to_string()).or_default();
        intervals.push((start, end));
        intervals.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
        for (start, end) in intervals.drain(..) {
            match merged.last_mut() {
                Some(last) if start <= last.1.saturating_add(1) => last.1 = last.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        *intervals = merged;
    }

    /// Adds one transaction.
    pub fn add(&mut self, source: &str, gno: u64) {
        self.fill(source, gno, gno);
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// The set as MySQL writes it.
    pub fn text(&self) -> String {
        self.sources
            .iter()
            .map(|(source, intervals)| {
                let parts: Vec<String> = intervals
                    .iter()
                    .map(|(a, b)| {
                        if a == b {
                            a.to_string()
                        } else {
                            format!("{a}-{b}")
                        }
                    })
                    .collect();
                format!("{source}:{}", parts.join(":"))
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The set for a binary log request by GTIDs.
    pub fn sids(&self) -> Result<Vec<Sid<'static>>, String> {
        self.text()
            .split(',')
            .filter(|e| !e.is_empty())
            .map(|entry| {
                Sid::from_str(entry).map_err(|e| format!("GTID set {entry} could not be read: {e}"))
            })
            .collect()
    }
}

/// A GTID event's source uuid as MySQL writes it.
pub(crate) fn uuid(bytes: [u8; 16]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "4d22fa47-71ca-11e1-9e33-c80aa9429563";

    #[test]
    fn a_set_reads_merges_and_writes_as_mysql_does() {
        let mut set = GtidSet::parse(&format!("{A}:1-5:7,\n{B}:1-3")).unwrap();
        assert_eq!(set.text(), format!("{A}:1-5:7,{B}:1-3"));
        set.add(A, 6);
        set.add(B, 5);
        assert_eq!(set.text(), format!("{A}:1-7,{B}:1-3:5"));
        set.add(B, 4);
        assert_eq!(set.text(), format!("{A}:1-7,{B}:1-5"));
        assert!(GtidSet::parse("").unwrap().is_empty());
        assert!(GtidSet::parse("nope:1-2").is_err());
        assert!(GtidSet::parse(&format!("{A}:a-b")).is_err());
    }

    #[test]
    fn tags_keep_their_intervals_apart() {
        let set = GtidSet::parse(&format!("{A}:1-3:nightly:1-2")).unwrap();
        assert_eq!(set.text(), format!("{A}:1-3,{A}:nightly:1-2"));
    }

    #[test]
    fn a_set_becomes_the_requests_sids_and_uuids_read_as_text() {
        let set = GtidSet::parse(&format!("{A}:1-5:7")).unwrap();
        let sids = set.sids().unwrap();
        assert_eq!(sids.len(), 1);
        assert_eq!(sids[0].to_string(), format!("{A}:1-5:7"));
        let bytes = [
            0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42,
            0x95, 0x62,
        ];
        assert_eq!(uuid(bytes), A);
    }
}
