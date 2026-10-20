//! Run logs read as one text over their segments: windows of lines and search.
//!
//! A block run's log is rotated into segments of 20 MB (`block.log.3`, `block.log.2`,
//! `block.log.1`, `block.log`, oldest first). The logs API read the newest segment whole and
//! the page parsed it in the browser: everything before the last rotation was hidden, and a
//! large log made the page download and parse megabytes every five seconds. Here every
//! segment counts, and a request reads only the lines it shows.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

use crate::error::AtlasError;

fn io(path: &str, error: std::io::Error) -> AtlasError {
    AtlasError::Engine(format!("{path}: {error}"))
}

/// Lines in a file: newline characters, plus one for a last line without one.
fn count_lines(path: &str) -> Result<u64, AtlasError> {
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut lines = 0u64;
    let mut last = b'\n';
    loop {
        let read = file.read(&mut buffer).map_err(|e| io(path, e))?;
        if read == 0 {
            break;
        }
        lines += memchr_count(&buffer[..read]);
        last = buffer[read - 1];
    }
    if last != b'\n' {
        lines += 1;
    }
    Ok(lines)
}

fn memchr_count(bytes: &[u8]) -> u64 {
    memchr::memchr_iter(b'\n', bytes).count() as u64
}

/// Byte offset where line `line` (0-based) of a file starts.
fn line_offset(path: &str, line: u64) -> Result<u64, AtlasError> {
    if line == 0 {
        return Ok(0);
    }
    let mut file = File::open(path).map_err(|e| io(path, e))?;
    let mut buffer = vec![0u8; 1 << 20];
    let mut seen = 0u64;
    let mut offset = 0u64;
    loop {
        let read = file.read(&mut buffer).map_err(|e| io(path, e))?;
        if read == 0 {
            return Ok(offset);
        }
        for i in memchr::memchr_iter(b'\n', &buffer[..read]) {
            seen += 1;
            if seen == line {
                return Ok(offset + i as u64 + 1);
            }
        }
        offset += read as u64;
    }
}

/// A window of a log's lines.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct Window {
    /// The lines, each ending with a newline.
    pub text: String,
    /// Index of the first line returned, 0-based over all segments.
    pub first_line: u64,
    /// Lines in all segments.
    pub total_lines: u64,
}

/// The `count` lines that end before line `end` (all lines when `end` is None), over the
/// segments in order (oldest first). Missing segments count as empty: a rotation can rename
/// them while the window is read.
pub fn window(paths: &[String], end: Option<u64>, count: u64) -> Result<Window, AtlasError> {
    let mut counts = Vec::with_capacity(paths.len());
    for path in paths {
        counts.push(if std::path::Path::new(path).exists() {
            count_lines(path)?
        } else {
            0
        });
    }
    let total: u64 = counts.iter().sum();
    let end = end.unwrap_or(total).min(total);
    let start = end.saturating_sub(count);
    let mut text = String::new();
    let mut base = 0u64;
    for (path, lines) in paths.iter().zip(&counts) {
        let (from, to) = (base, base + lines);
        base = to;
        if to <= start || from >= end || *lines == 0 {
            continue;
        }
        let first = start.saturating_sub(from);
        let last = end.min(to) - from;
        let mut file = File::open(path).map_err(|e| io(path, e))?;
        file.seek(SeekFrom::Start(line_offset(path, first)?))
            .map_err(|e| io(path, e))?;
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut line = Vec::new();
        for _ in first..last {
            line.clear();
            if reader
                .read_until(b'\n', &mut line)
                .map_err(|e| io(path, e))?
                == 0
            {
                break;
            }
            text.push_str(&String::from_utf8_lossy(&line));
            if !text.ends_with('\n') {
                text.push('\n');
            }
        }
    }
    Ok(Window {
        text,
        first_line: start,
        total_lines: total,
    })
}

/// A line that matched a search.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct Match {
    pub line: u64,
    pub text: String,
}

/// Lines that contain `needle` (case-insensitive), over the segments in order, at most
/// `limit` of them: the last ones, as the page shows the newest first.
pub fn search(paths: &[String], needle: &str, limit: usize) -> Result<Vec<Match>, AtlasError> {
    let needle = needle.to_lowercase();
    let mut found = std::collections::VecDeque::new();
    let mut number = 0u64;
    for path in paths {
        if !std::path::Path::new(path).exists() {
            continue;
        }
        let file = File::open(path).map_err(|e| io(path, e))?;
        let mut reader = BufReader::with_capacity(1 << 20, file);
        let mut line = Vec::new();
        loop {
            line.clear();
            if reader
                .read_until(b'\n', &mut line)
                .map_err(|e| io(path, e))?
                == 0
            {
                break;
            }
            let text = String::from_utf8_lossy(&line);
            if text.to_lowercase().contains(&needle) {
                found.push_back(Match {
                    line: number,
                    text: text.trim_end_matches('\n').to_string(),
                });
                if found.len() > limit {
                    found.pop_front();
                }
            }
            number += 1;
        }
    }
    Ok(found.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segments(parts: &[&str]) -> (tempfile::TempDir, Vec<String>) {
        let dir = tempfile::tempdir().unwrap();
        let paths = parts
            .iter()
            .enumerate()
            .map(|(i, text)| {
                let path = dir.path().join(format!("segment{i}"));
                std::fs::write(&path, text).unwrap();
                path.to_string_lossy().to_string()
            })
            .collect();
        (dir, paths)
    }

    #[test]
    fn a_window_reads_across_segments_oldest_first() {
        let (_dir, paths) = segments(&["a\nb\n", "c\nd\n", "e\n"]);
        let all = window(&paths, None, 100).unwrap();
        assert_eq!(all.text, "a\nb\nc\nd\ne\n");
        assert_eq!((all.first_line, all.total_lines), (0, 5));
        let tail = window(&paths, None, 2).unwrap();
        assert_eq!((tail.text.as_str(), tail.first_line), ("d\ne\n", 3));
        let earlier = window(&paths, Some(3), 2).unwrap();
        assert_eq!((earlier.text.as_str(), earlier.first_line), ("b\nc\n", 1));
        let none = window(&paths, Some(0), 2).unwrap();
        assert_eq!((none.text.as_str(), none.first_line), ("", 0));
    }

    #[test]
    fn a_last_line_without_a_newline_counts_and_a_missing_segment_is_empty() {
        let (dir, mut paths) = segments(&["a\n", "b"]);
        paths.insert(0, dir.path().join("gone").to_string_lossy().to_string());
        let all = window(&paths, None, 10).unwrap();
        assert_eq!((all.text.as_str(), all.total_lines), ("a\nb\n", 2));
    }

    #[test]
    fn search_finds_lines_in_every_segment_and_keeps_the_last_ones() {
        let (_dir, paths) = segments(&["Error one\nok\n", "ok\nerror two\n", "ERROR three\n"]);
        let found = search(&paths, "error", 10).unwrap();
        assert_eq!(
            found.iter().map(|m| m.line).collect::<Vec<_>>(),
            vec![0, 3, 4]
        );
        let last = search(&paths, "error", 2).unwrap();
        assert_eq!(last[0].text, "error two");
        assert_eq!(last[1].text, "ERROR three");
    }
}
