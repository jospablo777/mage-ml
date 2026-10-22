//! Run logs read as one text over their segments (the shared mage-log-window crate, which
//! the gateway reads them with too), with column_atlas's errors.

pub use mage_log_window::{Match, Window};

use crate::error::AtlasError;

/// The `count` lines that end before line `end`, over the segments oldest first.
pub fn window(paths: &[String], end: Option<u64>, count: u64) -> Result<Window, AtlasError> {
    mage_log_window::window(paths, end, count).map_err(AtlasError::Engine)
}

/// Lines that contain `needle` (case-insensitive), the last `limit` of them.
pub fn search(paths: &[String], needle: &str, limit: usize) -> Result<Vec<Match>, AtlasError> {
    mage_log_window::search(paths, needle, limit).map_err(AtlasError::Engine)
}
