//! The manifest and HTTP protocol of Mage pipeline services: pipelines exported from a
//! Mage project that run as standalone Docker services.

pub mod io_config;
pub mod json;
pub mod manifest;
pub mod protocol;
pub mod redact;
pub mod run_rules;
pub mod time;
