//! Runtime of Mage Rust blocks.
//!
//! A block is a Rust file with one function named for its type: `load_data`,
//! `transform`, `export_data`, `custom` or `condition`. Its parameters are the upstream
//! outputs and run values it needs, and its return value is its output:
//!
//! ```ignore
//! use mage::prelude::*;
//!
//! fn transform(orders: LazyFrame, vars: Vars) -> Result<LazyFrame> {
//!     let minimum: f64 = vars.get_or("minimum", 0.0)?;
//!     Ok(orders.filter(col("amount").gt(lit(minimum))))
//! }
//!
//! fn test_amounts(output: &DataFrame) -> Result<()> {
//!     ensure!(output.height() > 0, "No orders");
//!     Ok(())
//! }
//! ```
//!
//! Mage generates the binary's `main`, which calls [`main`] with the block function and
//! its `test_*` functions.

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub mod context;
#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod extract;
#[cfg(feature = "feast")]
pub mod feast;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "onnx")]
pub mod onnx;
pub mod output;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod profile;
pub mod protocol;
#[cfg(feature = "redis")]
pub mod redis;
pub mod runtime;
#[cfg(feature = "storage")]
pub mod storage;
pub mod stream;

pub use context::{BlockContext, Vars};
/// The Polars version the SDK uses; add features in the project's Cargo.toml.
pub use polars;
pub use runtime::{ChainBlock, ChainEntries, chain_block, main, run_chain, run_job, test};
pub use stream::{StreamBlock, StreamEntries, serve_stream, stream_block};

pub mod prelude {
    pub use crate::context::{BlockContext, Vars};
    pub use anyhow::{Context as _, Result, anyhow, bail, ensure};
    pub use polars::prelude::*;
    pub use serde_json::{Value, json};

    /// Shadows the trait of the same name from the Polars prelude, whose `context` and
    /// `with_context` methods would make anyhow's ambiguous on every Polars result.
    /// With this, `.context("...")` works on any result and returns `anyhow::Result`.
    #[doc(hidden)]
    pub trait PolarsContext {}
}
