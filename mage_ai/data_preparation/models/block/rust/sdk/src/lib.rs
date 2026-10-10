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

pub mod context;
pub mod extract;
pub mod output;
pub mod protocol;
pub mod runtime;

pub use context::{BlockContext, Vars};
/// The Polars version the SDK uses; add features in the project's Cargo.toml.
pub use polars;
pub use runtime::{main, run_job, test};

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
