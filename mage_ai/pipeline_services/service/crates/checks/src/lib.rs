//! Data checks of MageML over Parquet or Arrow IPC files, read whole: contracts (the rules
//! a block's output must meet) and the column profiles monitors compare across runs.
//! MageML runs them through ColumnAtlas (column_atlas_native) and exported services in the
//! engine (feature `checks` of mage-service): one implementation for both.

mod error;
pub mod kind;
mod table;

pub mod contract;
pub mod monitor;
pub mod profile;

pub use error::Error;
