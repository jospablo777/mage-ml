//! ColumnAtlas: read-only queries over a Parquet file for the block output explorer of
//! Mage. The engine has no Mage dependency; the Python service resolves and authorizes
//! sources and hands this crate a local file path.

pub mod engine;
pub mod error;
pub mod format;
pub mod schema;
pub mod summary;
pub mod view;
pub mod wkb;

#[cfg(feature = "python")]
mod python;

pub use engine::Table;
pub use error::AtlasError;
pub use view::ViewSpec;
