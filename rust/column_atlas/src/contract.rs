//! Data contracts, checked by mage-checks (the engine's crate, so MageML runs and
//! exported services check outputs with one checker): this module maps its errors to
//! ColumnAtlas's.

pub use mage_checks::contract::{ColumnRule, ExtraColumns, Report, Violation, MAX_EXAMPLES};
use serde_json::Value;

use crate::error::AtlasError;

impl From<mage_checks::Error> for AtlasError {
    fn from(error: mage_checks::Error) -> Self {
        match error {
            mage_checks::Error::Invalid(message) => AtlasError::Invalid(message),
            mage_checks::Error::Engine(message) => AtlasError::Engine(message),
        }
    }
}

/// A contract, checked for valid rules.
pub struct Contract;

impl Contract {
    pub fn parse(text: &str) -> Result<mage_checks::contract::Contract, AtlasError> {
        Ok(mage_checks::contract::Contract::parse(text)?)
    }
}

/// Checks a Parquet or Arrow IPC file against a contract (JSON).
pub fn validate(path: &str, contract: &str, examples: usize) -> Result<Report, AtlasError> {
    Ok(mage_checks::contract::validate(path, contract, examples)?)
}

/// A contract draft with the columns, types and nullability of a file.
pub fn infer(path: &str, name: &str) -> Result<Value, AtlasError> {
    Ok(mage_checks::contract::infer(path, name)?)
}

/// The differences between two versions of a contract and whether the new version number
/// fits them.
pub fn compatibility(
    old: &str,
    new: &str,
) -> Result<mage_checks::contract::Compatibility, AtlasError> {
    Ok(mage_checks::contract::compatibility(old, new)?)
}
