//! Data contracts, checked by mage-contracts (the engine's crate, so MageML runs and
//! exported services check outputs with one checker): this module maps its errors to
//! ColumnAtlas's.

pub use mage_contracts::{ColumnRule, ExtraColumns, Report, Violation, MAX_EXAMPLES};
use serde_json::Value;

use crate::error::AtlasError;

impl From<mage_contracts::Error> for AtlasError {
    fn from(error: mage_contracts::Error) -> Self {
        match error {
            mage_contracts::Error::Invalid(message) => AtlasError::Invalid(message),
            mage_contracts::Error::Engine(message) => AtlasError::Engine(message),
        }
    }
}

/// A contract, checked for valid rules.
pub struct Contract;

impl Contract {
    pub fn parse(text: &str) -> Result<mage_contracts::Contract, AtlasError> {
        Ok(mage_contracts::Contract::parse(text)?)
    }
}

/// Checks a Parquet or Arrow IPC file against a contract (JSON).
pub fn validate(path: &str, contract: &str, examples: usize) -> Result<Report, AtlasError> {
    Ok(mage_contracts::validate(path, contract, examples)?)
}

/// A contract draft with the columns, types and nullability of a file.
pub fn infer(path: &str, name: &str) -> Result<Value, AtlasError> {
    Ok(mage_contracts::infer(path, name)?)
}
