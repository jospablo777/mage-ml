use mage::prelude::*;

/// Decides whether the block this condition is attached to runs: return true to run it.
/// It can take the same upstream outputs as that block (LazyFrame, DataFrame, Value) and
/// the pipeline variables.
fn condition(vars: Vars) -> Result<bool> {
    let enabled: bool = vars.get_or("enabled", true)?;
    Ok(enabled)
}
