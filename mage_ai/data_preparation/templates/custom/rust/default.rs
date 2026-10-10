use mage::prelude::*;

/// A custom block takes any upstream outputs and returns any output: a table, a JSON
/// value or nothing.
fn custom(vars: Vars, context: BlockContext) -> Result<Value> {
    println!("Running {} in pipeline {:?}", context.block_uuid, context.pipeline_uuid);
    Ok(json!({ "variables": vars.raw().len() }))
}
