use mage::prelude::*;

/// Runs on each batch of the stream's messages, in one program that stays up for the
/// stream: code at the top of the file, such as loading a model, runs once.
///
/// Messages come as Vec<Value> (JSON), a DataFrame (one row per JSON object), Vec<String>
/// or Vec<Vec<u8>> (raw text or bytes); Vars gives the pipeline's variables. Return the
/// messages to send on, in any of those forms; an error sends the batch to the stream's
/// error handling, and with a dead-letter sink only the messages that fail go there.
fn transform(messages: Vec<Value>, vars: Vars) -> Result<Vec<Value>> {
    let minimum: f64 = vars.get_or("minimum", 0.0)?;
    Ok(messages
        .into_iter()
        .filter(|message| message["amount"].as_f64().unwrap_or(0.0) >= minimum)
        .map(|mut message| {
            message["seen_by"] = json!("rust");
            message
        })
        .collect())
}
