use mage::prelude::*;

// Needs `ureq = { version = "3", features = ["json"] }` under [workspace.dependencies] in
// the project's rust/Cargo.toml; projects created with this version of Mage have it.
/// Sends the upstream block's rows to an HTTP API as JSON, in batches. The URL comes from
/// the pipeline variable `url` and the batch size from `batch_size`; a token in the
/// environment variable API_TOKEN is sent as a bearer token.
fn export_data(data: DataFrame, vars: Vars) -> Result<()> {
    let url: String = vars.get_or("url", "https://api.example.com/records".to_string())?;
    let batch_size: usize = vars.get_or("batch_size", 1000)?;
    let token = std::env::var("API_TOKEN").ok();
    let mut sent = 0;
    for offset in (0..data.height()).step_by(batch_size.max(1)) {
        let mut batch = data.slice(offset as i64, batch_size);
        let mut buffer = Vec::new();
        JsonWriter::new(&mut buffer)
            .with_json_format(JsonFormat::Json)
            .finish(&mut batch)?;
        let mut request = ureq::post(&url).header("Content-Type", "application/json");
        if let Some(token) = &token {
            request = request.header("Authorization", &format!("Bearer {token}"));
        }
        request
            .send(&buffer[..])
            .with_context(|| format!("POST {url}, rows {offset} to {}", offset + batch.height()))?;
        sent += batch.height();
    }
    println!("Sent {sent} rows to {url}");
    Ok(())
}
