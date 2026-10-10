use mage::prelude::*;

// Needs `ureq = { version = "3", features = ["json"] }` under [workspace.dependencies] in
// the project's rust/Cargo.toml; projects created with this version of Mage have it.
/// Loads JSON records from an HTTP API into a table. The URL comes from the pipeline
/// variable `url`; the response is a list of objects, or an object whose `data` field is
/// one. A token in the environment variable API_TOKEN is sent as a bearer token.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let url: String = vars.get_or("url", "https://api.example.com/records".to_string())?;
    let mut request = ureq::get(&url).header("Accept", "application/json");
    if let Ok(token) = std::env::var("API_TOKEN") {
        request = request.header("Authorization", &format!("Bearer {token}"));
    }
    let body = request
        .call()
        .with_context(|| format!("GET {url}"))?
        .body_mut()
        .read_to_string()?;
    let payload: Value = serde_json::from_str(&body).context("The response is not JSON")?;
    let records = match payload {
        Value::Object(mut object) => object.remove("data").unwrap_or(Value::Array(vec![])),
        other => other,
    };
    let bytes = serde_json::to_vec(&records)?;
    Ok(JsonReader::new(std::io::Cursor::new(bytes)).finish()?)
}

fn test_has_rows(output: &DataFrame) -> Result<()> {
    ensure!(output.height() > 0, "The API returned no records");
    Ok(())
}
