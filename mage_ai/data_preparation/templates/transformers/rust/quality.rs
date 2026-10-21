use mage::prelude::*;

/// Checks the rows before they go on, and fails the block with every problem found: the
/// columns in the pipeline variable `required` (default ["id"]) have no missing values,
/// the column `unique` (default id) has no repeated values, and the column `amount`
/// (change it to yours) is not negative. The rows pass on unchanged.
fn transform(rows: DataFrame, vars: Vars) -> Result<DataFrame> {
    let required: Vec<String> = vars.get_or("required", vec!["id".to_string()])?;
    let unique: String = vars.get_or("unique", "id".to_string())?;
    let mut problems = Vec::new();
    for name in &required {
        let missing = rows.column(name)?.null_count();
        if missing > 0 {
            problems.push(format!("{name} is missing in {missing} rows"));
        }
    }
    let column = rows.column(&unique)?;
    let repeated = rows.height() - column.n_unique()?;
    if repeated > 0 {
        problems.push(format!("{unique} repeats in {repeated} rows"));
    }
    if let Ok(amount) = rows.column("amount") {
        let negative = amount.cast(&DataType::Float64)?.f64()?.lt(0.0).sum().unwrap_or(0);
        if negative > 0 {
            problems.push(format!("amount is negative in {negative} rows"));
        }
    }
    ensure!(problems.is_empty(), "The rows failed their checks: {}", problems.join("; "));
    Ok(rows)
}
