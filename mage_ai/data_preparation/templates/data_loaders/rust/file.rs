use mage::prelude::*;

/// Reads a CSV or Parquet file lazily: Polars reads only the columns and rows the
/// downstream query of this block needs. Set the path with a pipeline variable.
fn load_data(vars: Vars) -> Result<LazyFrame> {
    let path: String = vars.get_or("path", "data.parquet".to_string())?;
    let frame = if path.ends_with(".csv") {
        LazyCsvReader::new(PlRefPath::try_from_path(std::path::Path::new(&path))?)
            .with_has_header(true)
            .finish()?
    } else {
        LazyFrame::scan_parquet(PlRefPath::try_from_path(std::path::Path::new(&path))?, Default::default())?
    };
    Ok(frame)
}

fn test_output(output: &DataFrame) -> Result<()> {
    ensure!(output.width() > 0, "The file has no columns");
    Ok(())
}
