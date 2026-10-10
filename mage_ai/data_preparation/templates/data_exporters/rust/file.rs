use mage::prelude::*;

/// Writes the upstream block's output to a Parquet or CSV file, chosen by the extension of
/// the pipeline variable `output_path`. Parquet is compressed with zstd.
fn export_data(data: LazyFrame, vars: Vars) -> Result<()> {
    let path: String = vars.get_or("output_path", "output.parquet".to_string())?;
    let mut frame = data.collect()?;
    if let Some(parent) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::File::create(&path).with_context(|| format!("Creating {path}"))?;
    if path.ends_with(".csv") {
        CsvWriter::new(file).include_header(true).finish(&mut frame)?;
    } else {
        ParquetWriter::new(file)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
    }
    println!("Wrote {} rows and {} columns to {path}", frame.height(), frame.width());
    Ok(())
}
