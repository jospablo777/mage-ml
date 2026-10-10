use mage::prelude::*;
use rayon::prelude::*;

/// Computes a value per row in parallel with Rayon, for logic that Polars expressions do
/// not express. The thread pool uses every core unless MAGE_RUST_THREADS limits it.
fn transform(data: DataFrame) -> Result<DataFrame> {
    let values = data.column("value")?.f64()?;
    let input: Vec<Option<f64>> = values.iter().collect();
    let scores: Vec<Option<f64>> = input
        .par_iter()
        .map(|value| value.map(score))
        .collect();
    let mut output = data;
    output.with_column(Column::new("score".into(), scores))?;
    Ok(output)
}

/// Replace with your own computation.
fn score(value: f64) -> f64 {
    (0..100).fold(value, |accumulator, step| (accumulator * 1.0001 + step as f64).sqrt())
}
