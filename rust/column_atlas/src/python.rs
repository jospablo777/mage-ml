use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::engine::{Table, DEFAULT_ORDER_CACHE_BYTES};
use crate::error::AtlasError;
use crate::view::ViewSpec;

fn to_python(error: AtlasError) -> PyErr {
    match error {
        AtlasError::Invalid(message) => PyValueError::new_err(message),
        AtlasError::Engine(message) => PyRuntimeError::new_err(message),
    }
}

fn json(value: &impl serde::Serialize) -> PyResult<String> {
    serde_json::to_string(value).map_err(|error| PyRuntimeError::new_err(error.to_string()))
}

/// A read-only table over one Parquet file. The path must come from a resolver that has
/// authorized the request; the class applies no permissions. Queries release the GIL.
#[pyclass(frozen)]
struct NativeTable {
    table: Table,
}

#[pymethods]
impl NativeTable {
    #[new]
    #[pyo3(signature = (path, order_cache_bytes = DEFAULT_ORDER_CACHE_BYTES))]
    fn new(py: Python<'_>, path: String, order_cache_bytes: usize) -> PyResult<Self> {
        let table = py
            .detach(|| Table::open(&path, order_cache_bytes))
            .map_err(to_python)?;
        Ok(NativeTable { table })
    }

    fn metadata(&self) -> PyResult<String> {
        json(&self.table.metadata())
    }

    fn count(&self, py: Python<'_>, view: String) -> PyResult<u64> {
        let view = ViewSpec::parse(&view).map_err(to_python)?;
        py.detach(|| self.table.count(&view)).map_err(to_python)
    }

    fn rows(
        &self,
        py: Python<'_>,
        view: String,
        offset: usize,
        limit: usize,
        columns: Vec<usize>,
    ) -> PyResult<String> {
        let view = ViewSpec::parse(&view).map_err(to_python)?;
        let rows = py
            .detach(|| self.table.rows(&view, offset, limit, &columns))
            .map_err(to_python)?;
        json(&rows)
    }

    #[pyo3(signature = (view, columns, bins = 24))]
    fn summaries(
        &self,
        py: Python<'_>,
        view: String,
        columns: Vec<usize>,
        bins: usize,
    ) -> PyResult<String> {
        let view = ViewSpec::parse(&view).map_err(to_python)?;
        let summaries = py
            .detach(|| self.table.summaries(&view, &columns, bins))
            .map_err(to_python)?;
        json(&summaries)
    }
}

/// Checks a Parquet file against a contract (JSON); returns the report as JSON. The counts
/// cover every row; `examples` caps the rows and values listed per violation.
#[pyfunction]
#[pyo3(signature = (path, contract, examples = 10))]
fn validate_contract(
    py: Python<'_>,
    path: String,
    contract: String,
    examples: usize,
) -> PyResult<String> {
    let report = py
        .detach(|| crate::contract::validate(&path, &contract, examples))
        .map_err(to_python)?;
    json(&report)
}

/// Raises ValueError with the reason when a contract (JSON) is not valid.
#[pyfunction]
fn check_contract(contract: String) -> PyResult<()> {
    crate::contract::Contract::parse(&contract)
        .map(|_| ())
        .map_err(to_python)
}

/// A contract draft (JSON) with the columns, types and nullability of a Parquet file.
#[pyfunction]
fn infer_contract(py: Python<'_>, path: String, name: String) -> PyResult<String> {
    let draft = py
        .detach(|| crate::contract::infer(&path, &name))
        .map_err(to_python)?;
    json(&draft)
}

#[pymodule]
fn column_atlas_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<NativeTable>()?;
    module.add_function(wrap_pyfunction!(validate_contract, module)?)?;
    module.add_function(wrap_pyfunction!(infer_contract, module)?)?;
    module.add_function(wrap_pyfunction!(check_contract, module)?)?;
    module.add("MAX_ROWS", crate::engine::MAX_ROWS)?;
    module.add("MAX_COLUMNS", crate::engine::MAX_COLUMNS)?;
    module.add("MAX_SUMMARY_COLUMNS", crate::engine::MAX_SUMMARY_COLUMNS)?;
    Ok(())
}
