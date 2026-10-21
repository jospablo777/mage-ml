//! The file a contract checks: a lazy scan of a Parquet or Arrow IPC file, with its
//! columns and row count, read the way ColumnAtlas reads it.
use polars::prelude::*;

use crate::error::Error;

pub(crate) struct Column {
    pub name: String,
    pub dtype: DataType,
}

pub(crate) struct Table {
    path: String,
    columns: Vec<Column>,
}

/// Columns of an extension type (a UUID written by pyarrow) read as their storage type.
fn allow_extension_types() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        extension::set_unknown_extension_type_behavior(
            extension::UnknownExtensionTypeBehavior::LoadAsStorage,
        );
    });
}

/// Whether a file is Arrow IPC (by its extension), not Parquet.
pub(crate) fn is_ipc(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".arrow") || lower.ends_with(".ipc") || lower.ends_with(".feather")
}

impl Table {
    pub fn open(path: &str, _order_cache_bytes: usize) -> Result<Self, Error> {
        if !std::path::Path::new(path).is_file() {
            return Err(Error::invalid("The output file does not exist"));
        }
        allow_extension_types();
        let mut table = Table {
            path: path.to_string(),
            columns: Vec::new(),
        };
        let schema = table
            .scan()?
            .collect_schema()
            .map_err(|error| table.error(error))?;
        table.columns = schema
            .iter()
            .map(|(name, dtype)| Column {
                name: name.to_string(),
                dtype: dtype.clone(),
            })
            .collect();
        Ok(table)
    }

    fn error(&self, error: impl Into<Error>) -> Error {
        error.into().without_path(&self.path)
    }

    pub fn scan(&self) -> Result<LazyFrame, Error> {
        let path = PlRefPath::from(self.path.as_str());
        let scanned = if is_ipc(&self.path) {
            LazyFrame::scan_ipc(path, Default::default(), Default::default())
        } else {
            LazyFrame::scan_parquet(path, ScanArgsParquet::default())
        };
        scanned.map_err(|error| self.error(error))
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn collect(&self, frame: LazyFrame) -> Result<DataFrame, Error> {
        frame.collect().map_err(|error| self.error(error))
    }
}
