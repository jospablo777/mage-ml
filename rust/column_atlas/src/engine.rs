//! A read-only table over one Parquet file. Unfiltered, unsorted windows read only the row
//! groups they need. A filtered or sorted view is computed once into the list of its rows'
//! positions in the file, kept in a byte-bounded cache, and each window reads the rows at
//! its positions.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use polars::prelude::*;
use serde::Serialize;
use serde_json::{json, Value};

use crate::error::{guarded, AtlasError};
use crate::format;
use crate::schema::{columns_of, geometry_columns, ColumnInfo};
use crate::summary;
use crate::view::{apply_filters, apply_sort, validate, ViewSpec};

pub const MAX_ROWS: usize = 512;
pub const MAX_COLUMNS: usize = 48;
pub const MAX_SUMMARY_COLUMNS: usize = 16;
pub const DEFAULT_ORDER_CACHE_BYTES: usize = 64 * 1024 * 1024;
const ROW_POSITION: &str = "__column_atlas_row";

/// Row positions of computed views, most recently used last, within a byte budget.
struct OrderCache {
    budget: usize,
    used: usize,
    entries: VecDeque<(String, Arc<Vec<IdxSize>>)>,
}

impl OrderCache {
    fn new(budget: usize) -> Self {
        OrderCache {
            budget,
            used: 0,
            entries: VecDeque::new(),
        }
    }

    fn bytes(rows: &[IdxSize]) -> usize {
        std::mem::size_of_val(rows)
    }

    fn get(&mut self, key: &str) -> Option<Arc<Vec<IdxSize>>> {
        let index = self.entries.iter().position(|(entry, _)| entry == key)?;
        let entry = self.entries.remove(index)?;
        let rows = entry.1.clone();
        self.entries.push_back(entry);
        Some(rows)
    }

    fn put(&mut self, key: String, rows: Arc<Vec<IdxSize>>) {
        let size = Self::bytes(&rows);
        if size > self.budget {
            // A view larger than the whole budget is not kept; each window recomputes it.
            return;
        }
        while self.used + size > self.budget {
            match self.entries.pop_front() {
                Some((_, old)) => self.used -= Self::bytes(&old),
                None => break,
            }
        }
        self.used += size;
        self.entries.push_back((key, rows));
    }
}

/// Arrow extension types that Polars does not know, such as GeoArrow's, load as their
/// storage type. Polars panicked on them before the feature, and warns without this.
fn allow_extension_types() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        extension::set_unknown_extension_type_behavior(
            extension::UnknownExtensionTypeBehavior::LoadAsStorage,
        );
    });
}

pub struct Table {
    path: String,
    columns: Vec<ColumnInfo>,
    row_count: u64,
    orders: Mutex<OrderCache>,
}

#[derive(Debug, Serialize)]
pub struct Rows {
    pub offset: usize,
    pub column_ids: Vec<usize>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl Table {
    /// Opens the file. Every query of the table turns a panic of the engine into an
    /// error, so a file the engine cannot read fails its request, not the process.
    pub fn open(path: &str, order_cache_bytes: usize) -> Result<Self, AtlasError> {
        guarded(path, || Self::open_unguarded(path, order_cache_bytes))
    }

    fn open_unguarded(path: &str, order_cache_bytes: usize) -> Result<Self, AtlasError> {
        let file = std::path::Path::new(path);
        if !file.is_file() {
            return Err(AtlasError::invalid("The output file does not exist"));
        }
        let mut table = Table {
            path: path.to_string(),
            columns: Vec::new(),
            row_count: 0,
            orders: Mutex::new(OrderCache::new(order_cache_bytes)),
        };
        allow_extension_types();
        let schema = table
            .scan()?
            .collect_schema()
            .map_err(|error| table.error(error))?;
        table.columns = columns_of(&schema, &geometry_columns(&table.geo_metadata()));
        table.row_count = table.count_rows(table.scan()?)?;
        Ok(table)
    }

    /// The GeoParquet metadata of the file, empty when it has none.
    fn geo_metadata(&self) -> String {
        let Ok(file) = std::fs::File::open(&self.path) else {
            return String::new();
        };
        let mut reader = ParquetReader::new(file);
        let Ok(metadata) = reader.get_metadata() else {
            return String::new();
        };
        metadata
            .key_value_metadata
            .iter()
            .flatten()
            .find(|entry| entry.key == "geo")
            .and_then(|entry| entry.value.clone())
            .unwrap_or_default()
    }

    fn error(&self, error: impl Into<AtlasError>) -> AtlasError {
        error.into().without_path(&self.path)
    }

    pub(crate) fn scan(&self) -> Result<LazyFrame, AtlasError> {
        LazyFrame::scan_parquet(
            PlRefPath::from(self.path.as_str()),
            ScanArgsParquet::default(),
        )
        .map_err(|error| self.error(error))
    }

    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    pub(crate) fn collect(&self, frame: LazyFrame) -> Result<DataFrame, AtlasError> {
        frame.collect().map_err(|error| self.error(error))
    }

    fn count_rows(&self, frame: LazyFrame) -> Result<u64, AtlasError> {
        let counted = self.collect(frame.select([len().alias("n")]))?;
        let value = counted
            .column("n")
            .map_err(|error| self.error(error))?
            .get(0)
            .map_err(|error| self.error(error))?;
        value
            .extract::<u64>()
            .ok_or_else(|| AtlasError::engine("The row count is not a number"))
    }

    pub fn metadata(&self) -> Value {
        json!({
            "version": 1,
            "row_count": self.row_count,
            "columns": self.columns.iter().map(|column| column.to_json()).collect::<Vec<_>>(),
        })
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    /// The positions in the file of the view's rows, in the view's order.
    fn order(&self, view: &ViewSpec) -> Result<Arc<Vec<IdxSize>>, AtlasError> {
        let key = view.canonical();
        if let Some(rows) = self
            .orders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&key)
        {
            return Ok(rows);
        }
        let frame = self.scan()?.with_row_index(ROW_POSITION, None);
        let frame = apply_filters(frame, &self.columns, view)?;
        let frame = apply_sort(frame, &self.columns, view)?;
        let positions = self.collect(frame.select([col(ROW_POSITION)]))?;
        let positions = positions
            .column(ROW_POSITION)
            .map_err(|error| self.error(error))?
            .as_materialized_series()
            .idx()
            .map_err(|error| self.error(error))?
            .into_no_null_iter()
            .collect::<Vec<IdxSize>>();
        let rows = Arc::new(positions);
        self.orders
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .put(key, rows.clone());
        Ok(rows)
    }

    pub fn count(&self, view: &ViewSpec) -> Result<u64, AtlasError> {
        guarded(&self.path, || self.count_unguarded(view))
    }

    fn count_unguarded(&self, view: &ViewSpec) -> Result<u64, AtlasError> {
        validate(&self.columns, view)?;
        if view.is_identity() {
            return Ok(self.row_count);
        }
        if !view.has_filters() {
            return Ok(self.row_count);
        }
        Ok(self.order(view)?.len() as u64)
    }

    fn names(&self, ids: &[usize]) -> Result<Vec<String>, AtlasError> {
        ids.iter()
            .map(|id| {
                self.columns
                    .get(*id)
                    .map(|column| column.name.clone())
                    .ok_or_else(|| AtlasError::invalid(format!("Unknown column {id}")))
            })
            .collect()
    }

    pub fn rows(
        &self,
        view: &ViewSpec,
        offset: usize,
        limit: usize,
        ids: &[usize],
    ) -> Result<Rows, AtlasError> {
        guarded(&self.path, || self.rows_unguarded(view, offset, limit, ids))
    }

    fn rows_unguarded(
        &self,
        view: &ViewSpec,
        offset: usize,
        limit: usize,
        ids: &[usize],
    ) -> Result<Rows, AtlasError> {
        if limit == 0 || limit > MAX_ROWS {
            return Err(AtlasError::invalid(format!(
                "A window takes 1 to {MAX_ROWS} rows"
            )));
        }
        if ids.is_empty() || ids.len() > MAX_COLUMNS {
            return Err(AtlasError::invalid(format!(
                "A window takes 1 to {MAX_COLUMNS} columns"
            )));
        }
        validate(&self.columns, view)?;
        let names = self.names(ids)?;
        let selected: Vec<Expr> = names.iter().map(|name| col(name.as_str())).collect();

        let frame = if view.is_identity() {
            if offset as u64 >= self.row_count {
                return Ok(Rows {
                    offset,
                    column_ids: ids.to_vec(),
                    rows: Vec::new(),
                });
            }
            self.collect(
                self.scan()?
                    .slice(offset as i64, limit as IdxSize)
                    .select(selected),
            )?
        } else {
            let order = self.order(view)?;
            if offset >= order.len() {
                return Ok(Rows {
                    offset,
                    column_ids: ids.to_vec(),
                    rows: Vec::new(),
                });
            }
            let window = &order[offset..(offset + limit).min(order.len())];
            self.gather(window, selected)?
        };
        Ok(Rows {
            offset,
            column_ids: ids.to_vec(),
            rows: self.texts(&frame, &names)?,
        })
    }

    /// The rows at the given file positions, in that order. Only the row groups between the
    /// lowest and highest position are read: a filtered window is a short stretch of the
    /// file; a sorted one can span all of it.
    fn gather(&self, positions: &[IdxSize], selected: Vec<Expr>) -> Result<DataFrame, AtlasError> {
        let low = *positions.iter().min().expect("non-empty window");
        let high = *positions.iter().max().expect("non-empty window");
        let wanted = Series::new(ROW_POSITION.into(), positions.to_vec());
        let mut columns = selected;
        columns.push(col(ROW_POSITION));
        let found = self.collect(
            self.scan()?
                .with_row_index(ROW_POSITION, None)
                .slice(low as i64, (high - low + 1) as IdxSize)
                .filter(col(ROW_POSITION).is_in(lit(wanted).implode(false), false))
                .select(columns),
        )?;
        let found_positions: Vec<IdxSize> = found
            .column(ROW_POSITION)
            .map_err(|error| self.error(error))?
            .as_materialized_series()
            .idx()
            .map_err(|error| self.error(error))?
            .into_no_null_iter()
            .collect();
        let mut at = std::collections::HashMap::with_capacity(found_positions.len());
        for (index, position) in found_positions.iter().enumerate() {
            at.insert(*position, index as IdxSize);
        }
        let take: Vec<IdxSize> = positions
            .iter()
            .map(|position| {
                at.get(position)
                    .copied()
                    .ok_or_else(|| AtlasError::engine("A row of the view is missing from the file"))
            })
            .collect::<Result<_, _>>()?;
        let indices = IdxCa::from_vec("take".into(), take);
        found.take(&indices).map_err(|error| self.error(error))
    }

    fn texts(
        &self,
        frame: &DataFrame,
        names: &[String],
    ) -> Result<Vec<Vec<Option<String>>>, AtlasError> {
        let columns: Vec<&Column> = names
            .iter()
            .map(|name| {
                frame
                    .column(name.as_str())
                    .map_err(|error| self.error(error))
            })
            .collect::<Result<_, _>>()?;
        let geometry: Vec<bool> = names
            .iter()
            .map(|name| {
                self.columns
                    .iter()
                    .any(|column| &column.name == name && column.geometry.is_some())
            })
            .collect();
        let mut rows = Vec::with_capacity(frame.height());
        for row in 0..frame.height() {
            let mut cells = Vec::with_capacity(columns.len());
            for (column, geometry) in columns.iter().zip(&geometry) {
                let value = column.get(row).map_err(|error| self.error(error))?;
                cells.push(if *geometry {
                    format::geometry(&value)
                } else {
                    format::cell(&value)
                });
            }
            rows.push(cells);
        }
        Ok(rows)
    }

    pub fn summaries(
        &self,
        view: &ViewSpec,
        ids: &[usize],
        bins: usize,
    ) -> Result<Vec<Value>, AtlasError> {
        guarded(&self.path, || self.summaries_unguarded(view, ids, bins))
    }

    fn summaries_unguarded(
        &self,
        view: &ViewSpec,
        ids: &[usize],
        bins: usize,
    ) -> Result<Vec<Value>, AtlasError> {
        if ids.is_empty() || ids.len() > MAX_SUMMARY_COLUMNS {
            return Err(AtlasError::invalid(format!(
                "A summary request takes 1 to {MAX_SUMMARY_COLUMNS} columns"
            )));
        }
        if !(4..=64).contains(&bins) {
            return Err(AtlasError::invalid("Histograms take 4 to 64 bins"));
        }
        validate(&self.columns, view)?;
        let filtered = apply_filters(self.scan()?, &self.columns, view)?;
        ids.iter()
            .map(|id| {
                let column = self
                    .columns
                    .get(*id)
                    .ok_or_else(|| AtlasError::invalid(format!("Unknown column {id}")))?;
                summary::column(self, filtered.clone(), column, bins)
            })
            .collect()
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}
