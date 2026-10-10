# ColumnAtlas

The block output explorer. User guide: `docs/design/column-atlas.mdx`.

## Layout

| Part | Path | Role |
| --- | --- | --- |
| Engine | `rust/column_atlas` | Rust crate on Polars 0.55: reads one Parquet file, applies a view, returns rows and summaries as JSON |
| Extension | same crate, `python` feature | PyO3 module `column_atlas_native`, built by maturin as `column-atlas-native` (abi3, Python 3.11+) |
| Service | `mage_ai/column_atlas` | Validates requests, resolves sources, caches results, runs queries in worker processes |
| API | `mage_ai/api/resources/ColumnAtlasQueryResource.py` | `POST /api/column_atlas_queries` |
| UI | `mage_ai/frontend/components/ColumnAtlas` | Virtualized grid, profiles, filters, full window |
| Mage hook | `components/CodeBlock/CodeOutput/TableOutput.tsx` | Renders ColumnAtlas for `output_N` tables; the plain table is the fallback |

## Request

```json
{
  "column_atlas_query": {
    "action": "rows",
    "source": {"pipeline_uuid": "p", "block_uuid": "b", "variable_uuid": "output_0", "block_run_id": 12},
    "view": {"filters": [{"column_id": 3, "op": "gt", "value": "200"}], "sort": [{"column_id": 2, "descending": true}]},
    "offset": 0,
    "limit": 128,
    "columns": [0, 1, 2]
  }
}
```

- `action`: `metadata`, `count`, `rows` or `summaries` (`bins` 4 to 64).
- `block_run_id` selects the output of a block run; without it, the notebook output.
  The block run must belong to the pipeline and block.
- The view is typed data, not expressions: up to 16 filters and 8 sort keys, 16 KB.
  Values travel as text, so integers past 2^53 and decimals keep every digit; the engine
  parses them with the column's type.
- Limits: 512 rows and 48 columns per `rows` request, 16 columns per `summaries`
  request.
- Errors: 400 invalid request, 404 output not available here (the UI shows the plain
  table), 503 busy, 504 deadline passed. The message never carries the file path.

## Sources

`sources.resolve` accepts only outputs whose stored type is `DATAFRAME`,
`POLARS_DATAFRAME` or `GEO_DATAFRAME`, on local storage, whose real path sits inside the project's variables
directory. Names are checked against `^[\w\-.:/ ]{1,255}$` with no `..` segment. The
generation of a file (inode, size, modification time) keys every cache, so a block that
runs again invalidates them.

## Engine

- Scans with `LazyFrame::scan_parquet`. A window of the unfiltered output is a slice,
  pushed down to the row groups.
- A filtered or sorted view is computed once into row positions and cached in a
  byte-bounded LRU (64 MB per process). Windows gather rows by position: a row index, a
  slice over the positions' range and `is_in`.
- Sort is stable with nulls last. Ordering filters exclude NaN, as IEEE and pandas do;
  Polars orders NaN above every number.
- Datetime literals are wall times in the column's zone (`replace_time_zone`).
- Cells are formatted per type; text is cut at 256 characters. Floats print their
  shortest round-trip form.
- Geometry columns are the WKB columns that the GeoParquet `geo` metadata lists. Cells
  are converted to WKT (`src/wkb.rs`: ISO WKB with the EWKB Z, M and SRID flags, all
  seven geometry types), and the conversion stops at the cell's length limit. Arrow
  extension types Polars does not know, such as `geoarrow.wkb`, load as their storage
  type.
- Every `Table` method runs under `catch_unwind`: a Polars panic, such as on a type it
  cannot read, becomes an engine error and the process keeps serving.
- Summaries are exact except the distinct count of numeric and temporal columns
  (`approx_n_unique`). Integer columns with a small range get one bin per value.

## Processes

`WorkerPool` starts `MAGE_COLUMN_ATLAS_WORKERS` processes with spawn. A source always goes
to the same process (crc32 of its key), so its open file and cached orders are reused.
A query past its deadline, or a process that dies, is replaced by a new process; the
request fails with 504 or 500 and the next one works. On Linux each process has an
`RLIMIT_DATA` limit (`RLIMIT_AS` would count the memory-mapped Parquet file). Workers exit
when the server's end of their pipe closes.

## Performance

Apple M-series, release build, one process, 2 threads:

| Case | Time |
| --- | --- |
| 2M rows × 12 columns: metadata | 0.4 ms |
| 128 rows, any offset | 4 to 20 ms |
| Filter (2 conditions) and sort, first page | 28 ms |
| Sort 2M unique strings, first page / next page | 26 ms / 10 ms |
| `contains` over 2M strings, count | 8 ms |
| Summaries of 12 columns, 2M rows | 650 ms (peak 1.0 GB) |
| 2000 columns × 20k rows: metadata / 128 × 48 window / 16 summaries | 0.8 / 8 / 37 ms |

## Tests

- `cd rust/column_atlas && cargo test`; CI also runs `cargo fmt --check` and
  `cargo clippy --all-targets --features python -- -D warnings`.
- `pytest mage_ai/tests/column_atlas`: sources, worker pool (timeouts, crashes), service,
  endpoint permissions.
- `yarn test:unit` in `mage_ai/frontend`: layout, scaling of tall tables, filters, sort,
  formatting.
- `tests/column_atlas.spec.ts` (Playwright): run a block, sort, filter, full window,
  column summaries, cell inspector, fallback to the plain table.

## Building

`uv sync` builds the extension; it needs the Rust version in
`rust/column_atlas/rust-toolchain.toml` (rustup installs it). uv rebuilds when a file
listed in the crate's `tool.uv.cache-keys` changes. The Docker image compiles it in the
`python-build` stage; the runtime image has no Rust toolchain. The publish workflow
builds manylinux, macOS and Windows wheels with maturin.
