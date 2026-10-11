# Efficient data exchange

Status: pandas block outputs are written by the Polars Parquet writer when their column
types keep exactly (`write_pandas_parquet` in
`mage_ai/data_preparation/storage/base_storage.py`). Tested in
`mage_ai/tests/data_preparation/storage/test_pandas_parquet_writer.py`.

## Review of the starter

`do_not_commit/top_10_next_features/05_efficient_data_exchange` is a Rust crate that plans
linear regions of Rust blocks and passes immutable Arrow batches between them through
bounded channels, with a byte budget, leases, and a resident catalog that keeps outputs
in memory for ColumnAtlas. Measured against Mage:

- **The exchange between blocks already avoids encoding where it matters.** Block fusion
  passes Python outputs in memory within a stage. Rust blocks read their upstream outputs'
  stored Parquet files in place, and return uncompressed Arrow IPC. Downstream blocks
  read Parquet in about 10 ms per 2 million rows.
- **Resident outputs conflict with what Mage guarantees.** Every block keeps a stored
  output that its run page, ColumnAtlas, retries and run records read. Outputs held only
  in memory disappear with their process; the starter documents that recovery then needs
  replay.
- **The measured cost was the pandas writer.** A transformer on a 2-million-row pandas
  frame took 0.24 s, and 0.18 s of it was pyarrow writing the output Parquet. pyarrow
  encodes columns one after another. Polars writes the same Polars data in 0.02 s.

So the change targets the writer, not the transport.

## Measurements

2 million rows, 7 columns (integers, floats, strings, timestamps, nullable integers),
Apple M-series laptop, best of 5:

| | write | pandas read | Polars read | size |
| --- | --- | --- | --- | --- |
| pyarrow, snappy (before) | 0.163 s | 0.017 s | 0.021 s | 57.8 MB |
| Polars, zstd (after) | 0.032 s | 0.016 s | 0.009 s | 40.9 MB |

A transformer block's `execute_sync` on that output went from 0.24 s to 0.07–0.09 s.
pyarrow options don't close the gap: without statistics and dictionaries it took 0.149
s.

## Exactness

Polars converts the Arrow table to its own types and back. Given the table's schema
(`arrow_schema=`), it writes the original Arrow types and the schema metadata, pandas
metadata included, for integers, floats (not float16), booleans, large strings,
timestamps (any unit and time zone), date32 and decimal128. For dictionaries
(categoricals, including Polars `Enum`), lists, structs, binary and durations, it raises
instead of converting. So:

1. Only frames whose columns are all of those types go to Polars. Others go to pyarrow
   as before.
2. After writing, the file's schema (read from its footer) must equal the table's,
   metadata included; otherwise the file is written again by pyarrow.
3. Subclasses of `pd.DataFrame` (GeoDataFrame writes GeoParquet) use their own
   `to_parquet`.

The test compares the Arrow table and the pandas frame read back from both writers for
a corpus of frames: scalar types with nulls, NaN, -0.0 and infinities, time zones over a
DST change, uint64 above 2^63, named, offset and multi indexes, `df.attrs`, empty
frames, and frames that must fall back. It also tests that a schema mismatch falls back.

## Remaining gaps

1. **Categorical, list, struct, binary and duration columns** keep the pyarrow writer.
   A Rust writer on arrow-rs (`ArrowWriter` with parallel column encoding) would cover
   every Arrow type, at the cost of the arrow and parquet crates in the extension.
2. **Rust block chains** still hand each output back to Python, which stores it.
   Writing the stored output from the Rust process would save one IPC copy per block.
