# Rust pipeline optimization

Status: done for this round (2026-10-10). Measured on macOS, Apple M-series, 14 cores,
Python 3.12, with the 5-block chain of `integration_tests/benchmark` (load, clean, enrich,
aggregate, export on 1,000,000 rows) and the playground's `rust_postgres_margins`
(1.2 million order lines from PostgreSQL, two branches, two exports).

## How to reproduce

```bash
make -C integration_tests test-benchmark                    # pandas and Rust chains
MAGE_RUST_TIMINGS=1 make -C integration_tests test-benchmark  # plus phases per block
MAGE_BENCHMARK_ROWS=3000000 MAGE_BENCHMARK_RESULTS=/tmp/rust.json \
  uv run python -m pytest integration_tests/benchmark/test_rust_pipeline_benchmark.py -s
```

`MAGE_RUST_TIMINGS=1` prints, for every Rust block, the time to write its inputs, run its
process and read its outputs (Python side), and inside the process the time to read the
job, run the function (lazy plans compute in `outputs`), write the outputs and run the
tests. Single runs vary by up to a second; compare several.

## What a Rust block costs

A Rust block is its own binary, run in its own process. The upstream outputs reach it as
the stored Parquet files (or as Arrow IPC from memory inside a fused stage), and it
returns Arrow IPC. Before this round, most of the time of a Rust pipeline was not Rust:

| Cost | Before | After | Fix |
| --- | --- | --- | --- |
| Hashing the SDK before each block | 1.2 s per block (in a checkout) | 1 ms | b9b17f7d9: prune build output, cache the digest by size and mtime |
| macOS checking the binary again | 0.45 s per block | 0 | 0671819a0: the cache records use beside the binary, not in its mtime |
| Rebuilds after adding any block | every block, once, plus 0.7 s for macOS to check the new binary | none | b495dedaf: the cache key leaves the block crates out of Cargo.lock |
| Imports in each job process | 4.5 s | 0.8 s | 065a598ae: dbt, scikit-learn, IPython, jupyter_client, aiohttp imported where used |

Results:

| Run | Before | After |
| --- | --- | --- |
| `rust_postgres_margins`, warm, `mage run` | 15.5 s | 6 s |
| Rust chain, block by block, through the scheduler | 20.6 s | 10 to 11.7 s |
| Rust chain, fused (`block_fusion: chains`) | 10.8 s | 2.8 to 5.7 s |
| pandas chain, block by block | 17.9 s | 8.7 to 11.8 s |

The process of a cached Rust block now starts in about 10 ms. Inside a fused stage, a
Rust block on 1 million rows spends about 10 ms writing its input, 30 to 50 ms in its
process and 10 ms reading its output.

## What was evaluated and not done

- **One binary for a chain of Rust blocks**, passing frames and lazy plans in memory.
  It would save the 20 to 30 ms of Arrow IPC per block measured above, and would let
  Polars optimize across blocks. Mage still stores every block's output, which
  materializes each frame anyway, and the blocks would lose their own process, build
  cache entry and error isolation. Not worth it at these numbers.
- **The Rust process writing the stored Parquet output itself.** Polars writes it in
  Rust on either side; the saving is one IPC read, about 10 ms per million rows.
- **Starting job processes from a forkserver** that preloads Mage. A forked job started
  in 5 ms instead of about 1.8 s, but pandas and Polars start native threads when they
  are imported, so the preloaded server is not single-threaded and its children
  crashed (SIGSEGV) on macOS. Mage's own modules import pandas, so preloading less does
  not help. What came out of it: forked processes no longer share their parent's
  database connections (aaa00a8c4), which also protects the Linux workers that fork.

## Where the time goes now

- **Per job process (block by block):** about 1 s from the scheduler starting the job to
  the block starting (interpreter, imports, database session) and 0.5 to 0.7 s of
  first-use work inside the job. On Linux with Python 3.12 jobs fork from a loaded
  process and pay less; on macOS, Windows and Linux from Python 3.14 they start fresh.
  Fusion runs a chain as one job and pays it once.
- **Per block, in Mage:** storing the output as Parquet (about 80 ms for 1 million rows),
  run records and database updates: 0.1 to 0.2 s.
- **Rust itself:** tens of milliseconds for these blocks.

## Next steps

1. A long-lived worker per pool slot that runs several block runs (as the pipeline
   services' Python workers do), recycled after N jobs, removes the per-job process
   cost where jobs cannot fork. It has to purge the project's modules between jobs so
   edited code is picked up; design it with the item 27 benchmark numbers.
2. Benchmark R, Python and Rust pipelines with repetitions and medians (item 27).
