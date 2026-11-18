# Worker protocols

A block's code never runs in the engine's process or, for a Python worker, in MageML's: it
runs in a worker process that MageML or the engine drives through one of two protocols.

| Protocol | Who speaks it | Used for |
| --- | --- | --- |
| JSON lines | MageML's Python worker (`mage_ai/pipeline_services/runtime/worker.py`) | Python, R and SQL blocks in MageML runs (Python workers, pipeline environments) and in exported services; Python streams |
| Frames | The Rust block SDK (`mage::serve_stream`) and R (`mageml::serve_stream`) | Rust and R blocks of a stream, one resident program per stream |

The conformance suite for the JSON-lines protocol is
`mage_ai/tests/pipeline_services/test_worker_conformance.py`. It drives a worker as a black
box, through its process; `MAGE_WORKER_COMMAND` runs it against another implementation:

```bash
MAGE_WORKER_COMMAND="python /path/to/other_worker.py" \
  uv run --no-sync pytest mage_ai/tests/pipeline_services/test_worker_conformance.py
```

## JSON lines (protocol 1)

### Channels

- Requests: one JSON object per line on stdin, or on the descriptor `MAGE_WORKER_REQUEST_FD`
  names (the engine uses 4 and gives the worker `/dev/null` as stdin).
- Replies: one JSON object per line on descriptor 3, or the one `MAGE_WORKER_REPLY_FD`
  names. The worker makes both descriptors non-inheritable, so programs a block starts
  cannot write to them.
- stdout and stderr: what blocks print. MageML files it under the block that is running.
- `MAGE_REPO_PATH`: the project. The worker puts it and its parent on `sys.path` and starts
  in it.

### Messages

| Direction | `type` / `op` | Fields |
| --- | --- | --- |
| worker → | `ready` (first, once) | `protocol` (1), `pid`, `python` |
| → worker | a block (no `op`) | `id`, `block`, `inputs`, `kwargs`, `output_dir`; optional `run_tests`, `end_marker`, `conditions_only` |
| → worker | a chain (no `op`, `chain` set) | `id`, `chain` (blocks in order), `inputs`, `kwargs`, `output_dir`; optional `keep_outputs`, `end_marker` |
| → worker | `op: shutdown` | `id` |
| → worker | `op: stream`, `stream_load`, `r_stream_command` | the engine's stream batches (`mage_ai/pipeline_services/runtime/worker.py`, `_stream_batch`) |
| worker → | `result` (one per request) | `id` (the request's), `ok`, `rss_bytes`; when ok: `outputs`, `tests`, `seconds`, and for a chain `blocks` (each block's `block_uuid` and `seconds`); when not: `error` |
| worker → | `bye` | `id` (the shutdown's); the worker then exits with 0 |

A `block` is `uuid`, `type` (`data_loader`, `transformer`, `data_exporter`, `custom`),
`file` (its code), `configuration`, and optionally `language` (`r`, `sql`), `conditions`,
`pipeline_uuid`, `sha256`.

`error` is `phase`, `message`, `details` (the block's traceback) and, in a chain,
`block_uuid` (the block that failed). The phases: `load` (the block's code did not load),
`input` (an upstream output could not be read), `run` (the block raised), `test` (a test
failed), `output` (an output could not leave the process), `worker` (anything else, such as
an unknown request).

### Values

Inputs and outputs are records of files in the request's `output_dir` (or the caller's
folder for inputs); a returned list or tuple is several outputs, `None` is none.

| `kind` | Fields | Holds |
| --- | --- | --- |
| `frame` | `path`, `format` (`ipc`, `parquet`), `origin` (`pandas`, `pandas_series`, `polars`, `arrow`, `geopandas`), `rows` | A table, as uncompressed Arrow IPC (GeoParquet for geopandas). Lists of plain values stay lists (`mageml.list_columns` in the schema metadata), a DatetimeIndex keeps its frequency (`mageml.index_freq`). |
| `json` | `path` | Values JSON keeps exactly |
| `numpy` | `path` | A NumPy array without Python objects (`.npy`) |
| `pickle` | `path`, `type` | Anything else; it reads back only with the same packages |
| `list` | `items` | An upstream block's several outputs |
| `empty` | | No output |

A table reaches the block as what its Python has: a Polars table as pandas where Polars is
not installed, and the other way round.

### Guarantees a worker gives

The conformance suite checks each one.

1. `ready` comes first, before any request is read.
2. Every request gets exactly one `result` with its `id`, in the order requests came; a
   failure is a `result` with `ok: false`, and the worker serves the next request.
3. With `end_marker`, after the request and before its `result`, the worker prints
   `\x1emage-end <id>` on stdout and on stderr: every line before it belongs to that request.
4. A request does not change the next one's working directory.
5. At the end of its requests (end of input, or `shutdown`) the worker exits with 0.

### How MageML runs it

`mage_ai/data_preparation/warm_workers.py` keeps a worker process per environment warm
between blocks, sending `end_marker` with each request; a process that exits mid-request
fails that request with its exit code (and signal), and a new one serves the next.

## Frames (stream sessions of Rust and R)

`crates/service/src/stream_rust.rs` defines it: the engine starts one resident program per
stream (`mage-blocks` with `MAGE_STREAM=1`, or `Rscript` with `mageml::serve_stream`), which
connects to a Unix socket (Rust) or to a port on 127.0.0.1 with a token (R) and says it is
ready. Each batch goes as a header frame and one frame per message value; the answer is a
header frame and the messages each block writes. A frame is a little-endian `u32` length
and its bytes. Keyed state travels in the headers (`state`, `state_updates`), and the
engine keeps it (`keyed_state.rs`). Its tests are the engine's stream tests
(`crates/service/tests/service.rs`, and the unit tests in `stream_rust.rs`); the version is
`mage::protocol::STREAM_PROTOCOL`.
