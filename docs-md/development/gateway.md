# Rust gateway in front of the web server

Status: step 1 and shadow reads for block runs done (2026-10-21), off by default;
`MAGE_GATEWAY=on` turns it on.

The API moves to Rust one route family at a time behind a gateway (ADR-0007, ROADMAP P2).
Step 1 is the gateway alone: it answers nothing itself and passes every request to the
Tornado server, so the browser sees the same server.

## How it runs

With `MAGE_GATEWAY=on`, `mage start` binds the Tornado server to a free loopback port and
starts `mage-service gateway --listen <host>:<port> --upstream 127.0.0.1:<loopback port>
--exit-with-parent` on the port users open. The gateway is a subcommand of the engine that
`mage-ml[services]` installs, so there is no second binary. It exits when the server
process exits.

- Requests pass with their method, path, query, body and headers, Host included, so
  Tornado's WebSocket origin check and the URLs it builds are unchanged. Hop-by-hop headers
  stop at the gateway. `X-Real-Ip` is the client's address and `X-Forwarded-For` gains it;
  a client cannot claim another address through them.
- Bodies stream both ways without buffering: a log stream or a download starts arriving
  before the server finishes it.
- A WebSocket upgrade passes to the server; once it answers 101, the gateway copies bytes
  both ways, so kernels and terminals work without the gateway reading their frames.
- When the server does not answer, the gateway answers 502 with a short message.

## Shadow reads

For the routes in `MAGE_GATEWAY_SHADOW` (default `block_runs,pipeline_run`; `off` turns
them off), the
gateway still serves the server's answer, then computes its own from the metadata
database (SQLite, or PostgreSQL in UTC) and compares the two as JSON. The server's answer
for these routes is read whole before it is sent, and asked for uncompressed.
Runs move on while they are read, so when the two answers differ the gateway reads the
server's answer again and computes its own again, up to twice, 100 ms apart.
`/__gateway/shadow`, from loopback only, gives the counts per route (equal, equal after a
reread, different, failed) and the last 20 differences with the first path where they
differ; each difference is also logged. The database URL reaches the gateway in its environment
(`MAGE_GATEWAY_METADATA_DB`), so `ps` does not show its password.

| Route | Gateway answer | Reproduces |
| --- | --- | --- |
| `GET /api/block_runs` | One query over block runs, runs and triggers, with `pipeline_run_id`, `pipeline_uuid`, `order_by`, `_limit`, `_offset` | The default order (started, created, completed, newest first), `metadata.count` and `next`, ISO times without a zero fraction, statuses stored by name and shown by value |
| `GET /api/pipeline_runs/<id>` | The run's presenter attributes and every column of its block runs, with the trigger's id and name | `str(datetime)` times (a space, `+00:00` on PostgreSQL), JSON columns, the block runs sorted by creation, the `metadata: null` envelope |

A route starts being served by the gateway only after its shadow reads stay equal; its
authorization moves with it, with the policy matrix (`contracts/`) as the check.

## Measured

`integration_tests/gateway` (`make -C integration_tests test-gateway`) runs `mage start`
with the gateway and checks:

| Check | Result |
| --- | --- |
| Each GET of `contracts/openapi.json` without path parameters (74), plus `/`, `/pipelines`, signed in | The same status and body through the gateway as from the server |
| A terminal WebSocket, signed in | The shell answers a command through the gateway |
| `/api/status`, 300 requests on one connection | Under 1 ms added at p95 on a debug build of the engine; the budget is 2 ms |
| `GET /api/block_runs` for ten combinations of filters, order and pages over eight block runs | All ten shadow reads equal; a status written as stored instead of shown makes the test fail |
| `GET /api/pipeline_runs/<id>` for four runs while the scheduler runs them | All equal, some after a reread |
| The Playwright end-to-end suite against `mage start` with the gateway | 38 passed, 4 skipped, as without it: the editor runs blocks through kernel WebSockets, the terminal works, logs and outputs load |

## Next

Route families move into the gateway behind a flag each, starting with runs, block runs
and logs (ROADMAP P2), read from the same metadata database through `mage-ledger` and
compared with Tornado's answers before they take over.

## Found on the way

Block runs created together share their times, and the block runs list ordered only by
times, so pages repeated or skipped rows between requests; the id now breaks ties, newest
first (I-354).

The parity test found that listing integration destinations imported every connector
into the web server, after which each new terminal's shell died of SIGSEGV between fork
and exec. The lists now read each connector's templates and README from its folder, and
a terminal whose shell exits as it starts says so (I-350).
