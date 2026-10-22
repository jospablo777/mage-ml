# Rust gateway in front of the web server

Status: pass-through, shadow reads and decisions for two routes, and serving them behind
a flag, done (2026-10-22); off by default, `MAGE_GATEWAY=on` turns it on.

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

The gateway also decides, for each shadowed read, whether it would serve it, and compares
that with the server's answer (a refusal is a 200 with an error whose code is 401 or 403;
its data is not compared). The caller is found as OAuthMiddleware finds it: the API key
(only MageML's own), then a bearer token or the session cookie, a JWT checked against
`JWT_SECRET`, and the access token's row and expiry. The read is allowed as
`has_at_least_viewer_role` allows it on the route's pipeline, or the project: the owner
flag, an owner role on the project, the older role bits, or any access through the user's
roles, whose permissions on a pipeline fall back to the project's and then the global ones.
MageML passes what this depends on (`MAGE_GATEWAY_REQUIRE_AUTHENTICATION`,
`MAGE_GATEWAY_CLIENT_ID`, `MAGE_GATEWAY_PROJECT_UUID`) in the gateway's environment. The
counts are `decisions_agree`, `decisions_disagree` and `decisions_failed`.

## Serving

`MAGE_GATEWAY_SERVE=block_runs,pipeline_run` (off by default) has the gateway answer those
routes itself when its own decision allows the read: from the metadata database, with the
server's CORS headers and an ETag (304 when it matches). A read it would refuse, or one it
cannot answer (the database busy, a run that does not exist), goes to the server, which
answers as it always has, refusals included. `/__gateway/shadow` counts the answers it gave
as `served`. Turn a route on where its shadow reads and decisions have stayed equal; the
policy matrix (`contracts/`) is a further check.

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
| `MAGE_GATEWAY_SERVE=block_runs,pipeline_run`: both routes through the gateway and from the server | The same answers; a caller without a session refused by the server; the block runs list's p95 6.8 ms from the server, 3.3 ms from the gateway (debug build) |
| Who may read `GET /api/block_runs`: the owner, a caller without a session, a project viewer kept out of one pipeline by a permission | Four decisions, all as the server's; ignoring a permission of no access makes the test fail |
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
