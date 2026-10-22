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

For the routes in `MAGE_GATEWAY_SHADOW` (default
`block_runs,logs,pipeline_run,pipeline_runs,pipeline_schedules`;
`off` turns them off), the
gateway still serves the server's answer, then computes its own from the metadata
database (SQLite, or PostgreSQL in UTC) and compares the two as JSON. The server's answer
for these routes is read whole before it is sent, and asked for uncompressed.
Runs move on while they are read, so when the two answers differ the gateway reads the
server's answer again and computes its own again, up to twice, 100 ms apart.
At most four comparisons run at once (`MAGE_GATEWAY_SHADOW_CONCURRENCY`); one past that
is left out and counted as `skipped`, so shadow reads never pile up under load.
`/__gateway/shadow`, from loopback only and not through a reverse proxy, gives the counts per route (equal, equal after a
reread, different, failed) and the last 20 differences with the first path where they
differ; each difference is also logged. The database URL reaches the gateway in its environment
(`MAGE_GATEWAY_METADATA_DB`), so `ps` does not show its password.

| Route | Gateway answer | Reproduces |
| --- | --- | --- |
| `GET /api/block_runs` | One query over block runs, runs and triggers, with `pipeline_run_id`, `pipeline_uuid`, `order_by`, `_limit`, `_offset` | The default order (started, created, completed, newest first), `metadata.count` and `next`, ISO times without a zero fraction, statuses stored by name and shown by value |
| `GET /api/pipeline_runs/<id>` | The run's presenter attributes and every column of its block runs, with the trigger's id and name; with `_format=with_basic_details` (the run page's poll), its id, status and execution date | `str(datetime)` times (a space, `+00:00` on PostgreSQL), JSON columns, the block runs sorted by creation, the `metadata: null` envelope |
| `GET /api/pipeline_runs` | The runs pages' queries: filters by pipeline, status, backfill and the project's triggers, an order, pages, time ranges, the pipelines' tags and types (from each `metadata.yaml`, kept until the file changes) and names. With `GET /api/pipeline_schedules/<id>/pipeline_runs` (a trigger's page), each date's runs grouped with their retries as the server groups them. Tag and type filters and `last_run_failed` are counted as `unsupported` and left to the server | Every LIST item's keys, `pipeline_tags` and `pipeline_type` null unless asked for, the block runs and their counts, the trigger's name, token and type, `metadata.pipeline_uuids` in directory order |
| `GET /api/pipelines/<uuid>/logs` | The logs page's reads of logs on the server's disk: the logs of the pipeline's runs and block runs, its blocks' editor logs, the filters by trigger, run, block run, block and time, a text search, and the lines before a window (`log_path`). Each log is read over its rotated segments with the reader column_atlas_native uses (the `mage-log-window` crate). Logs in S3 or GCS, and paths it would have to resolve against the server's working directory, are counted as `unsupported` | `posixpath.join` paths, `clean_name` of block uuids, `execution_partition`, the first 20 logs (200 with a search), the editor logs in directory order |
| `GET /api/pipeline_schedules`, `GET /api/pipelines/<uuid>/pipeline_schedules` | The triggers pages' queries: search, type, interval, status and tag filters, the orders, pages; each trigger's run counts, last run status, tags and event matchers, and its next run date. Cron expressions other than five fields of digits, `*`, `/`, `-` and `,` (with a day of the month or of the week, not both) are counted as `unsupported` | `next_execution_date` as croniter computes it (croner gives the same dates for those expressions), landing times and start times ahead, `HIDE_API_TRIGGER_TOKEN` |

The gateway also decides, for each shadowed read, whether it would serve it, and compares
that with the server's answer (a refusal is a 200 with an error whose code is 401 or 403;
its data is not compared). The caller is found as OAuthMiddleware finds it: the API key
(only MageML's own), then a bearer token or the session cookie, a JWT checked against
`JWT_SECRET`, and the access token's row and expiry. The read is allowed as
`has_at_least_viewer_role` allows it on the route's pipeline, or the project: the owner
flag, an owner role on the project, the older role bits, or any access through the user's
roles, whose permissions on a pipeline fall back to the project's and then the global ones.
The triggers list, and the runs list without a `pipeline_uuid`, are the project's, and the
server then authorizes each item on its own pipeline, refusing the whole list for one the
user may not view; the gateway leaves a user kept from any pipeline with triggers (or runs)
to the server.
MageML passes what this depends on (`MAGE_GATEWAY_REQUIRE_AUTHENTICATION`,
`MAGE_GATEWAY_CLIENT_ID`, `MAGE_GATEWAY_PROJECT_UUID`) in the gateway's environment. The
counts are `decisions_agree`, `decisions_disagree` and `decisions_failed`.

A query with a name a route does not read goes to the server: for a caller who is not an
owner the server refuses names its policy does not list, and meta names such as `_format`
change the answer. Tornado drops names with a blank value, and the gateway does too.

## The scheduler in shadow

The first step of moving the scheduler to Rust (ROADMAP P3) is to check its decisions. Every
15 s the gateway reads the project's active triggers and works out, as `should_schedule`
does, the execution date each should have a run for: not before its start time, not for an
execution date before the trigger was enabled unless `create_initial_pipeline_run` is set,
and none for after-run triggers, `@once` or `@always_on`. When the run with that execution
date appears it counts as `on_time`; when it has not appeared after the grace period
(`MAGE_GATEWAY_SCHEDULER_GRACE_SECONDS`, 120 s) it counts as `missing`, and the trigger and
date are logged and listed; an execution date with more than one run counts as a
`duplicate`. Triggers with a landing time (their runs start ahead by the runs'
average duration), with a backfill, or with a cron form the gateway does not read are left
out and counted as `unsupported_triggers`. The scheduler keeps its authority. The counts are
under `scheduler` in `/__gateway/shadow`; `MAGE_GATEWAY_SCHEDULER_SHADOW=off` turns it off.

## Serving

`MAGE_GATEWAY_SERVE=block_runs,logs,pipeline_run,pipeline_runs,pipeline_schedules` (off by
default) has the gateway answer those
routes itself when its own decision allows the read: from the metadata database, with the
server's CORS headers and an ETag (304 when it matches). A read it would refuse, or one it
cannot answer (the database busy, a run that does not exist), goes to the server, which
answers as it always has, refusals included. `/__gateway/shadow` counts the answers it gave
as `served`. One served answer in ten is also asked of the server in the background and
compared as a shadow read is, so a served route keeps being checked. Turn a route on where its shadow reads and decisions have stayed equal; the
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
| `MAGE_GATEWAY_SERVE=block_runs,pipeline_run,pipeline_runs`: the routes through the gateway and from the server | The same answers; a caller without a session refused by the server. p95 on a debug build of the engine: the block runs list 7 to 13 ms from the server, 1 to 1.5 ms from the gateway; the runs page's list 7 to 8 ms from the server, 1.6 to 2 ms from the gateway. On a release build: 6.6 and 0.7 ms for the block runs list, 5.8 and 0.8 ms for the runs page's list. The triggers page's list, debug build: 6.2 ms from the server, 1.3 to 1.7 ms from the gateway; a pipeline's logs, 10 ms and 2 ms |
| The runs list shadowed for eight query shapes, one pipeline tagged | All equal; a type filter counted as unsupported |
| The logs shadowed for ten queries of the logs page over two runs and an editor log: the default day, a limit, filters by run, trigger and block, a search with and without matches, the lines before a window | All equal; `_format` counted as unsupported |
| A trigger's runs and a pipeline's runs grouped with their retries, eight pages over four dates with retries, one past the last run | All equal; paging shows every run once |
| The scheduler in shadow, a trigger every minute | Its run found within a minute; none missing over the gateway suite |
| The triggers list shadowed for twelve query shapes over a landing-time trigger, a cron trigger starting in three days, a weekly and an API trigger | All equal; `_format=with_runtime_average` counted as unsupported |
| Who may read `GET /api/block_runs`: the owner, a caller without a session, a project viewer kept out of one pipeline by a permission | Four decisions, all as the server's; ignoring a permission of no access makes the test fail |
| The Playwright end-to-end suite against `mage start` with the gateway | 38 passed, 4 skipped, as without it: the editor runs blocks through kernel WebSockets, the terminal works, logs and outputs load |
| The same suite with `MAGE_GATEWAY_SERVE=block_runs,pipeline_run,pipeline_runs` | 38 passed, 4 skipped; the gateway answered 50 of the pages' reads (46 runs lists, the overview's included), none unsupported |
| The same suite with all five routes served | 38 passed, 4 skipped; the gateway answered 68 of the pages' reads (46 runs lists, 14 triggers lists, 4 logs, 3 block runs lists, a run), and the ten it also asked the server for were equal |

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
