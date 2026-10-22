# Rust gateway in front of the web server

Status: step 1 done (2026-10-21), off by default; `MAGE_GATEWAY=on` turns it on.

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

## Measured

`integration_tests/gateway` (`make -C integration_tests test-gateway`) runs `mage start`
with the gateway and checks:

| Check | Result |
| --- | --- |
| Each GET of `contracts/openapi.json` without path parameters (74), plus `/`, `/pipelines`, signed in | The same status and body through the gateway as from the server |
| A terminal WebSocket, signed in | The shell answers a command through the gateway |
| `/api/status`, 300 requests on one connection | Under 1 ms added at p95 on a debug build of the engine; the budget is 2 ms |

## Next

Route families move into the gateway behind a flag each, starting with runs, block runs
and logs (ROADMAP P2), read from the same metadata database through `mage-ledger` and
compared with Tornado's answers before they take over.

## Found on the way

The parity test found that listing integration destinations imported every connector
into the web server, after which each new terminal's shell died of SIGSEGV between fork
and exec. The lists now read each connector's templates and README from its folder, and
a terminal whose shell exits as it starts says so (I-350).
