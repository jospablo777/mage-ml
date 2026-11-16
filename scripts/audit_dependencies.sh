#!/usr/bin/env bash
# Known vulnerabilities in MageML's dependencies, as the Dependency audit workflow checks
# them; the workflow runs this script, so the accepted advisories are listed here once.
#
#   scripts/audit_dependencies.sh            every check
#   scripts/audit_dependencies.sh python     the root lock: pip-audit and uv audit (OSV)
#   scripts/audit_dependencies.sh runtimes   each runtimes/* helper lock
#   scripts/audit_dependencies.sh rust       cargo audit on every Cargo.lock
#   scripts/audit_dependencies.sh frontend   yarn audit (fails only on fixable advisories)
#
# Needs uv (with the dev group synced for pip-audit), cargo-audit and yarn. Exits non-zero
# when any check finds a vulnerability.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
status=0

python_audit() {
  echo '== Python: the root lock, every extra'
  local requirements
  requirements="$(mktemp)"
  uv export --locked --extra all --extra integrations --no-default-groups \
    --no-hashes --no-emit-workspace --output-file "$requirements" >/dev/null || status=1
  uv run --no-sync pip-audit --disable-pip --no-deps --requirement "$requirements" || status=1
  rm -f "$requirements"
  # Accepted while no fixed release exists (the audit fails again once one does; a GHSA
  # ID also covers its PYSEC alias): chromadb, only in the opt-in chroma extra. The
  # advisories are about Chroma's HTTP server and its authorization providers; MageML runs
  # Chroma embedded and starts no server.
  # Ignored: PYSEC-2023-235 is about Couchbase Server 7.2.0 (a key in its debug.log), filed
  # against the Python SDK with no version bound.
  uv audit --frozen --preview-features audit-command \
    --ignore-until-fixed GHSA-f4j7-r4q5-qw2c --ignore-until-fixed GHSA-36p7-vc44-83pf \
    --ignore-until-fixed GHSA-2wm9-hf6c-p5cr --ignore-until-fixed GHSA-xph7-9rjv-w5fr \
    --ignore PYSEC-2023-235 || status=1
}

runtime_audit() {
  local runtime="$1" requirements
  echo "== Python: runtimes/$runtime"
  requirements="$(mktemp)"
  uv export --project "runtimes/$runtime" --locked --no-default-groups --no-hashes \
    --output-file "$requirements" >/dev/null || status=1
  uv run --no-sync pip-audit --disable-pip --no-deps --requirement "$requirements" || status=1
  rm -f "$requirements"
  uv audit --frozen --preview-features audit-command --project "runtimes/$runtime" || status=1
}

runtimes_audit() {
  local runtime
  for runtime in runtimes/*/; do
    [ -f "$runtime/uv.lock" ] && runtime_audit "$(basename "$runtime")"
  done
}

rust_audit() {
  # Accepted until Polars (0.55.2, the newest) updates them: quick-xml through
  # object_store, only reachable from a cloud storage server that sends crafted XML
  # (RUSTSEC-2026-0194, RUSTSEC-2026-0195); bincode unmaintained (RUSTSEC-2025-0141).
  # rustls-pemfile unmaintained (RUSTSEC-2025-0134), no known vulnerability: the mysql
  # crate's TLS needs it to read MYSQL_SSL_CA; the SDK's mysql feature only.
  # webpki-roots 0.26.11 (RUSTSEC-2026-0381) has no fixed 0.26 release: it re-exports
  # webpki-roots 1.x, which is 1.0.10, the fixed roots (async-nats > tokio-websockets).
  local lock
  for lock in $(git ls-files '*Cargo.lock'); do
    echo "== Rust: $lock"
    cargo audit --file "$lock" --deny warnings \
      --ignore RUSTSEC-2026-0194 --ignore RUSTSEC-2026-0195 \
      --ignore RUSTSEC-2025-0141 --ignore RUSTSEC-2025-0134 \
      --ignore RUSTSEC-2026-0381 || status=1
  done
}

frontend_audit() {
  echo '== Frontend'
  (
    cd mage_ai/frontend
    yarn install --frozen-lockfile --ignore-scripts --non-interactive >/dev/null
    set +o pipefail
    yarn audit --json | python3 ../../scripts/yarn_audit.py
  ) || status=1
}

case "${1:-all}" in
  python) python_audit ;;
  runtimes) runtimes_audit ;;
  runtime) runtime_audit "$2" ;;
  rust) rust_audit ;;
  frontend) frontend_audit ;;
  all) python_audit; runtimes_audit; rust_audit; frontend_audit ;;
  *) echo "Unknown check: $1 (python, runtimes, runtime <name>, rust, frontend)"; exit 2 ;;
esac

if [ "$status" -ne 0 ]; then
  echo 'Vulnerabilities found: see above.'
fi
exit "$status"
