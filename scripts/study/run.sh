#!/bin/bash
# Runs the compilation study's tests (integration_tests/topologies) inside a systemd user
# scope that caps their memory and CPU: a runaway case is killed in its own cgroup instead
# of exhausting the machine. Arguments go to pytest, such as a file or -k.
#
#   MAGE_STUDY_FULL=1 MAGE_STUDY_RESULTS_DIR=integration_tests/topologies/results \
#     scripts/study/run.sh integration_tests/topologies/test_scaling.py
#
# Each session appends to <experiment>-<session>.jsonl and writes <session>.meta.json
# (integration_tests/topologies/provenance.py).
#
# MAGE_STUDY_MEMORY_MAX (20G) and MAGE_STUDY_CPU_QUOTA (1600%, 16 threads) set the caps.
set -euo pipefail
cd "$(dirname "$0")/../.."
MEMORY=${MAGE_STUDY_MEMORY_MAX:-20G}
CPU=${MAGE_STUDY_CPU_QUOTA:-1600%}
export MAGE_TEST_R_PROJECT_DIR=${MAGE_TEST_R_PROJECT_DIR:-$PWD/integration_tests/r_env}
if command -v systemd-run > /dev/null; then
  exec systemd-run --user --scope --quiet -p MemoryMax="$MEMORY" -p MemorySwapMax=0 \
    -p CPUQuota="$CPU" -- uv run --no-sync python -m pytest -p no:xdist -p no:cacheprovider \
    -rs "${@:-integration_tests/topologies}"
fi
echo "systemd-run is missing: running without the memory and CPU caps." >&2
exec uv run --no-sync python -m pytest -p no:xdist -p no:cacheprovider -rs \
  "${@:-integration_tests/topologies}"
