#!/bin/bash
# Installs the project's requirements.txt at container start. A failure is reported and the
# server starts anyway: stopping it left a restart loop, and requirements.txt is fixed in the
# editor the server serves. MAGE_STRICT_REQUIREMENTS=1 stops the start instead, for deploys
# that must not run with missing packages.
set -o pipefail

REQUIREMENTS_FILE="$1"
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    exit 0
fi
echo "$REQUIREMENTS_FILE exists."

dependency_options=()
if [[ -n "${MAGE_RUNTIME_CONSTRAINTS}" ]]; then
    dependency_options+=(--constraint "$MAGE_RUNTIME_CONSTRAINTS")
fi

if uv pip install --python "$(command -v python3)" \
        "${dependency_options[@]}" --requirement "$REQUIREMENTS_FILE" \
    && uv pip check --python "$(command -v python3)"; then
    echo "Requirements installed."
    exit 0
fi

echo "WARNING: installing $REQUIREMENTS_FILE failed (see the output above). Blocks that" \
    "import the missing packages fail until it is fixed." >&2
if [[ "${MAGE_STRICT_REQUIREMENTS}" == "1" ]]; then
    echo "MAGE_STRICT_REQUIREMENTS=1: not starting." >&2
    exit 1
fi
exit 0
