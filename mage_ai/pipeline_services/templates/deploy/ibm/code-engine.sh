#!/usr/bin/env bash
# Deploy $name to IBM Cloud Code Engine from this folder: Code Engine builds the image
# from the Dockerfile and runs it. Needs the ibmcloud CLI with the code-engine plugin.
#
#   export REGION=us-south RESOURCE_GROUP=Default PROJECT=my-project
#   export ENV_SECRET=$name-env              # optional: a Code Engine secret with the variables
#   export TRUSTED_PROFILE=Profile-...       # optional: read IBM Secrets Manager without a key
#   export SECRETS='PGPASSWORD=ibm:<secret id>'  # optional, with TRUSTED_PROFILE
#   deploy/ibm/code-engine.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${REGION:?set REGION, such as us-south}"
: "${PROJECT:?set PROJECT, the Code Engine project}"
RESOURCE_GROUP="${RESOURCE_GROUP:-Default}"
APP="${APP:-$name}"

ibmcloud target -r "$REGION" -g "$RESOURCE_GROUP" >/dev/null
ibmcloud ce project select --name "$PROJECT"

args=(--name "$APP" --build-source . --strategy dockerfile --port 8080
      --min-scale 1 --max-scale 1 --env MAGE_SERVICE_HOST=0.0.0.0)
if [ -n "${ENV_SECRET:-}" ]; then
  args+=(--env-from-secret "$ENV_SECRET")
fi
if [ -n "${TRUSTED_PROFILE:-}" ]; then
  args+=(--trusted-profiles-enabled --env "MAGE_SERVICE_IBM_TRUSTED_PROFILE=$TRUSTED_PROFILE")
fi
if [ -n "${SECRETS:-}" ]; then
  args+=(--env "MAGE_SERVICE_SECRETS=$SECRETS")
fi
if [ -n "${SECRETS_MANAGER_URL:-}" ]; then
  args+=(--env "MAGE_SERVICE_IBM_SECRETS_MANAGER_URL=$SECRETS_MANAGER_URL")
fi

if ibmcloud ce app get --name "$APP" >/dev/null 2>&1; then
  ibmcloud ce app update "${args[@]}" --wait
else
  ibmcloud ce app create "${args[@]}" --wait
fi
echo "The service runs at $(ibmcloud ce app get --name "$APP" --output url)"
