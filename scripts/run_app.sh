#!/bin/bash
set -eo pipefail

PROJECT_PATH="default_repo"
MAGE_PROJECT_TYPE="standalone"

if [[ ! -z "${USER_CODE_PATH}" ]]; then
    PROJECT_PATH=$USER_CODE_PATH
fi

if [[ ! -z "${PROJECT_TYPE}" ]]; then
    MAGE_PROJECT_TYPE=$PROJECT_TYPE
fi

if [[ ! -z "${ULIMIT_NO_FILE}" ]]; then
    echo "Setting ulimit -n  to $ULIMIT_NO_FILE"
    ulimit -n "$ULIMIT_NO_FILE"
fi

"$(dirname "$0")/install_project_requirements.sh" "${PROJECT_PATH}/requirements.txt"

mage_args=()
if [[ ! -z "${PROJECT_UUID}" ]]; then
    mage_args+=( '--project-uuid' "$PROJECT_UUID" )
fi

if [[ ! -z "${CLUSTER_TYPE}" ]]; then
    mage_args+=( '--cluster-type' "$CLUSTER_TYPE" )
fi

if [ "$#" -gt 0 ]; then
    exec "$@"
else
    echo "Starting project at ${PROJECT_PATH}, project type ${MAGE_PROJECT_TYPE}"
    if [[ ! -z "${DBT_DOCS_INSTANCE}" ]]; then
        exec mage start "$PROJECT_PATH" --dbt-docs-instance 1
    elif [[ ! -z "${MANAGE_INSTANCE}" ]]; then
        exec mage start "$PROJECT_PATH" --manage-instance 1
    else
        exec mage start "$PROJECT_PATH" --project-type "$MAGE_PROJECT_TYPE" "${mage_args[@]}"
    fi
fi
