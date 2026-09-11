#!/bin/bash
set -eo pipefail

PROJECT_PATH="default_repo"
MAGE_PROJECT_TYPE="standalone"

if [[ ! -z "${FILESTORE_IP_ADDRESS}" && ! -z "${FILE_SHARE_NAME}" ]]; then
    echo "Mounting Cloud Filestore ${FILESTORE_IP_ADDRESS}:/${FILE_SHARE_NAME}"
    mount -o nolock "$FILESTORE_IP_ADDRESS:/$FILE_SHARE_NAME" /home/src
    echo "Mounting completed."
fi

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

REQUIREMENTS_FILE="${PROJECT_PATH}/requirements.txt"
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "$REQUIREMENTS_FILE exists."

    dependency_options=()
    if [[ -n "${MAGE_RUNTIME_CONSTRAINTS}" ]]; then
        dependency_options+=(--constraint "$MAGE_RUNTIME_CONSTRAINTS")
    fi
    uv pip install --python "$(command -v python3)" \
        "${dependency_options[@]}" --requirement "$REQUIREMENTS_FILE"
    uv pip check --python "$(command -v python3)"
fi

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
