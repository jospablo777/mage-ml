# Project metadata.yaml

*Reference for configuring project-level settings in metadata.yaml.*

## Overview

Each MageML project has a `metadata.yaml` file at the root of the project
directory. The file controls storage locations, feature flags, compute
configuration, and defaults that apply to all pipelines in the project.

```
your_project/
├─ pipelines/
├─ .../
├─ io_config.yaml
├─ metadata.yaml   <-- project metadata lives here
├─ requirements.txt
```

Use the fields below to customize how your project behaves.

## Sample `metadata.yaml`

```yaml
project_type: standalone
variables_dir: ~/.mage_data
remote_variables_dir: s3://bucket/path_prefix

notification_config:
  alert_on:
    - trigger_failure
  slack_config:
    webhook_url: "{{ env_var('MAGE_SLACK_WEBHOOK_URL') }}"

pipelines:
  settings:
    triggers:
      save_in_code_automatically: true

project_uuid: 123456789abcdefg
```

## Top-level fields

**`project_type`** (string enum)

Project layout. Common options: `standalone` (default), `main`, or `sub`.

**`cluster_type`** (string)

Optional cluster type used by the project (e.g., `k8s`, `docker`). Used by workspace
management (`mage start --manage-instance 1`).

**`project_uuid`** (string)

Unique identifier for the project. Generated when the project is created.

**`variables_dir`** (string)

Local path for storing pipeline variables and outputs. Paths are relative to
the project root unless an absolute path is provided. Default to `/home/src/mage_data`.

**`remote_variables_dir`** (string)

Remote path (e.g., `s3://bucket/prefix`) for storing variables in object
storage instead of the local filesystem.

**`variables_retention_period`** (string)

How long variables are retained (e.g., `90d`). `mage clean-cached-variables` deletes older
ones; see [Variable retention](../../design/data-pipeline-management.md#variable-retention).

**`workspace_initial_metadata`** (object)

Default project metadata applied to new workspaces created from this project.

**`workspace_config_defaults`** (object)

Default workspace configuration applied across environments (e.g., k8s
defaults when running on Kubernetes).

**`features`** (object)

Feature flags for the project (e.g., `command_center`, `dbt_v2`,
`automatic_kernel_cleanup`). Keys are booleans.

**`pipelines.settings.triggers.save_in_code_automatically`** (boolean)

Whether new or updated triggers are automatically written to code.

**`settings_backend`** (object)

Read settings from a backend such as AWS Secrets Manager before environment variables.
See [settings backend](../settings-backend.md).

## Compute and execution

**`emr_config`** (object)

Amazon EMR cluster settings (instance types, security groups, key pair, etc.).

**`spark_config`** (object)

Spark configuration shared across pipelines (e.g., `spark_master`,
`executor_env`, `spark_jars`, `use_custom_session`).

**`ecs_config`** (object)

Project-level defaults for AWS ECS execution.

**`gcp_cloud_run_config`** (object)

Project-level defaults for GCP Cloud Run execution.

**`azure_container_instance_config`** (object)

Project-level defaults for Azure Container Instances execution.

**`k8s_executor_config`** (object)

Kubernetes executor defaults applied to pipelines and blocks.

**`queue_config`** (object)

Configuration for queueing block runs, e.g. `concurrency`, the maximum number of
concurrent block runs (default `20`).

## Observability and safety

**`notification_config`** (object)

Project-level alerting configuration (alert types, Slack, Teams, Discord, Telegram,
Google Chat, Opsgenie and email). You can edit this in `metadata.yaml`.
Pipeline-level alert settings override this project-level configuration
for that pipeline.

**`logging_config`** (object)

Configure log destinations and formats for the project.

**`retry_config`** (object)

Default automatic retry behavior applied to block runs at the project level.
MageML merges retry settings from least specific to most specific: project
defaults, then pipeline-level `retry_config`, then block-level `retry_config`.
For the same retry field, the most specific configured value wins. Blank or
`null` retry fields are ignored during this merge, so a blank pipeline or
block field inherits the next available value from the less-specific config
instead of clearing it.

```yaml
retry_config:
  retries: 3
  delay: 5
  max_delay: 60
  exponential_backoff: true
```

- `retries`: number of retry attempts after a block run fails (default `0`, no
  retries).
- `delay`: seconds to wait before the next retry attempt (default `5`).
- `max_delay`: maximum accumulated retry delay in seconds; retries stop once the next
  delay would reach it (default `60`). This cap applies whether or not exponential
  backoff is enabled.
- `exponential_backoff`: whether to double the delay between retry attempts
  (default `true`).

**`ldap_config`** (object)

Optional LDAP connection settings for authentication.
