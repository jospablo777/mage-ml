# Pipeline services: run pipelines without Mage

*Export pipelines as a standalone Docker service with an HTTP API, schedules, run history, logs, metrics, secrets and embedded ML models.*

A pipeline service is a Docker image that runs one pipeline, or several, without Mage: no
web server, no scheduler, no metadata database. It starts in under a second and gives you:

- an HTTP API to start runs and read their outcome, block by block;
- the pipelines' schedules;
- a run history that survives restarts, and every line the blocks print;
- Prometheus metrics and a health check;
- retries, timeouts and cancellation, as configured in Mage;
- secrets from the environment, files, IBM Cloud Secrets Manager, AWS Secrets Manager and
  Parameter Store;
- ML models from MLflow, embedded in the image.

You build pipelines in Mage as always, then ship them where they need to run: next to an
application, in a Kubernetes cluster, on IBM Cloud Code Engine or AWS ECS.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../../docs/images/diagrams/pipeline-service.dark.svg">
  <img src="../../docs/images/diagrams/pipeline-service.light.svg" alt="Pipelines built in MageML are exported to a build context, built into a Docker image and run by the service" />
</picture>

## 1. Export

From the app: right-click a pipeline in the pipelines list, or open **File** in the editor,
and choose **Export as service**; or open **Services** in the left menu to save a service
and check its configuration first (see [Services in the app](services-page.md)).
From a terminal:

```bash
mage export service path/to/project sales_pipeline --out ./sales-service
```

The command reads the project and writes a Docker build context to `./sales-service`. It
prints a report: the blocks, the triggers, the project files it copied, the Python packages
the image needs and the environment variables the code reads. Several pipelines go in one
service by listing them:

```bash
mage export service path/to/project load_orders score_orders --out ./orders-service --name orders
```

A service saved on the Services page or with `mage service new`, in
`services/<name>/service.yaml`, exports by name, with its after-run triggers and settings
([Services page](services-page.md)):

```bash
mage export service path/to/project --service shop-live --out ./shop-live
```

`--build` also builds the image. `--platform linux/amd64,linux/arm64` builds for other
chips with `docker buildx`, and `--push` pushes it to the registry its `--tag` names (log in
first; several platforms need `--push`). An image with IBM MQ's client or Db2's (`ibm_db`)
builds for `linux/amd64` only, since IBM publishes them for nothing else; the export's notes
say so, and its manifest, README and GitHub workflow name that platform alone:

```bash
mage export service path/to/project --service shop-live --out ./shop-live \
  --build --platform linux/amd64,linux/arm64 --push --tag icr.io/ml-team/shop-live:1.0
```

What the export takes:

| From the project | In the service |
| --- | --- |
| The blocks of the selected pipelines | Their files, unchanged |
| Modules the blocks import (`from my_project.utils.x import y`) | Those modules, found by following imports |
| `io_config.yaml`, `requirements.txt` | Copied; values are never resolved at export |
| Retries and timeouts of blocks, pipelines and the project | Applied the same way |
| Triggers in `triggers.yaml` and in Mage's database | Schedules and API triggers |
| Python packages | Only those the blocks' imports load, pinned to your Mage environment's versions |
| MLflow models the blocks load | Their files and metadata (see [Models](#models)) |

Python, [R](../guides/blocks/r-blocks.md), [Rust](../guides/blocks/rust-blocks.md) and
[SQL](../guides/blocks/sql-blocks.md) blocks are supported.

- **Rust** blocks are compiled when the image builds, into one binary for all of them
  (`bin/mage-blocks`), so the service runs native code and needs no Rust installation. One
  binary instead of one per block keeps the image small (Polars is linked once, about
  75 MB) and loads once per run. The export brings `rust/Cargo.lock` up to date for the
  exported blocks and keeps the locked versions, so the image builds the crates you ran.
  A Rust block that connects with its io_config profile (`mage::postgres`) gets it from
  the service, which fills in `env_var()` and `mage_secret_var()` from its environment
  for each run; set those variables as for Python blocks.
- **R** blocks run with Mage's R runner and the project's rv environment: the image
  installs R 4.6 and the packages `rv.lock` pins, so the export needs that lock file
  (`mage r init`, then `mage r sync`). Packages install from binaries where Posit has them,
  as on amd64; on arm64 some build from source, which makes the first build slow. An
  image with R blocks is based on Debian trixie and is larger.

- **SQL** blocks on PostgreSQL, MySQL, DuckDB, ClickHouse, Trino, BigQuery, Snowflake,
  Redshift and Db2 run as in a Mage pipeline
  run: upstream outputs that the
  query reads as `{{ df_1 }}` are written to tables first (the table of an upstream SQL
  block on the same database is used directly), the query is rendered with the run's
  variables, `variables()`, `env_var()` and `mage_secret_var()`, and the result is created
  as the block's table by its export write policy, or the statements run as they are with
  raw SQL. The integration tests compare every table these pipelines leave, column by
  column, with Mage's. No BigQuery, Snowflake or Redshift runs in the tests: their blocks are
  compared with Mage's over stand-ins for the clients on DuckDB, request by request and row
  by row.

Blocks the service cannot run stop the export with the block named and the reason, so
nothing silently disappears: sensors, dynamic blocks, callbacks, conditional blocks written
in R or Rust, SQL blocks on other databases (SQL Server, Druid, Pinot and others), SQL blocks that use `block_output()`, and blocks that read their upstream with
chunk or batch settings are not supported yet. Chart, markdown and scratchpad blocks are
notebook only and are left out.

A block's [data contract](../guides/blocks/data-contracts.md) goes into the service, and the
engine checks the block's output against it after every run, with the checker MageML uses:
`fail` fails the block without retries, `warn` logs the broken rules. The report is kept
next to the output as `contract.json`. A contract the block pins to another version stops
the export. A block's [monitors](../guides/pipelines/monitoring.md) run after it the same way,
compared with the service's earlier runs; their results are in the run log and `/metrics`
([details](../guides/pipelines/monitoring.md#in-exported-services)).

## 2. Build and run

```bash
cd sales-service
docker build -t sales-service .
docker run --rm -p 8080:8080 \
  -e MAGE_SERVICE_TOKEN=change-me \
  --env-file .env \
  -v sales-service-data:/var/lib/mage-service \
  sales-service
```

`.env.example` lists every variable the code reads; copy it to `.env` and fill it in.
`docker compose up --build` does the same with `compose.yaml`.

To run a pipeline once and exit, for a cron job, a Kubernetes Job or a CI step:

```bash
docker run --rm --env-file .env sales-service run sales_pipeline --var day=2026-10-09
```

The exit code is 0 when every block completes and 1 otherwise, and the run prints a table
of its blocks with their durations and errors on stdout, or the run as JSON with `--json`;
the logs go to stderr, so the output can be piped.

## 3. Call it

Every call needs the token: `Authorization: Bearer <token>`. A service that other hosts can
reach does not start without `MAGE_SERVICE_TOKEN`; one listening only on 127.0.0.1 makes a
token and writes it to `service-token` in its data directory, readable only by its user. The
token never appears in the log.

```bash
# The token you started the service with, such as one from: openssl rand -hex 32
export TOKEN=...
# Start a run and wait up to 10 minutes for its outcome:
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "http://localhost:8080/v1/pipelines/sales_pipeline/runs?wait=600" \
  -d '{"variables": {"day": "2026-10-09"}, "idempotency_key": "sales-2026-10-09"}'
```

The response holds the run's status and each block's status, attempts, duration, error and
outputs. With an `idempotency_key`, sending the same request again returns the first run
instead of starting another, so a client can retry safely.

With `&output=<block>`, the response also carries that block's output once the run
completes: a list or dict as it is, and a DataFrame of up to 8 MB as JSON rows, whether a
Python, R or Rust block made it. A larger frame comes as `output_url`. This is how a
service answers [inference requests](../guides/pipelines/model-inference.md).

| Endpoint | |
| --- | --- |
| `POST /v1/pipelines/{id}/runs` | Start a run; `?wait=SECONDS` returns when it ends, `&output=BLOCK` with that block's output |
| `GET /v1/runs?pipeline=&status=&limit=` | Runs, newest first |
| `GET /v1/runs/{id}` | One run with its blocks |
| `GET /v1/runs/{id}/logs?block=&tail=&format=text` | Everything the blocks printed |
| `GET /v1/runs/{id}/output?block=&index=&format=json` | A block's output: JSON, an Arrow IPC file, Parquet or NumPy; `format=json` gives a frame as JSON rows |
| `POST /v1/runs/{id}/cancel` | Cancel a queued or running run |
| `GET /v1/pipelines` | The pipelines, their blocks and triggers |
| `GET /v1/models` | Embedded models and their MLflow metadata |
| `GET /v1/snapshot` | Counts, durations, triggers and monitor results per pipeline |
| `POST /v1/control` | Pause, resume, change a pipeline's concurrency |
| `POST /v1/streams/{pipeline}/messages` | Messages for a stream with a webhook source; answered once its sinks took them ([webhooks](stream-services.md#webhooks)) |
| `GET /v1/streams` | Each stream's state, message counts, acknowledged positions, lag per partition and last error |
| `GET /v1/streams/{pipeline}` | One stream's status, with the last minute of rates and lag, and each block's messages in and out, batches, errors and p50 and p95 time per batch |
| `GET /metrics` | Prometheus metrics, including `mage_service_stream_lag_messages` per stream and partition (scale on it); no token needed |
| `GET /healthz` | Health; no token needed |

`MAGE_SERVICE_READ_TOKEN` sets a second token that can read runs, logs and the snapshot but
cannot start, cancel or change anything; give it to dashboards and monitoring.

## Watch it from a terminal

`mage-console` is a terminal dashboard for a running service, included in every image:

```bash
docker exec -it sales-service mage-console
```

Per pipeline it shows succeeded, failed, running and queued runs, run durations with their
p95, throughput, the recent runs with their errors and the embedded models. It is
read-only unless started with `--allow-control`, which enables pause, resume and
concurrency changes checked against the pipeline's settings revision. See
[Service console](service-console.md) for the panels, keys and options.

## Schedules

The service carries the pipelines' time triggers, but it runs them only with
`MAGE_SERVICE_SCHEDULES=on`. That way a service built next to a running Mage does not run
the same schedule twice; turn it on when the service takes over. Its log says so at start,
and the snapshot shows each trigger's next run.

Each occurrence runs once, even across restarts, and one service owns the schedules at a
time when several share a data volume. Occurrences missed while the service was stopped
are not run; `skip_if_previous_running` is honored.

## After-run triggers

A pipeline can start when a run of another pipeline ends: features, then scoring; a load,
then its checks. These are MageML triggers of type `after_run`, made on the pipeline's
Triggers page (**After another pipeline**), on the Services page, or in `triggers.yaml`:

```yaml
triggers:
- name: after_live_traffic
  schedule_type: after_run
  status: active
  settings:
    after_pipeline: live_traffic
    after_status: completed   # completed (the default), failed or any
```

MageML runs them (see [Triggers](../orchestration/triggers/triggering-pipelines.md#after-run-triggers)),
and a service runs them for the pipelines it holds:

- The run starts as soon as the earlier run ends, with the trigger's variables. Its source
  is `after:<pipeline>`, so the run history shows what started it.
- Each earlier run starts one run, also when the service restarts while it runs.
- They work with `MAGE_SERVICE_SCHEDULES` off: they follow runs of the service, however
  those started.
- `skip_if_previous_running: true` in the settings skips the run while the pipeline still
  has one queued or running.
- A trigger that follows a pipeline left out of the service is left out, with a note.
  Triggers that start each other in a loop stop the export; MageML refuses them too.

## Configuration and secrets

The export lists the environment variables the code reads, in `io_config.yaml`
(`env_var('X')`) and in the blocks (`os.getenv`, `os.environ`, `mage_secret_var`). Every
variable of `io_config.yaml` is marked secret: hosts, users, databases and ports are
connection settings as much as passwords are. Those of a profile the exported code names,
such as `ConfigFileLoader(path, 'warehouse')` or a SQL block's profile, are required;
those of profiles no exported block uses are listed as optional, with a note. Elsewhere,
names that look secret (`PASSWORD`, `TOKEN`, `KEY`, `SECRET`) are marked secret, and
`os.environ['X']` is required.

`io_config.yaml` goes into the image as it is, with its `env_var` templates, and resolves
at run time from the container's environment, as in Mage. A CI/CD pipeline that injects
`POSTGRES_HOST_WAREHOUSE` into the container gives a block that loads the `warehouse` profile
its `POSTGRES_HOST`, with no change to the project. The generated `deploy/kubernetes.yaml`
lists the variables the container needs and sets `MAGE_SERVICE_STRICT_ENV=on`, so a pod
missing a required one stops at start with their names instead of failing on its first
connection. `mage-service env` shows which are set:

```bash
docker run --rm --env-file .env sales-service env
```

The service takes values from several places; a variable set directly always wins.

| Source | How |
| --- | --- |
| Environment | `-e NAME=value`, `--env-file`, Kubernetes `secretKeyRef`, ECS `secrets`, Code Engine `--env-from-secret` |
| A secret file | `NAME_FILE=/run/secrets/name` sets `NAME` from the file, for the variables the code reads, `MAGE_SECRET_*`, the service tokens and the cloud credentials; other `*_FILE` variables such as `SSL_CERT_FILE` are left to their tools. Files over 128 KiB are refused |
| A directory of secret files | `MAGE_SERVICE_SECRETS_DIR=/run/secrets`: one variable per file |
| A secrets provider | `MAGE_SERVICE_SECRETS`, read at start (below) |

`MAGE_SERVICE_SECRETS` lists secrets to fetch, one per comma or line:
`[NAME=]provider:reference[#field]`.

```bash
MAGE_SERVICE_SECRETS='PGPASSWORD=ibm:3f1c...e2, PGUSER=ibm:3f1c...e2#username, API_KEY=file:/run/keys/api'
MAGE_SERVICE_SECRETS='aws:prod/db, API_KEY=aws-ssm:/scoring/api-key'
```

| Provider | Reference | Settings |
| --- | --- | --- |
| `aws` | An AWS Secrets Manager secret name or ARN | `AWS_REGION` (or the ARN's region) and AWS credentials (below) |
| `aws-ssm` | An AWS Systems Manager parameter name, such as `/app/key` | The same; SecureString parameters are decrypted |
| `azure` | An Azure Key Vault secret name, or `https://<vault>.vault.azure.net/secrets/<name>[/<version>]` | `AZURE_KEY_VAULT_URL` and Azure credentials (below) |
| `gcp` | A Google Cloud Secret Manager secret, or `projects/<p>/secrets/<s>[/versions/<v>]` | `GOOGLE_CLOUD_PROJECT` (or the credentials' project) and Google credentials (below) |
| `ibm` | An IBM Cloud Secrets Manager secret id | `MAGE_SERVICE_IBM_SECRETS_MANAGER_URL`, and `IBM_CLOUD_API_KEY` or a Code Engine trusted profile (`MAGE_SERVICE_IBM_TRUSTED_PROFILE`) |
| `file` | A path | none |

The secret functions of `io_config.yaml`, `aws_secret_var`, `azure_secret_var`,
`gcp_secret_var` and `ibm_secret_var`, need no list: the export writes each secret they read
into the manifest, and the service fetches it at start-up with the provider's settings and
credentials. A variable set in the environment, or mapped in `MAGE_SERVICE_SECRETS`, wins.

An IBM arbitrary secret gives its payload, a username and password secret its password
(`#username` for the user), an IAM credentials secret its API key. A key-value secret
listed without a name sets one variable per key.

An AWS secret gives its string. `#key` picks one key of a JSON secret, such as the
`username` of an RDS secret, and a JSON secret listed without a name sets one variable per
key. The service finds AWS credentials as the AWS SDKs do, so on AWS no key is stored:

1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`;
2. EKS IAM roles for service accounts (`AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`);
3. ECS task roles and EKS Pod Identity (the container credentials endpoint);
4. the EC2 instance profile.

The role needs `secretsmanager:GetSecretValue` or `ssm:GetParameter` on the secrets it
reads, and `kms:Decrypt` when a customer managed key encrypts them.
`AWS_ENDPOINT_URL_SECRETS_MANAGER`, `AWS_ENDPOINT_URL_SSM` and `AWS_ENDPOINT_URL` point the
requests at a VPC endpoint.

An Azure secret gives its value; `#key` picks one key of a JSON value. Credentials come as
the Azure SDKs' `DefaultAzureCredential` finds them: a client secret (`AZURE_TENANT_ID`,
`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`), an AKS workload identity
(`AZURE_FEDERATED_TOKEN_FILE`), or a managed identity (App Service, Container Apps, VMs).
The identity needs the Key Vault Secrets User role.

A Google Cloud secret gives its latest version's payload; `#key` picks one key of a JSON
payload. Credentials come as Google's application default credentials: a service account key
or authorized user file in `GOOGLE_APPLICATION_CREDENTIALS`, or the metadata server on GKE,
Cloud Run and Compute Engine. The account needs the Secret Manager Secret Accessor role.

Platforms that inject secrets themselves need no provider: ECS task definition `secrets`,
Kubernetes `secretKeyRef` and the Secrets Store CSI driver (with `MAGE_SERVICE_SECRETS_DIR`)
set the variables before the service starts.

Secret values, and every value read from a file or a provider, are replaced by `***` in
logs and stored errors. Mage secrets read with `mage_secret_var('name')` come from
`MAGE_SECRET_<NAME>`. With `MAGE_SERVICE_STRICT_ENV=on`, the service refuses to start while
a variable the code requires is missing.

## Models

A service makes an ML service out of a pipeline. The pipeline can consume raw data, turn
it into features and pass them straight to the model for quick inference, or read the
features from a [feature store](#feature-stores-and-apis) you already run. You deploy it
like any other service and monitor it with its runs, logs and metrics.

A block that loads its model with Mage's helper runs the same code in Mage and in the
service:

```python
from mage_ai.pipeline_services.models import load_model

@transformer
def score(orders, *args, **kwargs):
    model = load_model('churn', uri='models:/churn@champion')
    return orders.assign(churn=model.predict(orders))
```

In Mage, `load_model` downloads the model from MLflow (`MLFLOW_TRACKING_URI`) once per
process. At export, Mage finds these calls, downloads each model with its metadata (registry
version, run, params, metrics, flavors, signature) and puts it in the image, with the
model's Python requirements. In the service the same call loads the embedded copy: the
service never calls MLflow and always runs the version it was built with. `model_path()`
returns the model's files, for frameworks you load yourself; `model_metadata()` returns its
metadata, which `GET /v1/models` also serves.

Models loaded another way can be embedded with `--model`:

```bash
mage export service project scoring --out ./scoring --model churn=models:/churn/7
```

### Choose the models of a saved service

A saved service names its models and the registry they come from, so every export embeds
the same ones. The Services page sets them under **Models in the image**; the file holds:

```yaml
services:
  churn-scoring:
    pipelines: [daily_features, churn_scoring]
    model_registry: http://mlflow.ml.svc:5000
    models:
      - name: churn                                   # the name blocks load it by
        uri: models:/churn-classifier@champion
```

- `uri` is a registered model with an alias (`models:/name@champion`) or a version
  (`models:/name/10`), or any URI MLflow can download: `runs:/<run id>/model`, `s3://`,
  `gs://`, `https://`.
- The check resolves each alias while you edit: the version it points to, its run, its
  tags (such as `validated`) and its Feast feature service. A model the registry lacks
  is named with the reason, and the export stays off.
- The export downloads each model into the image and reports it: "The model churn was
  embedded", with its version and size; or the reason it could not be downloaded.
- `model_registry` applies to the export only. Without it, the server's
  `MLFLOW_TRACKING_URI` is used. Credentials, such as `MLFLOW_TRACKING_TOKEN` or
  `MLFLOW_TRACKING_USERNAME` and `MLFLOW_TRACKING_PASSWORD`, come from the server's
  environment and never go in the image. From the CLI, `MLFLOW_TRACKING_URI` in the
  environment wins over the saved one, so CI can reach the registry its own way.

### Load a model in its own format

`load_model(name)` gives MLflow's pyfunc model, which has `predict`. `flavor='native'`
gives the framework's own object, such as a `CatBoostClassifier` with `predict_proba`:

```python
from mage_ai.pipeline_services.models import feature_service, load_model, model_metadata

URI = 'models:/churn-classifier@champion'

model = load_model('churn', uri=URI, flavor='native')
info = model_metadata('churn', uri=URI)   # version, aliases, run, tags, params, metrics
```

A model logged as a plain file, such as `churn.cbm` with `mlflow.log_artifact`, has no
MLflow flavor: `flavor='native'` reads the file by its suffix. A `.cbm` file loads as a
`CatBoostClassifier`, `CatBoostRegressor` or `CatBoostRanker`, from its loss function.
The same holds for `.onnx`, `.ubj`, `.skops`, `.joblib` and the other formats of
`load_artifact`. `flavor='catboost'` or `'sklearn'` names a flavor yourself.

Resolve the alias once per run and use the version for the model, the metadata and the
provenance you write. The embedded copy does this by itself: an image holds one version,
so a run cannot mix two.

### Champion and challengers

A model can have challengers: other versions the service scores in the shadow, on a sample
of the inputs, so you see how a candidate would do on live traffic before you promote it.
The callers only ever get the champion's output.

```yaml
    models:
      - name: churn
        uri: models:/churn-classifier@champion
        challengers:
          - name: v4
            uri: models:/churn-classifier/4
        shadow:
          sample: 0.25      # the share of inputs the challengers score; 0.1 by default
```

Score with `predict` instead of calling the model yourself:

```python
from mage_ai.pipeline_services.models import predict

scores = predict('churn', customers, uri='models:/churn-classifier@champion',
                 key='customer_id', features=FEATURES, flavor='native',
                 method='predict_proba')
```

- `predict` returns the champion's output as soon as it has it. The challengers score in
  a background thread of the worker, so they add no time to a request; when they fall
  behind by 100 calls, a call is dropped and counted rather than waited on.
- The sample is chosen by `key`: the same input is always in it or out of it, so a
  customer's scores line up across versions.
- A challenger that fails is recorded with its error and never reaches the caller.
- The records go to `<data>/shadow/<model>/<challenger>/<day>.jsonl`: each call with the
  rows, both versions and both times, and each row with its key and the two outputs.
- `GET /v1/models` (`?days=7`) compares each challenger with the champion over those
  records: the rows and calls compared, errors and dropped calls, how often the outputs
  agreed, for numeric scores the mean absolute difference, each model's mean and a shared
  histogram, and each model's p50 and p95 time per call.

In MageML, `predict` scores with the model alone.

### Audit records

With `audit` on a model, every input `predict` scores gets a record, written before
`predict` returns:

```yaml
    models:
      - name: churn
        uri: models:/churn-classifier@champion
        audit:
          features: values   # the features as the model saw them; hash: one keyed hash; none
          mask: [ssn]        # stored as ***
          hash: [email]      # stored as a keyed hash (MAGE_SERVICE_AUDIT_KEY)
          reasons: 3         # the three features that moved each score most
```

| In each record | |
| --- | --- |
| When, and the input `key` | |
| The model | Name, version and the digest of its files |
| The code | The service and its source digest, the pipeline, the block and the digest of its code |
| The run | `run_id` |
| The features | As configured above; secrets never belong in them, so mask or hash those columns |
| The output | The model's output for that input |
| Reason codes | The top features by SHAP contribution, computed by CatBoost, XGBoost or LightGBM themselves; other models have none |

- Records go to `<data>/audit/<model>/<day>-<process>.jsonl`. Each record holds the SHA-256
  of the record before it in its file, so a removed or changed record breaks the chain.
- `GET /v1/audit/<model>?key=<key>` (`&days=30&limit=100`) returns an input's records,
  newest first. `GET /v1/audit/<model>/verify` checks every file's chain and names the
  first broken line.
- Files older than `MAGE_SERVICE_AUDIT_RETENTION_DAYS` (365; 0 keeps them) are removed.
- A record that cannot be written fails the prediction: no decision goes unrecorded.
- With `MAGE_SERVICE_AUDIT_KEY` set, hashed columns use HMAC-SHA-256 with it, so a guessed
  value cannot be checked against a record without the key.

### Features that follow the model

Feast's [MLflow integration](https://docs.feast.dev/reference/mlflow) tags a model version
or its training run with `feast.feature_service`, the feature service it was trained on.
`feature_service(name)` reads that tag from the embedded metadata, so a block asks Feast
for the features the model expects:

```python
from mage_ai.feature_stores.feast import FeastClient
from mage_ai.pipeline_services.models import feature_service

features = FeastClient().get_online_features(
    entities=customers[['customer_id']],
    feature_service=feature_service('churn'),
)
```

When the alias moves to a version trained on another feature service, the next export
reads the new features with no code change. The Services page shows the feature service
of each model.

## Feature stores and APIs

Blocks that call HTTP APIs run unchanged: the export pins the client packages they import,
and URLs and credentials come from the environment like any other variable. The
integration tests export two pipelines that use the Feast feature server's REST API, one
reading online features (`/get-online-features`) and pushing derived features
(`/push`), the other streaming historical features into Polars, writing them with Mage's
PostgreSQL exporter and materializing them, and check that the service gives the same
results as Mage.

## Deploy

Every export includes ready-to-use deployment files:

| Target | Files |
| --- | --- |
| IBM Cloud Code Engine | `deploy/ibm/code-engine.sh`: builds and deploys the folder; `deploy/ibm/README.md` |
| IBM Cloud Continuous Delivery | `.tekton/`: clone, build with kaniko, push to IBM Cloud Container Registry, deploy to Code Engine by digest |
| Kubernetes (IKS, OpenShift, EKS, GKE, AKS) | `deploy/kubernetes.yaml`: Deployment with health probes, a read-only root filesystem and a volume for the run history |
| One machine | `compose.yaml` |

Any platform that runs containers works: the image needs its environment and, to keep the
run history, a volume at `/var/lib/mage-service`. A service is a long-running server: its
scheduler runs time triggers and its run history is on disk, so it needs at least one
instance that stays up.

| Platform | Cloud | How |
| --- | --- | --- |
| Kubernetes: EKS, GKE, AKS, IKS, OpenShift | AWS, Google Cloud, Azure, IBM Cloud | `deploy/kubernetes.yaml` |
| Code Engine | IBM Cloud | `deploy/ibm/code-engine.sh` (one instance, min and max scale 1) and `.tekton/` |
| Cloud Run | Google Cloud | The image as is; set minimum instances to 1 and instance-based billing (CPU always allocated) so schedules run |
| Container Apps | Azure | The image as is; set minimum replicas to 1 |
| ECS on Fargate | AWS | The image as is; an EFS volume keeps the run history |
| Lambda | AWS | Not a fit: Lambda freezes a container between events and stops it after 15 minutes, so schedules would not run and the run history would not last |

On serverless platforms without a volume, the run history lasts as long as the instance.

### Several instances

The run history is a SQLite file in the data directory by default. SQLite's write-ahead log
needs every instance that opens it on one machine, as during a rolling update on one node.
For instances on several machines, put the run history in PostgreSQL:

```bash
MAGE_SERVICE_LEDGER_URL='postgres://mage:secret@db.internal:5432/mage?sslmode=verify-full'
MAGE_SERVICE_LEDGER_SCHEMA=orders_service
```

Every instance of the service uses the same URL and schema. A schema holds one service's
runs: a second service on the same schema is refused at start, so give each its own.

| | Across instances |
| --- | --- |
| Runs | A run asked of any instance runs on whichever claims it first; each run once |
| `max_concurrent_runs` | Counts the pipeline's runs on every instance |
| Cancel | Sent to any instance; the instance running the run stops it within 2 seconds |
| A stopped instance | Its running runs are failed when its lease ends (30 seconds without a renewal), at once when it stopped cleanly or its process is gone from the same machine |
| A stalled instance | Finds its runs failed when it wakes, and cannot change them |
| Schedules | One instance owns them at a time |
| Logs and outputs | On the data volume of the instance that ran the run. Another instance answers `/v1/runs/{id}/logs` with 409 and the host that has them |

`sslmode` takes libpq's modes (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`)
and `sslrootcert` a file; the `key=value` form works too. The service makes the schema and
its tables at first start, and migrates them when a newer engine starts. With
`MAGE_SERVICE_LEDGER_MIGRATE=off` it refuses an older schema instead, and an operator runs
the migration before the rollout:

```bash
mage-service ledger migrate
```

To move an existing service from its file to PostgreSQL, copy its history once with the
new settings; runs already copied are skipped, so it can run again:

```bash
mage-service ledger import /var/lib/mage-service/runs.db
```

An engine refuses a run history newer than it knows, file or schema, instead of writing
to it.

## Settings

| Variable | Default | |
| --- | --- | --- |
| `MAGE_SERVICE_HOST` | `127.0.0.1`; `0.0.0.0` in the image | Address to listen on |
| `MAGE_SERVICE_PORT` | `PORT` when a platform sets it, else `8080` | Port to listen on |
| `MAGE_SERVICE_TOKEN` | required unless on 127.0.0.1 | Token for every API call (`_FILE` works); on 127.0.0.1 a made token goes to `service-token` in the data directory |
| `MAGE_SERVICE_READ_TOKEN` | none | Read-only token |
| `MAGE_SERVICE_SCHEDULES` | `off` | `on` runs the time triggers |
| `MAGE_SERVICE_MAX_RUNS` | `8` | Runs of all pipelines at once |
| `MAGE_SERVICE_PYTHON_WORKERS` | CPUs, at most 8 | Python blocks at once |
| `MAGE_SERVICE_RETENTION_DAYS` | `30` | Days runs and their outputs are kept |
| `MAGE_SERVICE_SHUTDOWN_SECONDS` | `60` | Time running runs get to finish after SIGTERM |
| `MAGE_SERVICE_LOG_FORMAT` | `text` | `json` for log collectors |
| `MAGE_SERVICE_SECRETS` | none | Secrets to fetch at start |
| `MAGE_SERVICE_SECRETS_DIR` | none | Directory of secret files |
| `MAGE_SERVICE_STRICT_ENV` | `off` | Refuse to start without required variables |
| `MAGE_SERVICE_CHAINS` | `on` | `off` runs every block on its own |
| `MAGE_SERVICE_RUST_CHAINS` | `on` | `off` runs Rust blocks on their own |
| `MAGE_SERVICE_RUST_RESIDENT` | `on` | `off` starts the Rust binary for each run instead of keeping it running between runs |
| `MAGE_SERVICE_RUST_MAX_JOBS` | `1000` | Jobs a resident Rust program serves before it is replaced |
| `MAGE_SERVICE_AUDIT_RETENTION_DAYS` | `365` | Days audit records are kept; 0 keeps them |
| `MAGE_SERVICE_AUDIT_KEY` | none | The key of hashed audit columns (HMAC-SHA-256) |
| `MAGE_SERVICE_R_RESIDENT` | `on` | `off` starts Rscript for each R block run instead of keeping an R session in each Python worker |
| `MAGE_SERVICE_KEEP_OUTPUTS` | `off` | `on` writes every block's output, not only the ones read |
| `MAGE_SERVICE_WORKER_ENV` | default | What block code sees; see below. `strict` passes only system variables, the variables the export lists, and `MAGE_SERVICE_PASS_ENV` |
| `MAGE_SERVICE_PASS_ENV` | none | In strict mode, more variables for blocks: names or `PREFIX_*` patterns, separated by commas |
| `MAGE_SERVICE_PYTHON_MAX_RSS_MB` | 0 | A Python worker serving streams is replaced when its memory passes this; 0 replaces it when its memory doubled since its first batch plus 256 MB. Stream batches do not count toward `MAGE_SERVICE_PYTHON_MAX_BLOCKS`, and the stream's blocks load into the next worker before its next batch |
| `MAGE_SERVICE_STREAM_LOG_MB` | 64 | A stream's log (`<data>/streams/<pipeline>/stream.log`) moves to `stream.log.1` past this size; every line also goes to the service's output |
| `MAGE_SERVICE_STREAM_LOG_FILES` | 4 | Rotated stream logs kept |
| `MAGE_SERVICE_INGEST_TOKEN` | none | A token that can only send messages to webhook streams, for webhook senders |
| `MAGE_SERVICE_LEDGER_URL` | none | A PostgreSQL run history, shared by instances on several machines; see [Several instances](#several-instances) |
| `MAGE_SERVICE_LEDGER_SCHEMA` | `mage_service` | Its schema; one service per schema |
| `MAGE_SERVICE_LEDGER_MIGRATE` | `on` | `off` refuses an older run history instead of migrating it at start |
| `MAGE_SERVICE_INSTANCE_ID` | none | A stable name for this replica, such as a Kubernetes StatefulSet pod's name. Kafka streams then join their group as static members (`group.instance.id`), so a restart within the session timeout keeps the replica's partitions instead of moving the group's partitions twice |

### What block code can read

Blocks run in processes that start from an empty environment. They get every variable of the
service except the service's own: the API tokens, `MAGE_SERVICE_SECRETS`, the IBM Cloud key the
service reads secrets with, and the other `MAGE_SERVICE_` settings (`MAGE_SERVICE_ENV` and
`MAGE_SERVICE_LOG_LEVEL` pass). Values read from secret files and managers reach the blocks.
Libraries that read their own variables, such as `MLFLOW_TRACKING_URI` or cloud credentials,
keep working. `MAGE_SERVICE_WORKER_ENV=strict` narrows the rest to system variables, the
variables the export lists and `MAGE_SERVICE_PASS_ENV`.

## How it runs your blocks

```mermaid
sequenceDiagram
  participant C as Client
  participant S as Service (Rust)
  participant W as Python worker
  participant R as Rust blocks binary
  C->>S: start a run
  S->>W: chain of Python, R and SQL blocks (one request)
  W-->>S: outputs as Arrow IPC
  S->>R: chain of Rust blocks (MAGE_CHAIN=1)
  R-->>S: last block's output
  S-->>C: run status, block by block
```

The service is written in Rust. Python blocks run in long-lived Python worker processes, so
a block pays no interpreter or import start-up; each block runs in a fresh namespace with
Mage's decorators, gets its upstream outputs in argument order and the run's variables as
`kwargs`, and its `@test` functions run on its outputs. Tables pass between blocks as Arrow
IPC files and keep their types and index. Independent branches run at the same time.

Consecutive blocks of one language run as one chain, as in Mage's production runs. A chain
of Rust blocks runs in one process of the image's Rust binary, and Polars optimizes the
blocks' lazy queries as one plan. A chain of Python, R or SQL blocks runs in one worker
request, with the outputs between them in memory. Each block keeps its own entry in the run,
its log lines and its retry settings. Only the outputs that later blocks or the API read are
written; `MAGE_SERVICE_KEEP_OUTPUTS=on` writes every block's output, and
`MAGE_SERVICE_CHAINS=off` runs every block on its own (`MAGE_SERVICE_RUST_CHAINS=off` for
Rust blocks only).

Rust blocks run in resident programs: the service starts its Rust binary once and sends it
the jobs of later runs, so a model a block loads (`mage::onnx::model`) or anything it keeps in
a `static` stays loaded between runs, as Python's workers keep theirs. A program serves one
job at a time and is replaced after `MAGE_SERVICE_RUST_MAX_JOBS` jobs; a block that panics or
runs past its timeout takes its program with it, and the next run starts a new one. Code that
counts on a fresh process per run (a `static` counter, a global it never resets) sees the
earlier runs' values: `MAGE_SERVICE_RUST_RESIDENT=off` starts a program per run.

R blocks run the same way: each Python worker keeps an R session, so R and the packages
blocks load start once. Each block runs in a fresh environment, but a package a block
attaches with `library()` stays attached for the blocks after it in that session. One row through an
exported service took 627 ms per request with Rscript per run and 8 ms resident (Rust ONNX
scoring: 22 ms and 8 ms). `MAGE_SERVICE_R_RESIDENT=off` starts Rscript for each run.

Conditional blocks written in Python run as in MageML. Before a block with conditions runs,
the worker calls each `@condition` function with the block's inputs and the run's
variables. When one returns False, the block is marked `condition_failed` with the reason
("Condition has_rows returned False."), every block after it is marked `condition_failed`
too, and the run still completes. A condition that raises counts as False; its traceback
goes to the block's log. Use it for branches such as "score the new rows, or record that
there were none": attach `has_rows` to the scoring branch and `no_rows` to the no-op one.

```python
@condition
def evaluate_condition(orders, *args, **kwargs) -> bool:
    return len(orders) > 0
```

A block that fails is retried as configured; when it still fails, no new block starts, the
blocks after it are marked `upstream_failed`, and the run fails with the block's error. A
block that runs past its timeout, or a cancelled run, is stopped with its child processes.
On SIGTERM, the service stops accepting runs, lets running ones finish up to
`MAGE_SERVICE_SHUTDOWN_SECONDS`, then cancels them. A run interrupted by a crash is marked
failed when the service starts again.

## Check that it works like Mage

`mage-service check-connections` checks that each stream's source and sinks answer, with the
service's settings and secrets, read-only, and exits 1 when one fails; `--json` prints the
reports. Run it in the container before switching traffic to a new deployment.

`mage-service validate` checks the image's files at build time. To compare outputs, run the
pipeline in Mage and with `mage-service run` from the export directory, and compare the
block outputs: the tests in `mage_ai/tests/pipeline_services` do exactly that.
