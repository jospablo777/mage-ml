# $name

Pipelines exported from the Mage project `$project` on
$exported_at. The service runs them without Mage: an HTTP
API, schedules, a run history and logs.

| Pipeline | Blocks | Triggers |
| --- | --- | --- |
$rows

## Build and run

```bash
docker build -t $name .
docker run --rm -p 8080:8080 -e MAGE_SERVICE_TOKEN=change-me -v $name-data:/var/lib/mage-service $name
```

Or `docker compose up --build`. Variables the blocks read with `env_var(...)`, such as
database credentials in `io_config.yaml`, go in the environment (`-e` or a `.env` file).

Run a pipeline once and exit, for a cron job or a Kubernetes Job:

```bash
docker run --rm $name run $first --var key=value
```

## Call it

```bash
export TOKEN=change-me
# Start a run and wait for it, up to 10 minutes:
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  "http://localhost:8080/v1/pipelines/$first/runs?wait=600" -d '{"variables": {}}'
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/runs
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/v1/runs/RUN_ID/logs?format=text"
```

| Endpoint | |
| --- | --- |
| `POST /v1/pipelines/{id}/runs` | Start a run: `variables`, optional `idempotency_key`; `?wait=SECONDS` returns when it ends |
| `GET /v1/runs`, `GET /v1/runs/{id}` | Runs, and one run with each block's status, duration and error |
| `GET /v1/runs/{id}/logs` | Everything the blocks printed; `?block=`, `?tail=`, `?format=text` |
| `POST /v1/runs/{id}/cancel` | Cancel a queued or running run |
| `GET /v1/snapshot` | Pipelines, counts, durations, triggers; what the console shows |
| `GET /v1/models` | Embedded models with their MLflow metadata |
| `GET /metrics` | Prometheus metrics, without a token |
| `GET /healthz` | Health, without a token |

## Models

$models

Blocks load a model the same way in Mage and in this service:

```python
from mage_ai.pipeline_services.models import load_model, model_metadata, model_path

model = load_model('fraud', uri='models:/fraud/3')   # MLflow pyfunc
path = model_path('fraud', uri='models:/fraud/3')    # the model's files
```

In Mage the model comes from MLflow; in the service it is the copy in the image, recorded
at export with its version, run, flavors, signature, params and metrics
(`GET /v1/models`). The service never calls MLflow, so it runs the exact version it was
built with. Export again to ship a new version: `mage export service ... --model
fraud=models:/fraud@champion`.

## Watch it from a terminal

`mage-console` shows the service live: each pipeline's runs, durations, throughput, recent
runs with their errors, the next scheduled run and the embedded models. It is read-only
unless started with `--allow-control`, which adds pause, resume and concurrency changes
that the service checks against the settings' revision. Build it once from this folder
(it needs Rust) and point it at the service:

```bash
cargo build --release --manifest-path build/mage_service/Cargo.toml -p mage-console
export MAGE_SERVICE_READ_TOKEN=...   # or MAGE_SERVICE_TOKEN
build/mage_service/target/release/mage-console --endpoint http://localhost:8080
```

The console accepts plain HTTP only on localhost; use HTTPS, or an SSH tunnel, for a remote
service.

## Configuration and secrets

The blocks and `io_config.yaml` read these environment variables. Values are never
exported; give them to the container. `mage-service env` shows which are set.

$environment

Every platform can put them in the container's environment, and the service also reads
secrets mounted as files:

| Where the value lives | How to give it |
| --- | --- |
| Your shell or a `.env` file | `docker run --env-file .env ...`; compose reads `.env` (see `.env.example`) |
| A file, such as a Docker or Kubernetes secret | `NAME_FILE=/run/secrets/name`: the service sets `NAME` from the file |
| A directory of secret files | `MAGE_SERVICE_SECRETS_DIR=/run/secrets`: one variable per file, named after it |
| IBM Cloud Secrets Manager | The service reads it at start-up: `MAGE_SERVICE_SECRETS='PGPASSWORD=ibm:<secret id>'` with `MAGE_SERVICE_IBM_SECRETS_MANAGER_URL` and a trusted profile or API key; see `deploy/ibm/README.md` |
| AWS Secrets Manager or Parameter Store | ECS task definition `secrets: [{name: PGPASSWORD, valueFrom: <secret ARN>}]`, or the Secrets Store CSI driver on EKS with `MAGE_SERVICE_SECRETS_DIR` |
| A Code Engine secret | `ibmcloud ce secret create --name db --from-env-file .env`, then `ibmcloud ce app create ... --env-from-secret db` |
| Kubernetes Secret | `envFrom: [{secretRef: {name: $name-secrets}}]`, or mount it and set `MAGE_SERVICE_SECRETS_DIR` |
| HashiCorp Vault | Vault Agent renders files; point `NAME_FILE` or `MAGE_SERVICE_SECRETS_DIR` at them |

A variable set directly wins over its file. Values of secret variables, and every value
read from a file, are replaced by `***` in logs and in stored errors. Mage secrets read
with `mage_secret_var('name')` come from `MAGE_SECRET_<NAME>`. With
`MAGE_SERVICE_STRICT_ENV=on` the service refuses to start while a required variable is
missing; otherwise it warns. The service's own tokens can come from files too:
`MAGE_SERVICE_TOKEN_FILE`, `MAGE_SERVICE_READ_TOKEN_FILE`.

## Deploy

| Where | Files |
| --- | --- |
| IBM Cloud Code Engine, by hand or with IBM Cloud Continuous Delivery (Tekton) | `deploy/ibm/README.md`, `deploy/ibm/code-engine.sh`, `.tekton/` |
| Kubernetes: IBM Cloud Kubernetes Service, OpenShift, EKS, GKE, AKS | `deploy/kubernetes.yaml` |
| One machine | `compose.yaml` |

Any platform that runs a container works: the image needs only its environment and a
volume at `/var/lib/mage-service` for the run history.

## Settings

| Variable | Default | |
| --- | --- | --- |
| `MAGE_SERVICE_TOKEN` | made at start and logged | Token for every API call |
| `MAGE_SERVICE_READ_TOKEN` | none | A token that can only read |
| `MAGE_SERVICE_SCHEDULES` | `off` | `on` runs the active time triggers; leave it off while Mage runs the same schedules |
| `MAGE_SERVICE_MAX_RUNS` | `8` | Runs of all pipelines at once |
| `MAGE_SERVICE_PYTHON_WORKERS` | CPUs, at most 8 | Python blocks at once |
| `MAGE_SERVICE_RETENTION_DAYS` | `30` | Days runs and their outputs are kept |
| `MAGE_SERVICE_SHUTDOWN_SECONDS` | `60` | Time runs get to finish after SIGTERM |
| `MAGE_SERVICE_LOG_FORMAT` | `text` | `json` for log collectors |
| `MAGE_SERVICE_SECRETS` | none | Secrets fetched at start-up: `[NAME=]provider:reference[#field]`, providers `ibm` and `file` |
| `MAGE_SERVICE_SECRETS_DIR` | none | A directory of secret files, one variable per file |
| `MAGE_SERVICE_STRICT_ENV` | `off` | `on` refuses to start while a required variable is missing |

See `report.txt` for what the export included and left out.
