# mageml-airflow

Run pipelines of [MageML](https://github.com/jospablo777/mage-ml) services, and of MageML
servers, from Apache Airflow 3 (and 2.10). The operators call an API: the pipeline runs in
the service or the server, with its own packages and compiled chains, never in Airflow's
Python.

```bash
uv pip install "mageml-airflow @ git+https://github.com/jospablo777/mage-ml#subdirectory=packages/mageml-airflow"
```

Add a connection of type **MageML service**: the service URL as its host and the service
token (`MAGE_SERVICE_TOKEN`) as its password. As an environment variable:

```bash
export AIRFLOW_CONN_MAGEML_DEFAULT='{"conn_type": "mageml_service", "host": "http://shop-live.internal:8080", "password": "..."}'
```

```python
from airflow.sdk import DAG
from mageml_airflow.operators import MageMLServiceRunOperator

with DAG('nightly_scores', schedule='@daily'):
    MageMLServiceRunOperator(
        task_id='score_orders',
        pipeline='score_orders',
        variables={'day': '{{ ds }}'},
        output_block='summary',
        deferrable=True,
    )
```

- One run per task instance: every try sends the same idempotency key, so a retry after a
  worker died finds the run still going instead of starting a second one, and a retry
  after a failed or cancelled run starts a new run.
- The run's log lines go to the task log as they come; a failed run fails the task with
  the failing block and its error.
- `output_block` makes that block's output the task's return value (XCom), as JSON (a
  table as rows). An output larger than `output_max_bytes` (1 MB) becomes a reference to
  read it from the service.
- `deferrable=True` waits in Airflow's triggerer and holds no worker slot.
- Clearing or marking the task failed while it runs cancels the run.

## Pipelines on a MageML server

`MageMLPipelineOperator` runs a pipeline on a MageML server (the app) through its API,
for pipelines that are not exported as a service. Add a connection of type **MageML
server** with the server URL as its host and a user's email and password (editor role):

```python
from mageml_airflow.operators import MageMLPipelineOperator

MageMLPipelineOperator(task_id='orders_report', pipeline='orders_report',
                       variables={'day': '{{ ds }}'})
```

Runs go under the pipeline's API trigger named Airflow. A retry finds the run its task
instance started, the blocks' log lines go to the task log when the run ends, a failed run
names its failed blocks, and killing the task cancels the run.

## Assets

`mageml_asset(pipeline, block=None)` is the Airflow Asset of a pipeline or a block's output.
In an operator's `outlets`, each successful run records an asset event, and DAGs scheduled
on the asset (`schedule=[asset]`) run after it.
