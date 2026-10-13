# Run MageML pipelines in Prefect

*Run pipelines in Prefect flows.*

First install MageML in the environment that runs Prefect, on Python 3.12:

```bash
uv add mage-ml prefect --index mageml=https://jospablo777.github.io/mage-ml/simple/
```

With pip, install the release's requirements file:
`python -m pip install -r https://jospablo777.github.io/mage-ml/install/v0.1.0/base.txt prefect`
(`pip install mage-ai` installs upstream Mage). Then download the project code into your
Prefect directory, for example as a git submodule.

The examples below use the Prefect 1 API (`Flow`, `ShellTask`).

There are two ways to run pipelines in Prefect.

1. Run pipeline in as a task or ShellTask.
2. Run pipeline as a Prefect Flow.

## Run pipeline as a Prefect task

```python
from prefect import task, Flow
import mage_ai
import os

ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
project_path = os.path.join(ABSOLUTE_PATH, 'project_path')
pipeline_name = 'pipeline_name'

@task
def run_pipeline():
    mage_ai.run(pipeline_name, project_path)

with Flow('My Mage Flow') as flow:
    run_pipeline()

flow.run()
```

## Run pipeline in a ShellTask

```python
from prefect import task, Flow
from prefect.tasks.shell import ShellTask
from prefect.triggers import all_finished
import mage_ai
import os
import prefect

ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
project_path = os.path.join(ABSOLUTE_PATH, 'project_path')
pipeline_name = 'pipeline_name'

mage_task = ShellTask(return_all=True)

@task(trigger=all_finished)
def output_print(output):
    logger = prefect.context.get("logger")
    for o in output:
        logger.info(o)

with Flow('My Mage Flow') as flow:
    run_pipeline = mage_task(command=f'mage run {project_path} {pipeline_name}')
    output = output_print(run_pipeline)

flow.run()
```

## Run pipeline as a Prefect flow

```python
from prefect import Task, Flow
from mage_ai.data_preparation.models.pipeline import Pipeline
import asyncio
import os

ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
project_path = os.path.join(ABSOLUTE_PATH, 'prefect_repo')
pipeline_name = 'default_pipeline'

pipeline = Pipeline(pipeline_name, repo_path=project_path)

tasks = {}

class ExecuteBlock(Task):
    def run(self, block):
        asyncio.run(
            block.execute(
                analyze_outputs=False,
                update_status=False,
            )
        )

flow = Flow('My Mage Flow')
blocks_by_uuid = pipeline.blocks_by_uuid

for uuid, b in blocks_by_uuid.items():
    if b.type == 'scratchpad':
        continue
    task = ExecuteBlock()
    tasks[uuid] = task

for uuid, task in tasks.items():
    flow.set_dependencies(
        task=task,
        upstream_tasks=[
            tasks[block_uuid] for block_uuid in blocks_by_uuid[uuid].upstream_block_uuids
        ],
        keyword_tasks=dict(block=blocks_by_uuid[uuid])
    )

flow.run()
```
