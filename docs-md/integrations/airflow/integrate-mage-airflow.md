# Integrate MageML into an existing Airflow project

<img src="https://www.penccil.com/files/U_95_240356858903_wt5.gif" alt="wind" />

> [!NOTE]
> This tutorial requires that you already have Airflow setup and running
> locally.

**1. Add MageML as a dependency in Airflow**

MageML has no PyPI package; `mage-ai` on PyPI is upstream Mage. Open the `requirements.txt`
file in the root directory of your Airflow project, and add MageML from its repository:

```
mage-ml @ git+https://github.com/jospablo777/mage-ml.git
```

MageML runs on Python 3.12 only, so Airflow must run on Python 3.12 too.

**2. Install MageML**

You can run MageML using Docker or from source.

### Using Docker

Build the image from the repository:

```bash
git clone https://github.com/jospablo777/mage-ml.git
cd mage-ml
docker build -t mageml .
```

### From source

```bash
git clone https://github.com/jospablo777/mage-ml.git
cd mage-ml
uv sync --locked --no-default-groups --extra postgres
```

Run the `mage` commands below with `uv run --no-sync mage` from that folder, passing the
full path of the project.

**3. Initialize MageML project**

Change directory into your Airflow’s DAGs folder. This is typically in the
folder `dags/`.

```bash
cd dags
```

Then, initialize a new MageML project in the `dags/` folder.

If you’re using Docker, run the following command in the `dags/` folder:

```bash
docker run -it -p 6789:6789 -v $(pwd):/home/src \
  mageml /app/run_app.sh mage init demo_project

```

If you installed MageML from source, run the following command in the `dags/`
folder:

```bash
mage init demo_project
```

Once finished, you should have a folder named `demo_project` inside your `dags/`
folder.

Your current folder structure should look like this:

```
airflow_root_directory/
| -- dags/
| -- | -- demo_project/
```

**4. Create one-time DAG for pipelines**

In the `dags/` folder, create a new file named `create_mage_pipelines.py`.

Then, add the following code:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from mage_ai.orchestration.airflow import create_dags
import os

ABSOLUTE_PATH = os.path.abspath(os.path.dirname(__file__))
project_path = os.path.join(ABSOLUTE_PATH, 'demo_project')

create_dags(
    project_path,
    DAG,
    PythonOperator,
    dag_settings=dict(
        start_date=datetime(2022, 8, 5),  # Change this to any start date you want
    ),
    globals_dict=globals(),
)
```

**5. Create pipeline**

### Start MageML

In the `dags/` folder, start MageML.

If you’re using Docker, run the following command in the `dags/` folder:

```bash
docker run -it -p 6789:6789 -v $(pwd):/home/src \
  mageml /app/run_app.sh mage start demo_project

```

If you installed MageML from source, run the following command in the `dags/`
folder:

```bash
mage start demo_project
```

Open [http://localhost:6789](http://localhost:6789) in your browser.

**6. Add a block**

Follow steps 1, 2, and 4 in this [tutorial](../../guides/load-api-data.md) to create
a new pipeline, add 1 data loader block, and add 1 transformer block.

**7. Run DAG in Airflow for pipeline**

1. Open the Airflow webserver UI at
   [http://localhost:8080](http://localhost:8080) in your browser.
2. If you named your pipeline `etl demo` based on the tutorial from the previous
   step, then find a DAG named `mage_pipeline_etl_demo`. If you named it
   something else, find a DAG with the prefix `mage_pipeline_`.
3. Click on the DAG to view the detail page. The URL could typically be this:
   [http://localhost:8080/admin/airflow/tree?dag_id=mage_pipeline_etl_demo](http://localhost:8080/admin/airflow/tree?dag_id=mage_pipeline_etl_demo).
4. Turn that DAG on if its currently off.
5. Trigger a new DAG run.
6. Watch the DAG as it runs each task according to the pipeline you created in
   MageML.

<img src="https://mage-ai.github.io/assets/mage-airflow.jpg" alt="MageML in Airflow" />
