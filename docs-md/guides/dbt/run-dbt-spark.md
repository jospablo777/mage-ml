# Run dbt-spark against a pySpark session.

The following procedure demonstrates how to run `dbt-spark` with a pySpark session.

1. Build a MageML Docker image with Spark following the instructions given at
[Build a Docker image with Spark environment](../../integrations/compute/spark-pyspark.md#1-build-a-docker-image-with-spark-environment).

2. Run the following command in your terminal to start MageML using Docker:
```bash
docker run -it --name mage_spark -p 6789:6789 -v $(pwd):/home/src mage_spark \
  /app/run_app.sh mage start demo_project
```

3. Create a new pipeline with a name `dbt_spark`, and add a `Scratchpad` to
test out the connection with PySpark, with the following code:
```python
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()

spark.sql("show databases;")
```
It should return results similar to the following when running:
```
[Stage 0:>                                                          
(0 + 1) / 1]

    namespace
0 default
```

<img src="https://mage-ai.github.io/assets/pyspark-scratchpad-results.png" alt="PySpark Scratchpad results" />

4. Click the `Terminal` icon on the right side of the UI, and create a dbt
project `spark_demo`, with the following commands:

```bash
root@488dc9529cf3:/home/src#  cd demo_project/dbt
root@488dc9529cf3:/home/src#  dbt init spark_demo -s
root@488dc9529cf3:/home/src#  touch spark_demo/profiles.yml
```

5. On the left side of the page in the file browser, expand the folder `demo_project/dbt/spark_demo/`.
Click the file named `profiles.yml`, and add the following settings to this file:

```yaml
spark_demo:
  target: dev
  outputs:
    dev:
      type: spark
      method: session
      schema: default
      host: local
``` 

6. Save the `profiles.yml` file by pressing `Command (⌘) + S`, then close the file
by pressing the X button on the right side of the file name `dbt/spark_demo/profiles.yml`.

7. In the file browser, create the file `model_1.sql` in the folder
`demo_project/dbt/spark_demo/models/example/`. Then click the button `dbt model`,
choose the option `Single model or snapshot (from file)`, and select `model_1.sql`.

8. In the dbt block named `model_1`, next to the label `Target` at the top,
choose `dev` in the dropdown list. You can also check `Manually enter target`,
and enter `dev` in the input field.

9. Paste the following SQL into the dbt block `model_1`:
```sql
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data
```

Click the `Compile & preview` button to execute this new model, which would
generate the results similar to the following:

<img src="https://mage-ai.github.io/assets/dbt-spark-testing-results.png" alt="dbt-spark testing results" />
