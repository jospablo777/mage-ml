# Spark and PySpark

![](https://user-images.githubusercontent.com/78053898/199308674-afd8860e-09de-4003-a7ed-9ab2db221b0c.png)

> [!WARNING]
> The MageML image does not install PySpark in its main Python environment (`/opt/mage`),
> because Spark SQL requires pandas below 3. The `pyspark` kernel (Sparkmagic, through Livy)
> runs from `/opt/mage-livy`. The `spark` target of the repository's Dockerfile
> (`docker build --target spark -t mage_spark .`) adds a JDK and PySpark 4.2 in a separate
> environment, `/opt/mage-spark`. The sections below that use a Spark session inside blocks
> (`kwargs['spark']`) come from upstream Mage, which installed PySpark next to Mage: they need
> PySpark in the Python environment that runs your blocks.

## Kubernetes

When you run MageML and Spark in the same Kubernetes cluster, you can set the environment variable `SPARK_MASTER_HOST`
to the url of the master node of the Spark cluster in the MageML container. Then you'll be able to connect MageML to your Spark
cluster and execute PySpark code.

Here is an overview of the steps required to use MageML with Spark in a Kubernetes cluster.

### 1. Run Spark cluster in Kubernetes cluster
You can use Helm command to run Spark cluster in the Kubernetes cluster:
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install my-release bitnami/spark
```
Then follow the instructions from the output to get the Spark master WebUI URL:
```bash
kubectl port-forward --namespace default svc/my-release-spark-master-svc 80:80
echo "Visit http://127.0.0.1:80 to use your application"
```
Find the Spark Master URL at the top of the web page.

### 2. Build a Docker image with Spark environment
* From the repository root, build the `spark` target of the Dockerfile:
  `docker build --target spark -t mage_spark .`

### 3. Run MageML in a Kubernetes cluster with the `SPARK_MASTER_HOST` environment variable
* Use the docker image built from step 2 as the container image in your Kubernetes yaml file.
* Add `SPARK_MASTER_HOST` environment variable with the Spark Master URL from step 1 to your the container spec
in your Kubernetes yaml file.
* Then deploy MageML to Kubernetes with the updated yaml file.

Here is the example Pod config:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: mage-server
spec:
  containers:
  - name: mage-server
    image: mage_spark
    imagePullPolicy: Never
    ports:
    - containerPort: 6789
    volumeMounts:
    - name: mage-fs
      mountPath: /home/src
    env:
      - name: KUBE_NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
      - name: SPARK_MASTER_HOST
        value: "spark://spark_master_url:7077"
  volumes:
  - name: mage-fs
    hostPath:
      path: /path/to/mage_project
  serviceAccountName: mage-user
```

Notes
* The environment variable is optional.
* You can specify the spark config in the project's `metadata.yaml` file as stated in [Custom Spark Session](#custom-spark-session-at-the-project-level) section.

### 4. Run PySpark code
When the MageML pod is running, you can create a "Standard (batch)" pipeline (`python` kernel) and then write PySpark code in any blocks.

In `Scratchpad` block, you'll need to manually create the Spark session with the code:
```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()
```

In other blocks, you can access the Spark session from the `kwargs` via `kwargs['spark']`.

---

## Standalone Spark cluster

### 1. Build a Docker image with Spark environment
* From the repository root, build the `spark` target of the Dockerfile:
  `docker build --target spark -t mage_spark .`

### 2. Start MageML with `SPARK_MASTER_HOST` environment variable
Type this command in your terminal to start MageML using Docker

```bash
docker run -it --name mage_spark -e SPARK_MASTER_HOST='local' -p 6789:6789 -v $(pwd):/home/src mage_spark \
  /app/run_app.sh mage start demo_project
```
Notes
* `demo_project` is the name of your project, you can change it to anything you want
* Set the Spark cluster's master node url as the value for environment variable `SPARK_MASTER_HOST`. If you
  use local Spark, you can set the value of `SPARK_MASTER_HOST` to `local` or not set the environment variable.

Notes
* The environment variable is optional.
* You can specify the spark config in the project's `metadata.yaml` file as stated in [Custom Spark Session](#custom-spark-session-at-the-project-level) section.

### 3. Run PySpark code
When MageML is running, you can create a "Standard (batch)" pipeline (`python` kernel) and then write PySpark code in any blocks.

In `Scratchpad` block, you'll need to manually create the Spark session with the code:
```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()
```

In other blocks, you can access the Spark session from the `kwargs` via `kwargs['spark']`.

---

## Custom Spark Session at the Project Level

### 1. Build a Docker image with Spark environment
* From the repository root, build the `spark` target of the Dockerfile:
  `docker build --target spark -t mage_spark .`

### 2. Start MageML using Docker with the following command in your terminal

```bash
docker run -it --name mage_spark -p 6789:6789 -v $(pwd):/home/src mage_spark \
  /app/run_app.sh mage start demo_project
```
Notes
* `demo_project` is the name of your project, you can change it to anything you want

### 3. Review the `spark_config` section in `metadata.yaml` under the project folder, and make necessary adjustments

```yaml
spark_config:
  # Application name
  app_name: 'my spark app'
  # Master URL to connect to
  # e.g., spark_master: 'spark://host:port', or spark_master: 'yarn'
  spark_master: 'local'
  # Executor environment variables
  # e.g., executor_env: {'PYTHONPATH': '/home/path'}
  executor_env: {}
  # Jar files to be uploaded to the cluster and added to the classpath
  # e.g., spark_jars: ['/home/path/example1.jar']
  spark_jars: []
  # Path where Spark is installed on worker nodes,
  # e.g. spark_home: '/usr/lib/spark'
  spark_home: null
  # List of key-value pairs to be set in SparkConf
  # e.g., others: {'spark.executor.memory': '4g', 'spark.executor.cores': '2'}
  others: {}
```

### 4. Run PySpark code
When MageML is running, you can create a "Standard (batch)" pipeline (`python` kernel), and add a block,
then write PySpark code using the Spark session via `kwargs['spark']`, e.g.,

```python
spark = kwargs.get('spark')
print(spark.sql('select 1'))
```

In `Scratchpad` block, you'll need to manually create the Spark session with the code:

```python
from mage_ai.data_preparation.repo_manager import RepoConfig, get_repo_path
from mage_ai.services.spark.config import SparkConfig
from mage_ai.services.spark.spark import get_spark_session

repo_config = RepoConfig(repo_path=get_repo_path())
spark_config = SparkConfig.load(
    config=repo_config.spark_config)

spark = get_spark_session(spark_config)
print(spark.sql('select 1'))
```

## Custom Spark Session at the Pipeline Level

It is also possible to use different Spark Sessions in different pipelines under the same project.

### 1. Build a Docker image with Spark environment
* From the repository root, build the `spark` target of the Dockerfile:
  `docker build --target spark -t mage_spark .`

### 2. Start MageML using Docker with the following command in your terminal

```bash
docker run -it --name mage_spark -p 6789:6789 -v $(pwd):/home/src mage_spark \
  /app/run_app.sh mage start demo_project
```
Notes
* `demo_project` is the name of your project, you can change it to anything you want

### 3. Create a "Standard (batch)" pipeline (`python` kernel), and update the `spark_config` section in `metadata.yaml` under the pipeline folder:

```yaml
spark_config:
  # Application name
  app_name: 'my spark app at pipeline level'
  # Master URL to connect to
  # e.g., spark_master: 'spark://host:port', or spark_master: 'yarn'
  spark_master: 'local'
  # Executor environment variables
  # e.g., executor_env: {'PYTHONPATH': '/home/path'}
  executor_env: {}
  # Jar files to be uploaded to the cluster and added to the classpath
  # e.g., spark_jars: ['/home/path/example1.jar']
  spark_jars: []
  # Path where Spark is installed on worker nodes,
  # e.g. spark_home: '/usr/lib/spark'
  spark_home: null
  # List of key-value pairs to be set in SparkConf
  # e.g., others: {'spark.executor.memory': '4g', 'spark.executor.cores': '2'}
  others: {}
```

### 4. Run PySpark code
Add a block to the pipeline, then write PySpark code using the Spark session via `kwargs['spark']`, e.g.,

```python
spark = kwargs.get('spark')
print(spark.sql('select 1'))
```

## Custom Spark session via code

It is also possible to create a custom Spark session using Python code. Here are the instructions:

1. Set the `run_pipeline_in_one_process: true` in your pipeline's metadata.yaml
2. In the `spark_config` of your project's metadata.yaml or pipeline's metadata.yaml, set `use_custom_session` to `true`. Example config:
```yaml
    spark_config:
      use_custom_session: true
      custom_session_var_name: 'spark'
    ```
3. In the first block in your pipeline, write Python code to create the Spark session and set it in the `kwargs['context']['spark']`. Example code:
```python
    from pyspark.conf import SparkConf
    from pyspark.sql import SparkSession

    if 'data_loader' not in globals():
        from mage_ai.data_preparation.decorators import data_loader

    @data_loader
    def load_data(*args, **kwargs):
        spark = (
            SparkSession
            .builder
            .appName('Test spark')
            .getOrCreate()
        )
        kwargs['context']['spark'] = spark
        ...
    ```
4. Then, you can access the Spark session in the subsequent blocks via `kwargs['spark']`.

---

## Hadoop and Yarn cluster for Spark

### 1. Start MageML with your cluster's configuration

From the repository root, build the `spark` target, then start it with the directory that
holds the cluster's `core-site.xml` and `yarn-site.xml`:

```bash
docker build --target spark -t mage_spark .
docker run -it -p 6789:6789 -v $(pwd):/home/src \
  -v /path/to/hadoop/conf:/etc/hadoop/conf:ro \
  -e HADOOP_CONF_DIR=/etc/hadoop/conf -e YARN_CONF_DIR=/etc/hadoop/conf \
  mage_spark mage start demo_project
```

`demo_project` is the name of your project.

### 2. Run PySpark code using Hadoop Yarn

* Change the `metadata.yaml` file in the main pipeline folder to include the following Spark settings:
```yaml
spark_config:
  # Application name
  app_name: 'my spark yarn app'
  # Master URL to connect to
  # e.g., spark_master: 'spark://host:port', or spark_master: 'yarn'
  spark_master: 'yarn'
  # Executor environment variables
  # e.g., executor_env: {'PYTHONPATH': '/home/path'}
  executor_env: {}
  # Jar files to be uploaded to the cluster and added to the classpath
  # e.g., spark_jars: ['/home/path/example1.jar']
  spark_jars: []
  # Path where Spark is installed on worker nodes,
  # e.g. spark_home: '/usr/lib/spark'
  spark_home: null
  # List of key-value pairs to be set in SparkConf
  # e.g., others: {'spark.executor.memory': '4g', 'spark.executor.cores': '2'}
  others: {}
```
* Create a new `Standalone (batch)` pipeline and add a `Data loader`, then run it with the following code:
```python
@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    spark = kwargs.get('spark')
    spark_conf = spark.sparkContext.getConf()
    print('#' * 40)
    print(spark_conf.toDebugString())
    print('#' * 40)
    print(spark.sql('select 1'))
    return {}
```
* Verify that the added Spark code is running through a Yarn master.

---

## AWS

Here is an overview of the steps required to use MageML locally with Spark in AWS:

1. [Create an S3 bucket for Spark](#1-create-an-s3-bucket-for-spark)
2. [Start MageML](#2-start-mageml)
3. [Configure project’s metadata settings](#3-configure-projects-metadata-settings)
4. [Launch EMR cluster](#4-launch-emr-cluster)
5. [Allow EMR connection permissions](#5-allow-emr-connection-permissions)
6. [Sample pipeline with PySpark code](#6-sample-pipeline-with-pyspark-code)
7. [Debugging](#7-debugging)
8. [Clean up](#8-clean-up)

If you get stuck, run into problems, or just want someone to walk you through
these steps, please open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues)

### 1. Create an S3 bucket for Spark

Using Spark on AWS EMR requires an AWS S3 bucket to store logs, scripts, etc.

Follow
[AWS’s guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)
to create an S3 bucket. You don’t need to add any special permissions or
policies to this bucket.

Once you created an S3 bucket, note the name of the bucket (we’ll need it
later).

### 2. Start MageML

Docker is the simplest way to run MageML with Spark. Build the image from the repository
(`docker build -t mageml .`), then type this command in your terminal to start it (Note:
`demo_project` is the name of your project, you can change it to anything you want):

```bash
docker run -it -p 6789:6789 -v $(pwd):/home/src mageml \
  /app/run_app.sh mage start demo_project
```

### 3. Configure project’s metadata settings

Open your project’s `metadata.yaml` file located at the root of your project’s
directory: `demo_project/metadata.yaml` (presuming your project is named
`demo_project`).

Change the values for the keys mentioned in the following steps.

* Set `remote_variables_dir`.

  Change the value for key `remote_variables_dir` to equal the S3 bucket you
  created in an earlier step.
  For example, if your S3 bucket is named `my-awesome-bucket`, then the value for
  the key `remote_variables_dir` should be `s3://my-awesome-bucket`.

* Remove optional settings

  You can remove the following 2 keys:
  * `master_security_group`
  * `slave_security_group`

Your final `metadata.yaml` file could look like this:

```yaml
variables_dir: ./
remote_variables_dir: s3://my-awesome-bucket

emr_config:
  master_instance_type: "r5.4xlarge"
  slave_instance_type: "r5.4xlarge"
```

Additionally, you can customize the instance count and Spark properties in your EMR cluster. Here is
an example config:
```yaml
emr_config:
  bootstrap_script_path: /path/to/emr_bootstrap.sh  # Use a custom bootstrap script for EMR cluster
  master_instance_type: 'r5.4xlarge'
  master_spark_properties:
    spark.driver.memory: 400M
    spark.executor.cores: '8'
  slave_instance_count: 2
  slave_instance_type: 'r5.4xlarge'
  slave_spark_properties:
    spark.driver.memory: 400M
    spark.executor.cores: '8'
  scaling_policy:             # Set up auto scaling of the EMR cluster
    unit_type: 'Instances'    # 'InstanceFleetUnits'|'Instances'|'VCPU'
    minimum_capacity_units: 1
    maximum_capacity_units: 4
    maximum_on_demand_capacity_units: 4
    maximum_core_capacity_units: 3
  spark_jars:                 # External Jar libraries
  - s3://bucket/mysql-connector-java-8.0.28.jar
```

> [!NOTE]
> You may need to request an increase in quota limits for using those instance
> types.
>
> For more information on how to view your quota limits and request an increase,
> check out this
> [AWS document](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html).
>

### 4. Launch EMR cluster

You’ll need an AWS Access Key ID and an AWS Secret Access Key. This is provided
from AWS’s IAM Management console.

Once you’ve acquired those credentials, you can switch your kernel in the notebook
to `pyspark` kernel. MageML creates the EMR cluster when you switch to the `pyspark`
kernel. The cluster is usually created and initialized within 8~10 minutes. Then you can select
your EMR cluster in the dropdown of your `pyspark` kernel selector.

### 5. Allow EMR connection permissions

You need to whitelist your IP in the security group of your EMR cluster to access it.

1. Open the EC2 dashboard in AWS
1. Click on "Security Groups" on the left side panel under the section "Network
   & Security"
1. Find the security group named "ElasticMapReduce-master"
1. Add a new inbound rule with the following values:

| Type       | Protocol | Port range | Source |
| ---------- | -------- | ---------- | ------ |
| Custom TCP | TCP      | 8998       | My IP  |

This will allow your locally running MageML to remotely access AWS EMR.

### 6. Sample pipeline with PySpark code

If you aren’t using Docker and you installed MageML from source, you must run the
following commands in your terminal to use the `pyspark` kernel:

```bash
pip install sparkmagic
mkdir ~/.sparkmagic
wget https://raw.githubusercontent.com/jupyter-incubator/sparkmagic/master/sparkmagic/example_config.json
mv example_config.json ~/.sparkmagic/config.json
sed -i 's/localhost:8998/host.docker.internal:9999/g' ~/.sparkmagic/config.json
```

---

1. Create a new pipeline by going to `File` in the top left corner of the page
   and then clicking `New pipeline`.
2. Change the pipeline’s kernel from `python` to `pyspark`. Click the button
   with the green dot and the word `python` next to it. This is located at the
   top of the page on the right side of your header.
3. Click `+ Data loader`, then `Generic (no template)` to add a new data loader
   block.
4. Paste the following sample code in the new data loader block:

```python
from pandas import DataFrame
import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

def data_from_internet():
    url = 'https://raw.githubusercontent.com/mage-ai/datasets/master/restaurant_user_transactions.csv'

    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')

@data_loader
def load_data(**kwargs) -> DataFrame:
    df_spark = kwargs['spark'].createDataFrame(data_from_internet())

    return df_spark
```

1. Click `+ Data exporter`, then `Generic (no template)` to add a new data
   exporter block.
2. Paste the following sample code in the new data exporter block (change the
   `s3://bucket-name` to the bucket you created from a previous step):

```python
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(df: DataFrame, **kwargs) -> None:
    (
        df.write
        .option('delimiter', ',')
        .option('header', 'True')
        .mode('overwrite')
        .csv('s3://mage-spark-cluster/demo_project/demo_pipeline/')
    )
```

#### Verify everything worked

Let’s load the data from S3 that we just created using Spark:

1. Click `+ Data loader`, then `Generic (no template)` to add a new data loader
   block.
2. Paste the following sample code in the new data loader block (change the
   `s3://bucket-name` to the bucket you created from a previous step):

```python
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(**kwargs) -> DataFrame:
    df = (
        kwargs['spark'].read
        .format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option('delimiter', ',')
        .load('s3://mage-spark-cluster/demo_project/demo_pipeline/*')
    )

    return df
```

### 7. Debugging

If you run into any problems, 1st thing to try is restarting the kernel: `Run` >
`Restart kernel`.

If the block is hanging when running the block, it could be due to network connection issue.
Make sure the EMR cluster is accessible from the MageML server. You can verify whether the server's
security group or IP is whitelisted in EMR cluster's security group following section [Allow EMR connection permissions](#5-allow-emr-connection-permissions)

If that doesn’t work, restart the app by stopping the docker container and
starting it again.

### 8. Clean up

Please make sure to terminate your EMR cluster when you’re done using it so you
can save money.

---
