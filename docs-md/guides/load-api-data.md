# ETL pipeline tutorial

*Build a data pipeline that loads restaurant data, transforms it, then exports it to a DuckDB database.*

## Overview

In this tutorial, we'll create a data pipeline with the following steps:

**1. Load data from an online endpoint**

We'll use a Python block to load data from an online endpoint: a CSV file containing restaurant user transactions. We'll also run a test to make sure the data is clean.

**2. Clean column names and add a new column**

We'll use a Python block to transform the data by cleaning the columns and creating a new column, `number of meals`, that counts the number of meals for each user.

**3. Write the transformed data to a local DuckDB database**

Finally, we'll write the transformed data to a local DuckDB table.

If you haven't created a MageML project before, follow the [setup guide](../getting-started/setup.md) before starting this tutorial.

## Tutorial

<details><summary>1. Create a pipeline</summary>

Open the pipelines list page (`/pipelines`). This is the default page when navigating to MageML in your web browser. If you have MageML running, click [here](http://localhost:6789/pipelines).

In the top left corner, select the button labeled *+ New*, then select the option labeled *Standard (batch)* to create a new pipeline.

In the left vertical navigation, click the last link labeled *Pipeline settings*. Change the pipeline's name to `ETL demo`. Click the button labeled *Save pipeline settings*.

<img src="https://mage-ai.github.io/assets/tutorials/etl/create-new-pipeline.gif" alt="Create new pipeline" />

</details>

<details><summary>2. Load data from an API</summary>

In the left vertical navigation, click the 1st link labeled *Edit pipeline*. Click the button labeled *+ Data loader* then hover over *Python*, and click the option labeled *API*.

A dialog menu will appear. Change the block name to `load data`. Click the button labeled *Save and add*. Paste the following code in the data loader block:

```python
import io
import pandas as pd
import requests

@data_loader
def load_data_from_api(*args, **kwargs):
    url = 'https://raw.githubusercontent.com/mage-ai/datasets/master/restaurant_user_transactions.csv'
    response = requests.get(url)
    return pd.read_csv(io.StringIO(response.text), sep=',')

@test
def test_row_count(df, *args) -> None:
    assert len(df.index) >= 1000, 'The data does not have enough rows.'
```

Run the block by clicking the play icon button in the top right corner of the data loader block or one of the following keyboard shortcuts:

* `⌘ + Enter`
* `Control + Enter`
* `Shift + Enter`

After you run the block (⌘ + Enter), you'll see a sample of the data that was loaded.

<img src="https://mage-ai.github.io/assets/tutorials/etl/load-data.gif" alt="Load data" />

</details>

<details><summary>3. Transform data</summary>

Let's transform the data in 2 ways:

* Add a column that counts the number of meals for each user.
* Clean the column names to properly store in a database.

Follow these steps:

Click the button labeled *+ Transformer*, then hover over *Python*, and click the option labeled *Generic (no template)*. Paste the following code in the transformer block:

```python
def number_of_rows_per_key(df, key, column_name):
    data = df.groupby(key)[key].agg(['count'])
    data.columns = [column_name]
    return data

def clean_column(column_name):
    return column_name.lower().replace(' ', '_')

@transformer
def transform(df, *args, **kwargs):
    # Add number of meals for each user
    df_new_column = number_of_rows_per_key(df, 'user ID', 'number of meals')
    df = df.join(df_new_column, on='user ID')

    # Clean column names
    df.columns = [clean_column(col) for col in df.columns]

    return df.iloc[:100]

@test
def test_number_of_columns(df, *args) -> None:
    assert len(df.columns) >= 11, 'There needs to be at least 11 columns.'
```

Run the block by clicking the play icon button in the top right corner of the transformer block or press one of the following keyboard shortcuts:
- `⌘ + Enter`
- `Control + Enter`
- `Shift + Enter`

After you run the block (⌘ + Enter), you'll see a sample of the data that was transformed.

<img src="https://mage-ai.github.io/assets/tutorials/etl/transform-data.gif" alt="Transform data" />

</details>

<details><summary>4. Export data to DuckDB</summary>

Let's export the data to a local DuckDB database. Create a new `Data Exporter` SQL block and change the connection type to be `DuckDB`.

Next, change the `Table name` to be `magic_duck.restaurant_user_transactions` and make sure the content of the block is:

```sql
SELECT * FROM {{ df_1 }}
```

This will take our data from the previous block and export it to a local DuckDB table, `magic_duck.restaurant_user_transactions`.

After you run the block (⌘ + Enter), you'll see a sample of the data that was exported.

<img src="https://mage-ai.github.io/assets/tutorials/etl/export-data.gif" alt="Export data" />

You should also see some logs indicating the export was completed, e.g.

```bash
DuckDB initialized
└─ Opening connection to DuckDB...DONE
Exporting data from upstream block floral_star to restaurant_user_transactions.
Type: integer
Type: integer
Type: string
Type: integer
Type: string
Type: integer
Type: string
Type: integer
Type: integer
Type: integer
```

</details>

You've built an end-to-end ETL pipeline that loads data, transforms it, and exports it to a
database. When a trigger runs it, MageML fuses the chain of blocks by default; see
[Block fusion](../design/data-pipeline-management.md#block-fusion).

If you have more questions or ideas, open an issue in
[GitHub issues](https://github.com/jospablo777/mage-ml/issues).
