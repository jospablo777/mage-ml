# Build an ML inference service

*Train a model, score with it in Python, Rust and R, and serve predictions over HTTP from an exported service.*

This tutorial trains a churn model, scores customers with it in three languages, and
exports the scoring pipelines as one service that answers HTTP requests with predictions.

Here the requests carry the features. A pipeline can also consume raw data, turn it into
features and pass them straight to the model, so each request gets its prediction right
away. If you already have mature ML infrastructure, such as a feature store, the pipeline
reads the features from it instead. Either way, you build an ML service quickly, deploy it
as one Docker image, and monitor it with its run history, logs and metrics.

| Step | What it gives you |
| --- | --- |
| 1. Train | A scikit-learn model saved as skops and as ONNX, and an R model saved as RDS |
| 2. Score in Python | A pipeline that scores the rows of a request with the skops model |
| 3. Score in Rust | The same with the ONNX model, compiled into one program |
| 4. Score in R | The same with the R model |
| 5. Serve | A Docker service that answers each request with JSON predictions |

Every command, output and time below comes from a real run. The project is called
`churn_lab`; create it with `mage init churn_lab` and give it an R environment with
`mage r init churn_lab` (see [R setup](../guides/blocks/r-setup.md)). Training also needs
skl2onnx: `uv pip install skl2onnx`.

## 1. Train the models

Create a pipeline `train_churn` with a data loader `customers` and two transformers below
it: `train_model` in Python and `train_glm` in R.

```python
# data_loaders/customers.py
import numpy as np
import pandas as pd

@data_loader
def customers(*args, **kwargs):
    """Customers and whether they churned, which depends on tenure, spend and tickets."""
    rng = np.random.default_rng(7)
    rows = 20_000
    tenure = rng.integers(1, 72, rows)
    spend = rng.gamma(2.0, 30.0, rows).round(2)
    tickets = rng.poisson(1.5, rows)
    logit = 1.2 - 0.05 * tenure + 0.01 * spend + 0.45 * tickets
    churned = rng.random(rows) < 1 / (1 + np.exp(-logit))
    return pd.DataFrame({
        'customer_id': np.arange(rows),
        'tenure_months': tenure.astype('float64'),
        'monthly_spend': spend,
        'support_tickets': tickets.astype('float64'),
        'churned': churned.astype('int64'),
    })
```

```python
# transformers/train_model.py
import os

import skops.io
from skl2onnx import to_onnx
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

from mage_ai.settings.repo import get_repo_path

FEATURES = ['tenure_months', 'monthly_spend', 'support_tickets']

@transformer
def train_model(customers, *args, **kwargs):
    """Trains a classifier, saves it as skops and as ONNX, and returns its test AUC."""
    train, test = train_test_split(customers, test_size=0.2, random_state=7)
    model = GradientBoostingClassifier(random_state=7).fit(train[FEATURES], train['churned'])
    auc = roc_auc_score(test['churned'], model.predict_proba(test[FEATURES])[:, 1])

    models = os.path.join(get_repo_path(), 'models')
    os.makedirs(models, exist_ok=True)
    skops.io.dump(model, os.path.join(models, 'churn.skops'))
    # zipmap=False: the probabilities come out as a tensor, one column per class.
    onnx = to_onnx(model, train[FEATURES].to_numpy('float32')[:1],
                   options={id(model): {'zipmap': False}})
    with open(os.path.join(models, 'churn.onnx'), 'wb') as file:
        file.write(onnx.SerializeToString())
    return {'auc': round(auc, 4), 'trained_on': len(train)}
```

```r
# transformers/train_glm.r
#* @transformer
train_glm <- function(df_1, ...) {
  model <- glm(
    churned ~ tenure_months + monthly_spend + support_tickets,
    data = df_1, family = binomial()
  )
  # The training rows are not needed to predict; leave them out of the file.
  model$data <- NULL
  model$model <- NULL
  dir.create("models", showWarnings = FALSE)
  saveRDS(model, "models/churn_glm.rds")
  tibble::tibble(term = names(coef(model)), estimate = unname(coef(model)))
}
```

Run it:

```bash
mage run churn_lab train_churn
```

It takes 9 seconds and writes `models/churn.skops` (2.5 MB), `models/churn.onnx` (54 KB)
and `models/churn_glm.rds` (3.2 MB). Each model is in its framework's own format; see
[Model inference](../guides/pipelines/model-inference.md) for the others.

## 2. Score in Python

Create a pipeline `score_churn`. Its data loader reads the customers to score from the
`rows` variable, which each request sets:

```python
# data_loaders/request_rows.py
import pandas as pd

FEATURES = ['tenure_months', 'monthly_spend', 'support_tickets']

@data_loader
def request_rows(*args, **kwargs):
    """The customers to score, from the run's rows variable."""
    rows = pd.DataFrame(kwargs.get('rows') or [])
    missing = [c for c in ['customer_id', *FEATURES] if c not in rows.columns]
    if missing:
        raise ValueError(f'Each row needs {", ".join(missing)}.')
    return rows
```

The transformer below it scores them. Start it from the **Model inference** template:

```python
# transformers/score_python.py
from mage_ai.pipeline_services.models import load_artifact

FEATURES = ['tenure_months', 'monthly_spend', 'support_tickets']

@transformer
def score_python(customers, *args, **kwargs):
    """The probability that each customer churns."""
    # skops loads only the types it is told to trust; a tree model needs its Tree type.
    model = load_artifact('models/churn.skops', trusted=['sklearn.tree._tree.Tree'])
    return customers[['customer_id']].assign(
        churn_probability=model.predict_proba(customers[FEATURES])[:, 1],
    )
```

`load_artifact` loads the model once per process. Without `trusted`, the first run stops
with the types to check:

```text
ValueError: .../models/churn.skops holds types skops does not trust by default:
sklearn.tree._tree.Tree. Check them, then pass them: load_artifact(path, trusted=[...]).
```

> [!NOTE]
> Name the parameter `customers`, not `rows`: MageML passes the variables as keyword
> arguments, so a parameter named like a variable gets two values.

Score two customers:

```bash
mage run churn_lab score_churn --runtime-vars '{"rows": [
  {"customer_id": 1, "tenure_months": 3, "monthly_spend": 80.5, "support_tickets": 4},
  {"customer_id": 2, "tenure_months": 60, "monthly_spend": 20.0, "support_tickets": 0}]}'
```

## 3. Score in Rust

Rust blocks run ONNX models with ONNX Runtime. Turn on the `onnx` feature of MageML's crate
in the project's `rust/Cargo.toml` (the file appears with the first Rust block):

```toml
mage-block = { path = ".mage/sdk", features = ["postgres", "onnx"] }
```

Create a pipeline `score_churn_rust` with two Rust blocks. Both are Rust, so they compile
into one program:

```rust
// data_loaders/request_rows_rust.rs
use mage::prelude::*;
use std::io::Cursor;

/// The customers to score, from the run's rows variable.
fn load_data(vars: Vars) -> Result<DataFrame> {
    let rows: Vec<Value> = vars.get("rows")?;
    Ok(JsonReader::new(Cursor::new(serde_json::to_vec(&rows)?)).finish()?)
}
```

```rust
// transformers/score_rust.rs
use mage::prelude::*;

/// The probability that each customer churns, from the ONNX copy of the model.
fn transform(rows: DataFrame) -> Result<DataFrame> {
    let model = mage::onnx::model("models/churn.onnx")?;
    let scores = model.predict(&rows, &["tenure_months", "monthly_spend", "support_tickets"])?;
    let probability = scores
        .column("probabilities_1")?
        .clone()
        .with_name("churn_probability".into());
    Ok(DataFrame::new(rows.height(), vec![rows.column("customer_id")?.clone(), probability])?)
}
```

```bash
mage run churn_lab score_churn_rust --runtime-vars '{"rows": [...]}'
```

```text
Compiled the chain request_rows_rust, score_rust in 1.9 s.
Pipeline run completed.
```

Without the feature, the build says which line to add to `rust/Cargo.toml`. The first
build downloads ONNX Runtime.

## 4. Score in R

Create a pipeline `score_churn_r` with two R blocks:

```r
# data_loaders/request_rows_r.r
#* @data_loader
request_rows_r <- function(...) {
  # The customers to score, from the run's rows variable.
  dplyr::bind_rows(variable("rows"))
}
```

```r
# transformers/score_r.r
#* @transformer
score_r <- function(df_1, ...) {
  model <- readRDS("models/churn_glm.rds")
  tibble::tibble(
    customer_id = df_1$customer_id,
    churn_probability = predict(model, df_1, type = "response")
  )
}
```

```bash
mage run churn_lab score_churn_r --runtime-vars '{"rows": [...]}'
```

## 5. Serve predictions

Export the three scoring pipelines as one service:

```bash
mage export service churn_lab score_churn score_churn_rust score_churn_r \
  --name churn-scoring --out churn-service
```

The export finds the model files the blocks name and puts them in the image, with the
packages that read them (skops for the Python model; ONNX Runtime is inside the Rust
program):

```text
Notes:
  - Included models/churn.onnx (54.3 KB), named in transformers/score_rust.rs.
  - Included models/churn.skops (2.5 MB), named in transformers/score_python.py.
  - Included models/churn_glm.rds (3.2 MB), named in transformers/score_r.r.
```

Build and start it:

```bash
docker build -t churn-scoring churn-service
docker run -d -p 8080:8080 -e MAGE_SERVICE_TOKEN=change-me churn-scoring
```

Each request starts a run with the rows as a variable; `wait` returns when it ends and
`output` puts the scoring block's output in the response:

```bash
curl -X POST "http://localhost:8080/v1/pipelines/score_churn/runs?wait=60&output=score_python" \
  -H "Authorization: Bearer change-me" -H "Content-Type: application/json" \
  -d '{"variables": {"rows": [
        {"customer_id": 1, "tenure_months": 3, "monthly_spend": 80.5, "support_tickets": 4},
        {"customer_id": 2, "tenure_months": 60, "monthly_spend": 20.0, "support_tickets": 0}]}}'
```

```json
{
  "status": "completed",
  "output": [
    {"churn_probability": 0.9565807902763772, "customer_id": 1},
    {"churn_probability": 0.1854533884319084, "customer_id": 2}
  ],
  ...
}
```

The Rust pipeline returns the same probabilities from the ONNX copy of the model
(`0.9565808`, `0.18545341`); the R model is another model and gives `0.977` and `0.164`.
Each block returns a DataFrame, which comes back as JSON rows whatever the language.

### How fast it answers

Measured on a laptop (Docker on Apple M4 Pro), 30 requests of two rows each after the
first ones:

| Pipeline | Median | p95 |
| --- | --- | --- |
| Python (`score_churn`) | 8 ms | 9 ms |
| Rust (`score_churn_rust`) | 21 ms | 23 ms |
| R (`score_churn_r`) | 391 ms | 416 ms |

The Python worker stays loaded between requests, so `load_artifact` loads the model once.
A Rust program starts for each run and loads the ONNX model, which adds about 15 ms. An R
block starts an R session for each run. The first Python request took 1.6 s: the worker
imported its packages and loaded the model.

For online scoring with small requests, Python is the fastest of the three here. For
large batches, Rust computes faster and with less memory; see
[Pipeline performance](../production/pipeline-performance.md).

## Make it production ready

- Deploy the image where your containers run: Kubernetes with the generated
  `deploy/kubernetes.yaml`, IBM Cloud Code Engine, Google Cloud Run, Azure Container Apps
  or AWS ECS. See [Deploy](../production/pipeline-services.md#deploy).
- Set your own `MAGE_SERVICE_TOKEN`, and give read-only clients
  `MAGE_SERVICE_READ_TOKEN`.
- Run more requests at once with `--max-runs`, and more replicas behind a load balancer.
- Each request is a run with stored outputs: lower `MAGE_SERVICE_RETENTION_DAYS` when you
  score often.
- Scrape `/metrics` with Prometheus, and watch the scores' drift with a
  [monitor](../guides/pipelines/monitoring.md) on a pipeline that scores a sample in MageML.
- To ship a new model, train it, export again and deploy the new image.

The [Model inference](../guides/pipelines/model-inference.md) guide covers other frameworks
and formats: CatBoost, XGBoost, LightGBM, PyTorch, Keras, Darts, Nixtla and MLflow.
