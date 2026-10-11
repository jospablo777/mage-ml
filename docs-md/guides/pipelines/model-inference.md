# Model inference

*Score data with trained models in Python, R or Rust blocks, in batch pipelines and in exported services that answer HTTP requests.*

A pipeline that scores data with a trained model is an inference pipeline. It runs in
MageML on a schedule (batch scoring), and as an [exported service](../../production/export-a-service.md)
it answers each HTTP request with its predictions (online scoring). The tutorial
[Build an ML inference service](../../getting-started/inference-service.md) goes through both with
one model in each language.

## Save models in their framework's format

Load a model in the format its framework writes. These formats load without running code
from the file, keep the model's version, and load in other languages too.

| Framework | Save | Load in a block |
| --- | --- | --- |
| scikit-learn | `skops.io.dump(model, "models/m.skops")` | `load_artifact("models/m.skops", trusted=[...])` |
| CatBoost | `model.save_model("models/m.cbm")` | `load_artifact("models/m.cbm")` |
| XGBoost | `model.save_model("models/m.ubj")` | `load_artifact("models/m.ubj")`, then `predict(xgboost.DMatrix(df))` |
| LightGBM | `booster.save_model("models/m.txt")` | `load_artifact("models/m.txt", loader="lightgbm")` |
| PyTorch | `torch.export.save(torch.export.export(model, example), "models/m.pt2")` | `load_artifact("models/m.pt2")` |
| PyTorch weights | `safetensors.torch.save_file(model.state_dict(), "models/w.safetensors")` | `model.load_state_dict(load_artifact("models/w.safetensors"))` |
| Keras | `model.save("models/m.keras")` | `load_artifact("models/m.keras")` |
| Darts | `model.save("models/forecaster.pt")` | `load_artifact("models/forecaster.pt", loader=NBEATSModel.load)` |
| Nixtla | `sf.save("models/sf.pkl")`, `nf.save("models/nf")`, `fcst.save("models/mlf")` | `load_artifact(path, loader=StatsForecast.load)` (or `NeuralForecast.load`, `MLForecast.load`) |
| MLflow | a logged model | `load_model("churn", uri="models:/churn@champion")`, or `load_artifact` on its folder |
| Any of the above | ONNX: skl2onnx, onnxmltools, `torch.onnx.export`, CatBoost `format="onnx"` | `load_artifact("models/m.onnx")` in Python, `mage::onnx` in Rust |

Pickle and joblib files also load (`.pkl`, `.joblib`). They run code from the file when
they load, so use them only for files you made. Darts and Nixtla save with pickle inside.

## Python

`load_artifact` loads a model file once per process and returns the same object to later
calls, until the file changes. In an exported service, the Python worker stays loaded
between requests, so the model loads on the first request only.

```python
from mage_ai.pipeline_services.models import load_artifact

@transformer
def score(customers, *args, **kwargs):
    model = load_artifact('models/churn.cbm')
    return customers.assign(churn_probability=model.predict(customers[FEATURES],
                                                            prediction_type='Probability')[:, 1])
```

- Paths are relative to the project, in MageML and in a service.
- The suffix picks the loader; `loader=` picks it for other files, by name (`'lightgbm'`,
  `'torchscript'`, `'joblib'`) or as a function that takes the path.
- skops loads only the types it trusts. For other types, the error lists them; check them
  and pass them with `trusted=[...]`.
- The **Model inference** transformer template starts from this.

## Rust

`mage::onnx` runs ONNX models with ONNX Runtime, linked into the block's program. Turn on
the `onnx` feature of MageML's crate in the project's `rust/Cargo.toml`:

```toml
mage-block = { path = ".mage/sdk", features = ["postgres", "onnx"] }
```

```rust
use mage::prelude::*;

fn transform(customers: DataFrame) -> Result<DataFrame> {
    let model = mage::onnx::model("models/churn.onnx")?;
    let scores = model.predict(&customers, &["tenure_months", "monthly_spend", "support_tickets"])?;
    Ok(customers.hstack(scores.columns())?)
}
```

- `predict` passes the feature columns to the model: as one matrix for a model with one
  input, or column by column for a model with an input per column (scikit-learn pipelines
  with encoders), text included. Missing numbers reach the model as NaN.
- It returns one column per output, or one per class or dimension, such as `label`,
  `probabilities_0` and `probabilities_1`.
- Convert scikit-learn classifiers with `options={id(model): {'zipmap': False}}`, so
  their probabilities are a table instead of a list of maps.
- The model loads once per process: blocks of a chain share it. In a service, a Rust
  program starts for each run, so the model loads at each request; ONNX Runtime loads
  small models in milliseconds. The **ONNX model inference** transformer template starts
  from this.

## R

R blocks load models saved with `saveRDS()`: `lm`, `glm`, ranger, tidymodels workflows
and others with a `predict()` method.

```r
#* @transformer
score_r <- function(df_1, ...) {
  model <- readRDS("models/churn_glm.rds")
  tibble::tibble(
    customer_id = df_1$customer_id,
    churn_probability = predict(model, df_1, type = "response")
  )
}
```

- Models whose engine keeps state outside R, such as keras or torch, need
  [bundle](https://rstudio.github.io/bundle/) before `saveRDS()`. XGBoost models save with
  `xgb.save()` and load with `xgb.load()`.
- [butcher](https://butcher.tidymodels.org/) removes the training data some models keep,
  which makes the file smaller.
- An R block starts an R session per run, so the model loads each run. Keep files small
  for fast responses.
- The **Model inference** R template starts from this.

## Serve predictions over HTTP

Export the scoring pipeline as a service. The export puts in the image every model file a
block names, such as `"models/churn.onnx"`, and the packages that read them; `--include`
adds files a block reaches by a computed path.

```bash
mage export service churn_lab score_churn --out churn-service
```

The request passes the rows to score as a variable; `wait` and `output` return the
block's output in the response:

```bash
curl -X POST "http://localhost:8080/v1/pipelines/score_churn/runs?wait=30&output=score_python" \
  -H "Authorization: Bearer $MAGE_SERVICE_TOKEN" -H "Content-Type: application/json" \
  -d '{"variables": {"rows": [{"customer_id": 1, "tenure_months": 3, "monthly_spend": 80.5, "support_tickets": 4}]}}'
```

A DataFrame up to 8 MB comes back as JSON rows in `output`, whichever language computed
it; a larger one, as `output_url`, which serves it as an Arrow file
(`?format=json` for JSON). A block that returns a list or a dict comes back as it is.

Name a block's parameters differently from the request's variables: Mage passes variables
as keyword arguments, and a parameter named `rows` collides with a variable named `rows`.

## In production

| Concern | What to do |
| --- | --- |
| Concurrency | `--max-runs` (or the pipeline setting) runs several requests at once; more replicas behind a load balancer scale out |
| Latency | Keep the scoring pipeline short and in one language. Python workers stay loaded, so `load_artifact` loads a model once; Rust and R start a process per run |
| Authentication | Set `MAGE_SERVICE_TOKEN`; give clients `MAGE_SERVICE_READ_TOKEN` when they only read |
| Disk | Each request is a run with stored outputs; lower `MAGE_SERVICE_RETENTION_DAYS` for frequent scoring |
| Model updates | Export and deploy again with the new file; `GET /v1/models` shows embedded MLflow models |
| Monitoring | `/metrics` for Prometheus; score a sample in MageML with a [monitor](monitoring.md) for drift |
