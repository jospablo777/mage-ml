import os

import joblib
import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def train(data, *args, **kwargs):
    """
    Trains a scikit-learn random forest that predicts the column the target variable names
    (target by default) from the numeric and boolean columns, on 80% of the rows, and saves
    it to the path of the model variable (models/model.joblib by default): the Model
    inference template reads it with model set to that path. A text target makes it a
    classifier. Returns its metrics on the other 20%.
    """
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    from sklearn.metrics import accuracy_score, mean_absolute_error, r2_score
    from sklearn.model_selection import train_test_split

    rows = data.collect() if isinstance(data, pl.LazyFrame) else data
    if not isinstance(rows, pl.DataFrame):
        rows = pl.from_pandas(rows)
    target = kwargs.get('target', 'target')
    path = kwargs.get('model', 'models/model.joblib')
    features = [
        name for name, dtype in rows.schema.items()
        if name != target and (dtype.is_numeric() or dtype == pl.Boolean)
    ]
    x = rows.select(features).to_pandas()
    y = rows[target].to_pandas()
    classes = rows.schema[target] in (pl.String, pl.Categorical, pl.Boolean)
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=42)
    model = (RandomForestClassifier if classes else RandomForestRegressor)(
        n_estimators=200, random_state=42, n_jobs=-1,
    ).fit(x_train, y_train)
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    joblib.dump(model, path)
    predicted = model.predict(x_test)
    if classes:
        metrics = dict(accuracy=accuracy_score(y_test, predicted))
    else:
        metrics = dict(
            mae=mean_absolute_error(y_test, predicted), r2=r2_score(y_test, predicted),
        )
    return pl.DataFrame(dict(metric=list(metrics), value=[float(v) for v in metrics.values()]))


@test
def test_the_model_was_saved(output, *args, **kwargs) -> None:
    path = kwargs.get('model', 'models/model.joblib')
    assert os.path.exists(path), f'The model was not saved to {path}'
