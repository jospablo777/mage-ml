import pandas as pd

from mage_ai.pipeline_services.models import load_model, model_metadata

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(features, *args, **kwargs):
    model = load_model('churn', flavor='native')
    version = int(model_metadata('churn')['version'])
    inputs = features[list(model.feature_names_)].astype('float64')
    return pd.DataFrame({
        'driver_id': features['driver_id'].astype('int64'),
        'score': model.predict_proba(inputs)[:, 1],
        'model_version': version,
    })


@test
def test_scores_are_probabilities(output, *args) -> None:
    assert output['score'].between(0, 1).all()
