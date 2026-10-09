import cloudpickle
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load(**kwargs):
    """The champion model, downloaded over HTTP from the artifact proxy."""
    api = f"{kwargs['mlflow_url']}/api/2.0/mlflow"
    version = requests.get(
        f'{api}/registered-models/alias',
        params=dict(name=kwargs['model_name'], alias='champion'),
        timeout=30,
    )
    version.raise_for_status()
    uri = requests.get(
        f'{api}/model-versions/get-download-uri',
        params=dict(name=kwargs['model_name'], version=version.json()['model_version']['version']),
        timeout=30,
    ).json()['artifact_uri']
    path = uri[len('mlflow-artifacts:/'):]
    model = requests.get(
        f"{kwargs['mlflow_url']}/api/2.0/mlflow-artifacts/artifacts/{path}/model.pkl",
        timeout=60,
    )
    model.raise_for_status()
    return cloudpickle.loads(model.content)


@test
def test_is_a_fitted_model(model, **kwargs) -> None:
    assert hasattr(model, 'coef_'), type(model)
