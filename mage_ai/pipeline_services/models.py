"""
ML models in blocks, the same code in Mage and in an exported pipeline service.

    from mage_ai.pipeline_services.models import load_model, model_path

    model = load_model('fraud', uri='models:/fraud/3')     # an MLflow pyfunc model
    path = model_path('fraud', uri='models:/fraud/3')      # its local directory

In Mage, the model comes from MLflow (`MLFLOW_TRACKING_URI`) and is downloaded once per
process. In a service, `mage export service` has put it in the image, with its metadata,
and the same call returns the embedded copy: the service needs no MLflow server and runs
the model version it was exported with.

URIs: `models:/<name>/<version>`, `models:/<name>@<alias>`, `runs:/<run id>/<path>`, or
any artifact URI MLflow can download.
"""
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, Optional

MODELS_DIR_ENV = 'MAGE_SERVICE_MODELS_DIR'
METADATA_FILE = 'mage-model.json'

_lock = threading.Lock()
_downloaded: Dict[str, str] = {}
_loaded: Dict[str, Any] = {}


class ModelNotFound(Exception):
    pass


def _embedded(name: str) -> Optional[Path]:
    directory = os.environ.get(MODELS_DIR_ENV)
    if not directory:
        return None
    path = Path(directory) / name
    return path if path.is_dir() else None


def model_path(name: str, uri: Optional[str] = None) -> str:
    """The local directory of the model's files."""
    embedded = _embedded(name)
    if embedded is not None:
        return str(embedded / 'artifacts')
    if os.environ.get('MAGE_SERVICE'):
        raise ModelNotFound(
            f'The model {name!r} is not in this service. Export the pipeline again with '
            f'--model {name}=<uri>, or call load_model({name!r}, uri=...) with a literal uri '
            'so the export finds it.'
        )
    if not uri:
        raise ModelNotFound(f'The model {name!r} needs a uri, such as models:/{name}/1.')
    with _lock:
        if uri not in _downloaded:
            import mlflow

            _downloaded[uri] = mlflow.artifacts.download_artifacts(artifact_uri=uri)
        return _downloaded[uri]


def model_metadata(name: str, uri: Optional[str] = None) -> Dict:
    """Version, run, flavors, signature, params and metrics, as recorded at export."""
    embedded = _embedded(name)
    if embedded is not None:
        return json.loads((embedded / METADATA_FILE).read_text())
    if not uri:
        raise ModelNotFound(f'The model {name!r} needs a uri.')
    from mage_ai.pipeline_services.model_export import describe

    return describe(name, uri)


def load_model(name: str, uri: Optional[str] = None):
    """The model loaded with MLflow's pyfunc flavor; loaded once per process."""
    key = f'{name}|{uri}'
    with _lock:
        if key in _loaded:
            return _loaded[key]
    path = model_path(name, uri)
    import mlflow.pyfunc

    model = mlflow.pyfunc.load_model(path)
    with _lock:
        _loaded[key] = model
    return model


# Model files by suffix: the loader that reads them and the module it imports. A suffix
# several frameworks use (.json, .txt, .pt) needs loader=.
ARTIFACT_LOADERS = {
    '.onnx': ('onnx', 'onnxruntime'),
    '.cbm': ('catboost', 'catboost'),
    '.ubj': ('xgboost', 'xgboost'),
    '.skops': ('skops', 'skops'),
    '.safetensors': ('safetensors', 'safetensors'),
    '.pt2': ('torch_export', 'torch'),
    '.keras': ('keras', 'keras'),
    '.joblib': ('joblib', 'joblib'),
    '.pkl': ('pickle', None),
    '.pickle': ('pickle', None),
}
LOADER_MODULES = {
    'onnx': 'onnxruntime', 'catboost': 'catboost', 'xgboost': 'xgboost',
    'lightgbm': 'lightgbm', 'skops': 'skops', 'safetensors': 'safetensors',
    'torch_export': 'torch', 'torchscript': 'torch', 'keras': 'keras', 'joblib': 'joblib', 'mlflow': 'mlflow',
}
_artifacts: Dict[Any, Any] = {}


def _read(loader: str, path: str, options: Dict) -> Any:
    if loader == 'onnx':
        import onnxruntime

        return onnxruntime.InferenceSession(
            path, providers=options.get('providers') or ['CPUExecutionProvider'],
        )
    if loader == 'catboost':
        import catboost

        return catboost.CatBoost().load_model(path)
    if loader == 'xgboost':
        import xgboost

        booster = xgboost.Booster()
        booster.load_model(path)
        return booster
    if loader == 'lightgbm':
        import lightgbm

        return lightgbm.Booster(model_file=path)
    if loader == 'skops':
        import skops.io

        trusted = options.get('trusted')
        try:
            return skops.io.load(path, trusted=trusted)
        except Exception as error:
            if 'untrusted' not in str(error).lower():
                raise
            types = skops.io.get_untrusted_types(file=path)
            raise ValueError(
                f'{path} holds types skops does not trust by default: {", ".join(types)}. '
                'Check them, then pass them: load_artifact(path, trusted=[...]).'
            ) from error
    if loader == 'safetensors':
        from safetensors.torch import load_file

        return load_file(path)
    if loader == 'torch_export':
        import torch

        # A program saved with torch.export.save; .module() is the callable model.
        return torch.export.load(path).module()
    if loader == 'torchscript':
        import torch

        return torch.jit.load(path, map_location=options.get('map_location', 'cpu'))
    if loader == 'keras':
        import keras

        return keras.models.load_model(path)
    if loader == 'mlflow':
        import mlflow.pyfunc

        return mlflow.pyfunc.load_model(path)
    if loader == 'joblib':
        import joblib

        return joblib.load(path)
    if loader == 'pickle':
        import pickle

        with open(path, 'rb') as file:
            return pickle.load(file)
    raise ValueError(f'Unknown loader {loader!r}; use one of {", ".join(sorted(LOADER_MODULES))}, '
                     'pickle, or a function that takes the path.')


def artifact_path(path: str) -> str:
    """A model file's absolute path: relative paths start at the project, here and in a service."""
    if os.path.isabs(path):
        return path
    from mage_ai.settings.repo import get_repo_path

    return os.path.join(get_repo_path(), path)


def load_artifact(path: str, loader: Any = None, **options):
    """
    A model saved in its framework's own format, loaded once per process and reused: an
    exported service loads it on the first request, not on every one. A changed file is
    loaded again.

        model = load_artifact('models/churn.cbm')                 # CatBoost
        model = load_artifact('models/churn.onnx')                # an ONNX Runtime session
        model = load_artifact('models/booster.txt', loader='lightgbm')
        model = load_artifact('models/forecaster.pt', loader=NBEATSModel.load)  # Darts

    loader picks the format when the suffix does not (a name in LOADER_MODULES, 'pickle',
    or a function that takes the path). Pickle and joblib run code from the file when they
    load it: use them only for files you made.
    """
    absolute = artifact_path(path)
    if not os.path.exists(absolute):
        raise ModelNotFound(
            f'No model file at {absolute}. Paths are relative to the project; an exported '
            'service includes the files its blocks name.'
        )
    if loader is None:
        suffix = Path(absolute).suffix.lower()
        if os.path.isdir(absolute) and os.path.exists(os.path.join(absolute, 'MLmodel')):
            loader = 'mlflow'
        elif suffix in ARTIFACT_LOADERS:
            loader = ARTIFACT_LOADERS[suffix][0]
        else:
            raise ValueError(
                f'{path}: the suffix does not say which framework reads it; pass loader=, '
                f'one of {", ".join(sorted(LOADER_MODULES))}, or a function that takes the path.'
            )
    key = (absolute, os.path.getmtime(absolute), loader if isinstance(loader, str) else id(loader),
           tuple(sorted((k, repr(v)) for k, v in options.items())))
    with _lock:
        if key in _artifacts:
            return _artifacts[key]
    model = loader(absolute) if callable(loader) else _read(loader, absolute, options)
    with _lock:
        _artifacts[key] = model
    return model
