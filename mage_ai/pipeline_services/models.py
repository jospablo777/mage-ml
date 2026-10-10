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
