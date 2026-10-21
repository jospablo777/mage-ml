"""
ML models in blocks, the same code in Mage and in an exported pipeline service.

    from mage_ai.pipeline_services.models import load_model, model_path

    model = load_model('churn', uri='models:/churn/3')     # an MLflow pyfunc model
    model = load_model('churn', uri='models:/churn/3', flavor='native')  # CatBoost, sklearn
    path = model_path('churn', uri='models:/churn/3')      # its local directory
    service = feature_service('churn', uri='models:/churn/3')  # its Feast feature service
    scores = predict('churn', features, uri='models:/churn/3', key='customer_id')

In Mage, the model comes from MLflow (`MLFLOW_TRACKING_URI`) and is downloaded once per
process. In a service, `mage export service` has put it in the image, with its metadata,
and the same call returns the embedded copy: the service needs no MLflow server and runs
the model version it was exported with.

URIs: `models:/<name>/<version>`, `models:/<name>@<alias>`, `runs:/<run id>/<path>`, or
any artifact URI MLflow can download.

`predict` scores with the model and, in a service whose definition gives the model
challengers, also scores a sample of the inputs with each challenger in the background:
the caller gets the champion's output only, as soon as it has it, and the challengers'
outputs go to shadow records next to the champion's, by input key, for the comparison.
"""
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

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

            from mage_ai.pipeline_services.model_export import download_uri

            _downloaded[uri] = mlflow.artifacts.download_artifacts(
                artifact_uri=download_uri(uri),
            )
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


def load_model(name: str, uri: Optional[str] = None, flavor: Optional[str] = None):
    """
    The model, loaded once per process. flavor: None loads MLflow's pyfunc flavor, which
    has predict; 'native' loads the framework's own object, such as a CatBoostClassifier
    with predict_proba; or a flavor by name, such as 'catboost' or 'sklearn'.
    """
    key = f'{name}|{uri}|{flavor}'
    with _lock:
        if key in _loaded:
            return _loaded[key]
    model = _load_path(model_path(name, uri), flavor)
    with _lock:
        _loaded[key] = model
    return model


def _load_path(path: str, flavor: Optional[str]):
    if flavor == 'native' and not (Path(path) / 'MLmodel').is_file():
        # A model file logged as is, such as model.cbm: read in its own format.
        model = load_artifact(_model_file(path))
    else:
        chosen = _native_flavor(path) if flavor == 'native' else flavor
        import importlib

        module = importlib.import_module(f'mlflow.{chosen or "pyfunc"}')
        model = module.load_model(path)
    return model


def _model_file(path: str) -> str:
    """The one file in a model's artifacts whose suffix names its format."""
    root = Path(path)
    files = [root] if root.is_file() else sorted(
        p for p in root.rglob('*') if p.is_file() and p.suffix.lower() in ARTIFACT_LOADERS
    )
    if len(files) != 1:
        found = ', '.join(str(p.relative_to(root)) for p in files) or 'none'
        raise ModelNotFound(
            f'The model at {path} has no MLmodel file and {len(files)} model files ({found}); '
            'load one with load_artifact(model_path(name) + "/<file>").'
        )
    return str(files[0])


def _native_flavor(path: str) -> str:
    """The framework flavor the model was logged with, from its MLmodel file."""
    import yaml

    mlmodel = Path(path) / 'MLmodel'
    flavors = []
    if mlmodel.is_file():
        flavors = list((yaml.safe_load(mlmodel.read_text()) or {}).get('flavors') or {})
    native = [f for f in flavors if f != 'python_function']
    if not native:
        raise ModelNotFound(
            f'The model at {path} has no framework flavor, only {flavors or "none"}; load it '
            'without flavor= to use pyfunc.'
        )
    return native[0]


# The tag Feast's MLflow integration puts on a model version and its training run.
FEATURE_SERVICE_TAG = 'feast.feature_service'


def feature_service(name: str, uri: Optional[str] = None) -> Optional[str]:
    """
    The Feast feature service the model was trained with, from its feast.feature_service
    tag (the model version's, then its training run's), or None. Blocks pass it to
    FeastClient.get_online_features, so the features follow the model when its alias
    moves to a version trained on another feature service.
    """
    metadata = model_metadata(name, uri)
    return (metadata.get('tags') or {}).get(FEATURE_SERVICE_TAG) or \
        (metadata.get('run_tags') or {}).get(FEATURE_SERVICE_TAG)


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
    'torch_export': 'torch', 'torchscript': 'torch', 'keras': 'keras', 'joblib': 'joblib',
    'mlflow': 'mlflow',
}
_artifacts: Dict[Any, Any] = {}


def _read(loader: str, path: str, options: Dict) -> Any:
    if loader == 'onnx':
        import onnxruntime

        return onnxruntime.InferenceSession(
            path, providers=options.get('providers') or ['CPUExecutionProvider'],
        )
    if loader == 'catboost':
        return _read_catboost(path)
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


# CatBoost losses by the class that trains with them; a model with another loss keeps the
# generic CatBoost.
CATBOOST_CLASSIFIER_LOSSES = frozenset((
    'Logloss', 'CrossEntropy', 'MultiClass', 'MultiClassOneVsAll', 'MultiLogloss',
    'MultiCrossEntropy',
))
CATBOOST_REGRESSOR_LOSSES = frozenset((
    'RMSE', 'MAE', 'Quantile', 'MultiQuantile', 'LogLinQuantile', 'MAPE', 'Poisson', 'Tweedie',
    'Huber', 'Expectile', 'Lq', 'LogCosh', 'RMSEWithUncertainty', 'MultiRMSE', 'SurvivalAft',
))
CATBOOST_RANKER_LOSSES = frozenset((
    'YetiRank', 'YetiRankPairwise', 'PairLogit', 'PairLogitPairwise', 'QueryRMSE',
    'QuerySoftMax', 'StochasticRank', 'LambdaMart', 'QueryCrossEntropy',
))


def _read_catboost(path: str):
    """
    A .cbm file as the class it was saved from: the generic CatBoost returns raw scores from
    predict and has no predict_proba.
    """
    import catboost

    model = catboost.CatBoost().load_model(path)
    loss = str((model.get_all_params() or {}).get('loss_function') or '').split(':')[0]
    for losses, model_class in (
        (CATBOOST_CLASSIFIER_LOSSES, catboost.CatBoostClassifier),
        (CATBOOST_REGRESSOR_LOSSES, catboost.CatBoostRegressor),
        (CATBOOST_RANKER_LOSSES, catboost.CatBoostRanker),
    ):
        if loss in losses:
            return model_class().load_model(path)
    return model


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


# ---- champion and challengers -------------------------------------------------------------

SHADOW_DIR_ENV = 'MAGE_SERVICE_SHADOW_DIR'
CHALLENGERS_FILE = 'challengers.json'
# Calls waiting for the challengers; past this, a call is dropped and counted, so a slow
# challenger never holds the champion's path.
SHADOW_QUEUE = int(os.environ.get('MAGE_SERVICE_SHADOW_QUEUE') or 100)

_shadow_queue = None
_shadow_lock = threading.Lock()
_write_lock = threading.Lock()


def challengers(name: str) -> Dict:
    """The model's challengers in this service and the share of inputs they score."""
    embedded = _embedded(name)
    if embedded is None or not (embedded / CHALLENGERS_FILE).is_file():
        return dict(sample=0.0, challengers=[])
    return json.loads((embedded / CHALLENGERS_FILE).read_text())


def _challenger(name: str, label: str, flavor: Optional[str]):
    key = f'{name}|challenger:{label}|{flavor}'
    with _lock:
        if key in _loaded:
            return _loaded[key]
    path = str(_embedded(name) / 'challengers' / label / 'artifacts')
    model = _load_path(path, flavor)
    with _lock:
        _loaded[key] = model
    return model


def _call(model, method: str, data):
    return getattr(model, method)(data)


def _sampled(key, sample: float) -> bool:
    """The same input key is always in or out of the sample."""
    if sample >= 1:
        return True
    if sample <= 0:
        return False
    import hashlib

    digest = hashlib.sha256(str(key).encode()).digest()
    return int.from_bytes(digest[:4], 'big') / 2**32 < sample


def _keys(data, key: Optional[str]) -> List:
    if key is None:
        return list(range(_length(data)))
    if hasattr(data, 'columns'):
        column = data[key]
        return list(column.to_list() if hasattr(column, 'to_list') else column)
    return [row[key] for row in data]


def _length(data) -> int:
    shape = getattr(data, 'shape', None)
    return int(shape[0]) if shape else len(data)


def _take(data, positions: List[int]):
    if hasattr(data, 'iloc'):
        return data.iloc[positions].copy()
    if hasattr(data, 'filter') and hasattr(data, 'with_row_index'):  # Polars
        return data[positions]
    if hasattr(data, 'shape'):  # NumPy
        return data[positions]
    return [data[p] for p in positions]


def _plain(value):
    if hasattr(value, 'item') and getattr(value, 'shape', None) == ():
        value = value.item()
    if isinstance(value, float):
        return round(value, 9)
    if hasattr(value, 'tolist'):
        return _plain(value.tolist())
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    return value


def _features(data, features: Optional[List[str]]):
    """The columns the model reads, when the data has others, such as the key."""
    return data[features] if features else data


def _rows(output, count: int) -> List:
    """One JSON value per input row: a label, a score, or a row of scores."""
    if hasattr(output, 'to_dict') and hasattr(output, 'columns'):  # pandas or Polars frame
        records = (output.to_dict('records') if hasattr(output, 'iloc')
                   else output.to_dicts())
        rows = [r[next(iter(r))] if len(r) == 1 else r for r in records]
    elif hasattr(output, 'tolist'):
        rows = output.tolist()
    else:
        rows = list(output)
    if len(rows) != count:
        raise ValueError(f'the output has {len(rows)} rows for {count} inputs')
    return [_plain(r) for r in rows]


def _write(name: str, label: str, records: List[Dict]) -> None:
    from datetime import datetime, timezone

    folder = Path(os.environ[SHADOW_DIR_ENV]) / name / label
    folder.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    lines = ''.join(json.dumps(r, default=str) + '\n' for r in records)
    with _write_lock:
        with open(folder / f'{day}.jsonl', 'a', encoding='utf-8') as file:
            file.write(lines)


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


def _shadow_worker(work):
    while True:
        job = work.get()
        try:
            _score_challengers(**job)
        except Exception:  # never stops the thread: each call records its own errors
            pass
        finally:
            work.task_done()


def _score_challengers(name, settings, flavor, method, data, keys, champion, champion_ms,
                       champion_version):
    for challenger in settings['challengers']:
        label = challenger['name']
        record = dict(kind='call', at=_now(), rows=len(keys), champion_ms=champion_ms,
                      champion_version=champion_version,
                      challenger_version=challenger.get('version'), error=None)
        rows = []
        try:
            import time

            model = _challenger(name, label, flavor)
            started = time.perf_counter()
            output = _call(model, method, data)
            record['challenger_ms'] = round((time.perf_counter() - started) * 1000, 3)
            for key, mine, theirs in zip(keys, champion, _rows(output, len(keys))):
                rows.append(dict(kind='row', at=record['at'], key=key, champion=mine,
                                 challenger=theirs))
        except Exception as error:
            record['error'] = f'{type(error).__name__}: {error}'
        _write(name, label, [record, *rows])


def predict(name: str, data, uri: Optional[str] = None, key: Optional[str] = None,
            flavor: Optional[str] = None, method: str = 'predict',
            features: Optional[List[str]] = None):
    """
    The model's output for data (`method`: predict, or predict_proba with flavor='native');
    `features`, when given, are the columns of data the model reads.
    In a service whose definition gives the model challengers, a sample of the inputs (by
    `key`, a column or field of data; the same key is always sampled or not) is also scored
    by each challenger, in a background thread: the champion's output returns at once, and
    the challengers' outputs are written as shadow records for the comparison. A challenger
    that fails, or a full queue, is recorded and never reaches the caller.
    """
    import time

    model = load_model(name, uri, flavor)
    started = time.perf_counter()
    output = _call(model, method, _features(data, features))
    champion_ms = round((time.perf_counter() - started) * 1000, 3)
    settings = challengers(name)
    if not settings['challengers'] or not os.environ.get(SHADOW_DIR_ENV):
        return output
    try:
        keys = _keys(data, key)
        positions = [i for i, k in enumerate(keys) if _sampled(k, settings['sample'])]
        if not positions:
            return output
        champion = _rows(output, len(keys))
        job = dict(
            name=name, settings=settings, flavor=flavor, method=method,
            data=_features(_take(data, positions), features),
            keys=[_plain(keys[p]) for p in positions],
            champion=[champion[p] for p in positions], champion_ms=champion_ms,
            champion_version=(model_metadata(name).get('version')
                              if _embedded(name) is not None else None),
        )
    except Exception as error:
        for challenger in settings['challengers']:
            _write(name, challenger['name'], [dict(kind='call', at=_now(), rows=0,
                                                   error=f'{type(error).__name__}: {error}')])
        return output
    global _shadow_queue
    with _shadow_lock:
        if _shadow_queue is None:
            import queue

            _shadow_queue = queue.Queue(maxsize=SHADOW_QUEUE)
            threading.Thread(target=_shadow_worker, args=(_shadow_queue,), daemon=True,
                             name='mage-challengers').start()
    try:
        _shadow_queue.put_nowait(job)
    except Exception:
        for challenger in settings['challengers']:
            _write(name, challenger['name'], [dict(
                kind='call', at=_now(), rows=len(job['keys']), dropped=True,
                error='dropped: the challengers were behind by a full queue',
            )])
    return output


def wait_for_challengers(timeout: float = 30) -> None:
    """Waits until the challengers scored every queued call (tests, shutdown)."""
    import time

    deadline = time.monotonic() + timeout
    while _shadow_queue is not None and time.monotonic() < deadline:
        if _shadow_queue.unfinished_tasks == 0:
            return
        time.sleep(0.01)
