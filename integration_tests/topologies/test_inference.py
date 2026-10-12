"""
Inference pipelines for the compilation study: a Python loader makes N rows of 8 features,
a scoring block in one language and runtime scores them, and a pandas block aggregates the
scores. Run block by block and fused; every scorer must agree with scikit-learn's own
predict_proba.

Two tracks:
- the same logistic model scored in pandas (NumPy), Polars expressions, R (matrix product)
  and Rust (Polars expressions), so the languages do the same arithmetic;
- a gradient boosted tree model (branchy) and a 256x256 MLP (dense) through ONNX: Python
  ONNX Runtime on the CPU and on CUDA, and Rust through mage::onnx, against
  scikit-learn's predict_proba.

The models are trained once, quickly and untuned: the study measures the machine, not the
models. MAGE_STUDY_FULL=1 runs every batch size with MAGE_STUDY_REPEATS repetitions;
MAGE_STUDY_RESULTS_DIR keeps the results (provenance.py). Models train in at most a few seconds and
batches stop at 1,000,000 rows (64 MB of features), within the guards of conftest.py.
"""
import json
import os
import random
import re
import shutil
import textwrap
import uuid
import zlib
from importlib.util import find_spec
from pathlib import Path

import numpy as np
import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

FULL = bool(os.getenv('MAGE_STUDY_FULL'))
REPEATS = int(os.getenv('MAGE_STUDY_REPEATS', '5')) if FULL else 1
SIZES = [1, 1_000, 100_000, 1_000_000] if FULL else [1_000]
FEATURES = [f'f{i}' for i in range(8)]
THREADS = int(os.getenv('MAGE_STUDY_THREADS', '8'))


def cuda_available() -> bool:
    if not find_spec('onnxruntime'):
        return False
    import onnxruntime as ort

    return 'CUDAExecutionProvider' in ort.get_available_providers()


@pytest.fixture(scope='module')
def models(mage_project):
    """Trains the three models once and writes them to the project's models folder."""
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.neural_network import MLPClassifier

    rng = np.random.default_rng(11)
    x = rng.normal(size=(20_000, 8)).astype('float32')
    logit = x[:, 0] * 1.5 - x[:, 1] + 0.5 * x[:, 2] * x[:, 3] + np.sin(x[:, 4])
    y = (logit + rng.normal(scale=0.5, size=len(x)) > 0).astype(int)
    folder = Path(mage_project) / 'models'
    folder.mkdir(exist_ok=True)
    linear = LogisticRegression(max_iter=200).fit(x, y)
    (folder / 'linear.json').write_text(json.dumps(dict(
        weights=[float(w) for w in linear.coef_[0]], bias=float(linear.intercept_[0]))))
    trained = dict(linear=linear)
    trained['gbm'] = GradientBoostingClassifier(n_estimators=100, max_depth=3,
                                                random_state=0).fit(x, y)
    trained['mlp'] = MLPClassifier(hidden_layer_sizes=(256, 256), max_iter=20,
                                   random_state=0).fit(x, y)
    import joblib

    for name in ('gbm', 'mlp'):
        joblib.dump(trained[name], folder / f'{name}.joblib')
    if find_spec('skl2onnx'):
        from skl2onnx import to_onnx

        for name in ('gbm', 'mlp'):
            onnx = to_onnx(trained[name], x[:1], options={id(trained[name]): {'zipmap': False}},
                           target_opset=17)
            (folder / f'{name}.onnx').write_bytes(onnx.SerializeToString())
    return trained


LOADER = '''
import numpy as np
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    rng = np.random.default_rng(5)
    frame = pd.DataFrame(rng.normal(size=({rows}, 8)).astype('float32'),
                         columns=[f'f{{i}}' for i in range(8)])
    frame.insert(0, 'id', np.arange({rows}, dtype='int64'))
    return frame
'''

AGGREGATE = '''
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def aggregate(scores, *args, **kwargs):
    import pandas as pd

    if hasattr(scores, 'to_pandas'):
        scores = scores.to_pandas()
    return pd.DataFrame({'rows': [len(scores)], 'mean_score': [float(scores['score'].mean())],
                         'id_sum': [int(scores['id'].sum())]})
'''

PY_HEADER = '''
import os

import numpy as np
import pandas as pd

from mage_ai.settings.repo import get_repo_path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

FEATURES = [f'f{i}' for i in range(8)]
'''

SCORERS = {
    # name: (Mage language, code)
    'linear_pandas': ('python', PY_HEADER + '''

@transformer
def score(frame, *args, **kwargs):
    import json

    model = json.load(open(os.path.join(get_repo_path(), 'models', 'linear.json')))
    z = frame[FEATURES].to_numpy(dtype='float64') @ np.array(model['weights']) + model['bias']
    return pd.DataFrame({'id': frame['id'], 'score': 1 / (1 + np.exp(-z))})
'''),
    'linear_polars': ('python', PY_HEADER + '''

@transformer
def score(frame, *args, **kwargs):
    import json

    import polars as pl

    model = json.load(open(os.path.join(get_repo_path(), 'models', 'linear.json')))
    frame = frame if isinstance(frame, pl.DataFrame) else pl.from_pandas(frame)
    z = pl.sum_horizontal([pl.col(f).cast(pl.Float64) * w
                           for f, w in zip(FEATURES, model['weights'])]) + model['bias']
    return frame.select('id', (1 / (1 + (-z).exp())).alias('score'))
'''),
    'linear_r': ('r', '''
#* @transformer
score <- function(df_1, ...) {
  model <- jsonlite::fromJSON("models/linear.json")
  x <- as.matrix(df_1[, paste0("f", 0:7)])
  z <- as.vector(x %*% model$weights) + model$bias
  tibble::tibble(id = df_1$id, score = 1 / (1 + exp(-z)))
}
'''),
    'linear_rust': ('rust', '''
use mage::prelude::*;

fn transform(rows: DataFrame) -> Result<DataFrame> {
    let text = std::fs::read_to_string("models/linear.json")?;
    let model: Value = serde_json::from_str(&text)?;
    let weights: Vec<f64> = model["weights"]
        .as_array()
        .ok_or_else(|| anyhow!("no weights"))?
        .iter()
        .map(|w| w.as_f64().unwrap_or(0.0))
        .collect();
    let bias = model["bias"].as_f64().unwrap_or(0.0);
    let n = rows.height();
    let mut z = vec![bias; n];
    for (i, w) in weights.iter().enumerate() {
        let column = rows.column(format!("f{i}").as_str())?.cast(&DataType::Float64)?;
        let mut k = 0;
        for chunk in column.f64()?.downcast_iter() {
            for v in chunk.values().iter() {
                z[k] += w * v;
                k += 1;
            }
        }
    }
    let score: Vec<f64> = z.iter().map(|v| 1.0 / (1.0 + (-v).exp())).collect();
    Ok(DataFrame::new(n, vec![rows.column("id")?.clone(), Column::new("score".into(), score)])?)
}
'''),
}

for model_name in ('gbm', 'mlp'):
    SCORERS[f'{model_name}_sklearn'] = ('python', PY_HEADER + f'''

@transformer
def score(frame, *args, **kwargs):
    import joblib

    model = joblib.load(os.path.join(get_repo_path(), 'models', '{model_name}.joblib'))
    proba = model.predict_proba(frame[FEATURES].to_numpy(dtype='float32'))[:, 1]
    return pd.DataFrame({{'id': frame['id'], 'score': proba}})
''')
    for device in ('cpu', 'cuda'):
        provider = 'CUDAExecutionProvider' if device == 'cuda' else 'CPUExecutionProvider'
        SCORERS[f'{model_name}_ort_{device}'] = ('python', PY_HEADER + f'''

@transformer
def score(frame, *args, **kwargs):
    import onnxruntime as ort

    if hasattr(ort, 'preload_dlls') and '{device}' == 'cuda':
        ort.preload_dlls()
    options = ort.SessionOptions()
    options.intra_op_num_threads = {THREADS}
    session = ort.InferenceSession(
        os.path.join(get_repo_path(), 'models', '{model_name}.onnx'), options,
        providers=['{provider}'],
    )
    assert session.get_providers()[0] == '{provider}', session.get_providers()
    inputs = {{session.get_inputs()[0].name: frame[FEATURES].to_numpy(dtype='float32')}}
    probabilities = session.run(None, inputs)[1]
    return pd.DataFrame({{'id': frame['id'], 'score': probabilities[:, 1]}})
''')
    SCORERS[f'{model_name}_rust_onnx'] = ('rust', f'''
use mage::prelude::*;

fn transform(rows: DataFrame) -> Result<DataFrame> {{
    let model = mage::onnx::model("models/{model_name}.onnx")?;
    let features: Vec<String> = (0..8).map(|i| format!("f{{i}}")).collect();
    let names: Vec<&str> = features.iter().map(|f| f.as_str()).collect();
    let scores = model.predict(&rows, &names)?;
    let score = scores.column("probabilities_1")?.clone().with_name("score".into());
    Ok(DataFrame::new(rows.height(), vec![rows.column("id")?.clone(), score])?)
}}
''')


def enable_onnx(project: str):
    """Turns on the onnx feature of the project's Rust workspace."""
    path = Path(project) / 'rust' / 'Cargo.toml'
    if not path.exists():
        return
    text = path.read_text()
    match = re.search(r'(mage-block = \{[^}]*features = \[)([^\]]*)\]', text)
    if match and '"onnx"' not in match.group(2):
        items = [i.strip() for i in match.group(2).split(',') if i.strip()] + ['"onnx"']
        text = text[:match.start(2)] + ', '.join(items) + text[match.end(2):]
        path.write_text(text)


def build(project, scorer, rows):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    language, code = SCORERS[scorer]
    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'infer_{suffix}', repo_path=project)
    blocks = []
    for name, block_type, lang, text in [
        ('features', 'data_loader', 'python', LOADER.format(rows=rows)),
        (f'score_{scorer}', 'transformer', language, code),
        ('summary', 'transformer', 'python', AGGREGATE),
    ]:
        block = Block.create(f'{name}_{suffix}', block_type, project, language=lang)
        Path(block.file_path).write_text(textwrap.dedent(text).strip() + '\n')
        if lang == 'rust':
            enable_onnx(project)
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in blocks[-1:]])
        blocks.append(block)
    return Pipeline.get(pipeline.uuid, repo_path=project), [b.uuid for b in blocks]


@pytest.fixture
def scorer_ready(monkeypatch, tmp_path):
    def check(scorer):
        language, _ = SCORERS[scorer]
        if language == 'rust' and shutil.which('cargo') is None:
            pytest.skip('Rust is not installed')
        if language == 'r':
            path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
            if not path or shutil.which('Rscript') is None:
                pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
            monkeypatch.setenv('MAGE_R_PROJECT_DIR', path)
            monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))
        if ('_ort_' in scorer or 'rust_onnx' in scorer) and not find_spec('skl2onnx'):
            pytest.skip('skl2onnx is not installed: no ONNX models')
        if '_ort_' in scorer and not find_spec('onnxruntime'):
            pytest.skip('onnxruntime is not installed')
        if scorer.endswith('_cuda') and not cuda_available():
            pytest.skip('ONNX Runtime has no CUDA provider here')
    return check


# By default only the scorers the dev environment always has, so make ci never skips: the
# ONNX scorers need skl2onnx and onnxruntime (or onnxruntime-gpu for CUDA).
RUN = list(SCORERS) if FULL else [s for s in SCORERS if '_ort_' not in s and 'onnx' not in s]


@pytest.mark.parametrize('rows', SIZES)
@pytest.mark.parametrize('scorer', RUN)
def test_inference(mage_project, models, scorer_ready, scorer, rows):
    scorer_ready(scorer)
    pipeline, uuids = build(mage_project, scorer, rows)
    model = models[scorer.split('_')[0]]
    x = np.random.default_rng(5).normal(size=(rows, 8)).astype('float32')
    expected = float(model.predict_proba(x)[:, 1].mean())
    order = random.Random(zlib.crc32(f'{scorer}|{rows}'.encode()))
    for repeat in range(REPEATS + 1):
        modes = ['off', 'chains']
        order.shuffle(modes)
        for position, mode in enumerate(modes):
            before = provenance.wait_quiet()
            pipeline_run, seconds = lib.run(pipeline, mode)
            states = lib.statuses(pipeline_run)
            entry = dict(scorer=scorer, rows=rows, mode=mode, repeat=repeat,
                         position=position, cold=repeat == 0, seconds=round(seconds, 4),
                         pipeline=pipeline.uuid,
                         block_seconds=lib.block_seconds(pipeline_run), statuses=states)
            problem = None
            if any(s != 'completed' for s in states.values()):
                problem = f'statuses {states}'
            else:
                summary = lib_summary(pipeline, uuids[2], pipeline_run)
                entry['mean_score'] = summary['mean_score']
                if summary['rows'] != rows:
                    problem = f"{summary['rows']} rows, expected {rows}"
                elif abs(summary['mean_score'] - expected) > 1e-4:
                    problem = f"mean score {summary['mean_score']} != {expected}"
            entry['problems'] = {'summary': problem} if problem else {}
            provenance.record('inference', entry, before)
            assert problem is None, problem


def lib_summary(pipeline, block_uuid, pipeline_run) -> dict:
    frame = lib.stored(pipeline, block_uuid, pipeline_run)
    if hasattr(frame, 'to_pandas') and not hasattr(frame, 'iloc'):
        frame = frame.to_pandas()
    return dict(rows=int(frame['rows'].iloc[0]), mean_score=float(frame['mean_score'].iloc[0]))
