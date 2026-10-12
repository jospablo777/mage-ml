"""
Model scoring in Python, R and Rust for the compilation study: the same models, the same
rows, scored in each language's runtime inside MageML pipelines. Measures time, GPU energy,
agreement with the Python reference and determinism.

Data: a base table of synthetic customers made with Faker (seed 7, 20,000 rows, 16 numeric
features) and a label from a fixed rule plus noise. The loader block resamples it to the
batch size with a fixed seed, so every scorer gets the same rows. Making 1,000,000 rows with
Faker would take minutes; resampling keeps the batches cheap and identical.

Models, trained once in Python and not tuned (the study measures the machine, not the
models):
- xgb: XGBoost, 200 trees of depth 6, saved as JSON (Python, R) and ONNX (Rust, ONNX Runtime);
- cat: CatBoost, 200 iterations of depth 6, saved as .cbm (Python, R) and ONNX;
- mlp: a torch MLP 16-512-512-1, saved as TorchScript (Python, R) and ONNX.

Every scorer works in chunks of 65,536 rows and uses 8 threads, so memory stays bounded
and the languages get the same share of the machine.

Runs only with MAGE_STUDY_FULL=1 (conftest.py leaves it out otherwise): it needs xgboost,
catboost, torch, onnxmltools, onnxruntime-gpu and nvidia-ml-py in Python and the R
environment integration_tests/topologies/r_ml.
"""
import hashlib
import os
import random
import shutil
import textwrap
import time
import uuid
import zlib
from pathlib import Path

import numpy as np
import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

REPEATS = int(os.getenv('MAGE_STUDY_REPEATS', '3'))
SIZES = [int(s) for s in os.getenv('MAGE_STUDY_ML_SIZES', '1000,100000,1000000').split(',')]
FEATURES = [
    'age', 'tenure_months', 'monthly_spend', 'support_tickets', 'city_code', 'plan_code',
    'days_since_signup', 'days_since_login', 'devices', 'late_payments', 'discount',
    'satisfaction', 'orders', 'returns', 'referrals', 'sessions',
]
CHUNK = 65_536
THREADS = int(os.getenv('MAGE_STUDY_THREADS', '8'))
R_ML = Path(__file__).resolve().parent / 'r_ml'


def faker_base(rows: int = 20_000, seed: int = 7):
    """The synthetic customers: 16 numeric features and a churn label."""
    import pandas as pd
    from faker import Faker

    fake = Faker()
    fake.seed_instance(seed)
    cities = [fake.city() for _ in range(40)]
    plans = ['basic', 'plus', 'pro', 'team']
    records = []
    for _ in range(rows):
        records.append(dict(
            age=fake.pyint(18, 90), tenure_months=fake.pyint(0, 120),
            monthly_spend=fake.pyfloat(min_value=0, max_value=500, right_digits=2),
            support_tickets=fake.pyint(0, 20),
            city_code=cities.index(fake.random_element(cities)),
            plan_code=plans.index(fake.random_element(plans)),
            days_since_signup=(fake.date_between('-6y', 'today') - fake.date_between(
                '-6y', '-6y')).days,
            days_since_login=fake.pyint(0, 365), devices=fake.pyint(1, 6),
            late_payments=fake.pyint(0, 12),
            discount=fake.pyfloat(min_value=0, max_value=0.5, right_digits=3),
            satisfaction=fake.pyint(1, 10), orders=fake.pyint(0, 300),
            returns=fake.pyint(0, 40), referrals=fake.pyint(0, 15), sessions=fake.pyint(0, 900),
        ))
    frame = pd.DataFrame(records).astype('float32')
    rng = random.Random(seed)
    score = (0.03 * frame['support_tickets'] - 0.02 * frame['tenure_months']
             + 0.004 * frame['days_since_login'] - 0.15 * frame['satisfaction']
             + 0.2 * frame['late_payments'] - 2.0 * frame['discount'])
    noise = np.array([rng.gauss(0, 0.5) for _ in range(rows)])
    frame['churned'] = ((score + noise) > score.median()).astype('int32')
    return frame


def resample(base, rows: int):
    rng = np.random.default_rng(13)
    frame = base[FEATURES].iloc[rng.integers(0, len(base), size=rows)].reset_index(drop=True)
    frame.insert(0, 'id', np.arange(rows, dtype='int64'))
    return frame


def mlp_module(scale):
    """The MLP 16-512-512-1 with the feature scale inside, so every runtime gets raw rows."""
    import torch

    class Scale(torch.nn.Module):
        def __init__(self):
            super().__init__()
            self.register_buffer('scale', torch.tensor(scale, dtype=torch.float32))

        def forward(self, x):
            return x / self.scale

    torch.manual_seed(0)
    return torch.nn.Sequential(
        Scale(), torch.nn.Linear(16, 512), torch.nn.ReLU(), torch.nn.Linear(512, 512),
        torch.nn.ReLU(), torch.nn.Linear(512, 1), torch.nn.Sigmoid(),
    )


@pytest.fixture(scope='module')
def ml_project(mage_project):
    """Writes the base table and the three models to the project; returns references."""
    import catboost
    import onnxmltools
    import torch
    import xgboost
    from onnxmltools.convert.common.data_types import FloatTensorType

    root = Path(mage_project)
    (root / 'data').mkdir(exist_ok=True)
    (root / 'models').mkdir(exist_ok=True)
    started = time.perf_counter()
    base = faker_base()
    faker_seconds = time.perf_counter() - started
    base.to_parquet(root / 'data' / 'ml_base.parquet')
    x, y = base[FEATURES].to_numpy(), base['churned'].to_numpy()

    xgb = xgboost.XGBClassifier(n_estimators=200, max_depth=6, tree_method='hist',
                                n_jobs=THREADS, random_state=0).fit(x, y)
    xgb.get_booster().save_model(str(root / 'models' / 'xgb.json'))
    onnx = onnxmltools.convert_xgboost(xgb, initial_types=[('input', FloatTensorType([None, 16]))],
                                       target_opset=15)
    (root / 'models' / 'xgb.onnx').write_bytes(onnx.SerializeToString())

    cat = catboost.CatBoostClassifier(iterations=200, depth=6, thread_count=THREADS,
                                      random_seed=0, verbose=False,
                                      allow_writing_files=False).fit(x, y)
    cat.save_model(str(root / 'models' / 'cat.cbm'))
    cat.save_model(str(root / 'models' / 'cat.onnx'), format='onnx')
    drop_zipmap(root / 'models' / 'cat.onnx')

    mlp = mlp_module(x.max(axis=0).clip(min=1))
    optimizer = torch.optim.Adam(mlp.parameters(), lr=1e-3)
    tx = torch.tensor(x, dtype=torch.float32)
    ty = torch.tensor(y, dtype=torch.float32)
    for _ in range(3):
        optimizer.zero_grad()
        loss = torch.nn.functional.binary_cross_entropy(mlp(tx).squeeze(1), ty)
        loss.backward()
        optimizer.step()
    scaled = mlp.eval()
    traced = torch.jit.trace(scaled, torch.zeros(4, 16))
    traced.save(str(root / 'models' / 'mlp.pt'))
    torch.onnx.export(scaled, torch.zeros(4, 16), str(root / 'models' / 'mlp.onnx'),
                      input_names=['input'], output_names=['score'],
                      dynamic_axes={'input': {0: 'rows'}, 'score': {0: 'rows'}},
                      opset_version=17, dynamo=False)
    return dict(base=base, xgb=xgb, cat=cat, mlp=traced, root=root,
                faker_seconds=round(faker_seconds, 2))


def drop_zipmap(path: Path):
    """CatBoost's ONNX ends in a ZipMap: probabilities as a list of {class: value} maps,
    which only Python's ONNX Runtime reads. Removing it leaves the probability tensor."""
    import onnx

    model = onnx.load(str(path))
    graph = model.graph
    zipmaps = [n for n in graph.node if n.op_type == 'ZipMap']
    for node in zipmaps:
        source, target = node.input[0], node.output[0]
        graph.node.remove(node)
        for output in list(graph.output):
            if output.name == target:
                graph.output.remove(output)
                graph.output.append(onnx.helper.make_tensor_value_info(
                    source, onnx.TensorProto.FLOAT, ['N', 2]))
    onnx.checker.check_model(model)
    onnx.save(model, str(path))


def reference(models, model: str, rows: int):
    """Python's own scores for the batch: XGBoost, CatBoost, and torch on the CPU."""
    import torch

    x = resample(models['base'], rows)[FEATURES].to_numpy(dtype='float32')
    if model == 'xgb':
        return models['xgb'].predict_proba(x)[:, 1].astype('float64')
    if model == 'cat':
        return models['cat'].predict_proba(x)[:, 1].astype('float64')
    with torch.inference_mode():
        return np.concatenate([
            models['mlp'](torch.from_numpy(x[i:i + CHUNK])).numpy()[:, 0]
            for i in range(0, len(x), CHUNK)
        ]).astype('float64')


LOADER = '''
import os

import numpy as np
import pandas as pd

from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

FEATURES = {features!r}


@data_loader
def load(*args, **kwargs):
    base = pd.read_parquet(os.path.join(get_repo_path(), 'data', 'ml_base.parquet'))
    rng = np.random.default_rng(13)
    frame = base[FEATURES].iloc[rng.integers(0, len(base), size={rows})].reset_index(drop=True)
    frame.insert(0, 'id', np.arange({rows}, dtype='int64'))
    return frame
'''

PY = '''
import os

import numpy as np
import pandas as pd

from mage_ai.settings.repo import get_repo_path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

FEATURES = {features!r}
CHUNK = {chunk}
THREADS = {threads}


def model_path(name):
    return os.path.join(get_repo_path(), 'models', name)


@transformer
def score(frame, *args, **kwargs):
    x = frame[FEATURES].to_numpy(dtype='float32')
    parts = []
{body}
    return pd.DataFrame({{'id': frame['id'], 'score': np.concatenate(parts).astype('float64')}})
'''

PY_BODIES = {
    'xgb_py': '''
    import xgboost
    booster = xgboost.Booster(model_file=model_path('xgb.json'))
    booster.set_param({'nthread': THREADS})
    for i in range(0, len(x), CHUNK):
        parts.append(booster.predict(xgboost.DMatrix(x[i:i + CHUNK])))''',
    'cat_py': '''
    import catboost
    model = catboost.CatBoostClassifier().load_model(model_path('cat.cbm'))
    for i in range(0, len(x), CHUNK):
        parts.append(model.predict_proba(x[i:i + CHUNK], thread_count=THREADS)[:, 1])''',
    'mlp_py_cpu': '''
    import torch
    torch.set_num_threads(THREADS)
    model = torch.jit.load(model_path('mlp.pt'), map_location='cpu')
    with torch.inference_mode():
        for i in range(0, len(x), CHUNK):
            parts.append(model(torch.from_numpy(x[i:i + CHUNK])).numpy()[:, 0])''',
    'mlp_py_cuda': '''
    import torch
    model = torch.jit.load(model_path('mlp.pt'), map_location='cuda')
    with torch.inference_mode():
        for i in range(0, len(x), CHUNK):
            out = model(torch.from_numpy(x[i:i + CHUNK]).to('cuda'))
            parts.append(out.cpu().numpy()[:, 0])''',
}
for _model in ('xgb', 'cat', 'mlp'):
    for _device in ('cpu', 'cuda'):
        if _device == 'cuda' and _model != 'mlp':
            continue
        _provider = 'CUDAExecutionProvider' if _device == 'cuda' else 'CPUExecutionProvider'
        PY_BODIES[f'{_model}_ort_{_device}'] = f'''
    import onnxruntime as ort
    if '{_device}' == 'cuda' and hasattr(ort, 'preload_dlls'):
        ort.preload_dlls()
    options = ort.SessionOptions()
    options.intra_op_num_threads = THREADS
    session = ort.InferenceSession(model_path('{_model}.onnx'), options,
                                   providers=['{_provider}'])
    assert session.get_providers()[0] == '{_provider}', session.get_providers()
    names = [o.name for o in session.get_outputs()]
    pick = next((i for i, n in enumerate(names) if 'prob' in n), 0)
    for i in range(0, len(x), CHUNK):
        out = np.asarray(session.run(None, {{session.get_inputs()[0].name: x[i:i + CHUNK]}})[pick])
        parts.append(out[:, 1] if out.ndim == 2 and out.shape[1] == 2 else out.reshape(-1))'''

R_BODIES = {
    'xgb_r': '''
  model <- xgboost::xgb.load("models/xgb.json")
  for (i in seq(1, nrow(x), by = chunk)) {
    rows <- i:min(i + chunk - 1, nrow(x))
    parts[[length(parts) + 1]] <- predict(model, x[rows, , drop = FALSE])
  }''',
    'cat_r': '''
  model <- catboost::catboost.load_model("models/cat.cbm")
  for (i in seq(1, nrow(x), by = chunk)) {
    rows <- i:min(i + chunk - 1, nrow(x))
    pool <- catboost::catboost.load_pool(as.data.frame(x[rows, , drop = FALSE]))
    parts[[length(parts) + 1]] <- catboost::catboost.predict(
      model, pool, prediction_type = "Probability", thread_count = threads)
  }''',
    'mlp_r': '''
  torch::torch_set_num_threads(threads)
  model <- torch::jit_load("models/mlp.pt")
  torch::with_no_grad({
    for (i in seq(1, nrow(x), by = chunk)) {
      rows <- i:min(i + chunk - 1, nrow(x))
      input <- torch::torch_tensor(x[rows, , drop = FALSE], dtype = torch::torch_float())
      parts[[length(parts) + 1]] <- as.numeric(model(input))
    }
  })''',
}

R_TEMPLATE = '''
#* @transformer
score <- function(df_1, ...) {{
  features <- c({features})
  x <- as.matrix(df_1[, features])
  storage.mode(x) <- "double"
  chunk <- {chunk}L
  threads <- {threads}L
  parts <- list()
{body}
  tibble::tibble(id = df_1$id, score = as.numeric(unlist(parts)))
}}
'''

RUST_TEMPLATE = '''
use mage::prelude::*;

fn transform(rows: DataFrame) -> Result<DataFrame> {{
    let model = mage::onnx::model("models/{model}.onnx")?;
    let features: Vec<&str> = vec![{features}];
    let mut scores: Vec<f64> = Vec::with_capacity(rows.height());
    let mut offset = 0usize;
    while offset < rows.height() {{
        let length = {chunk}usize.min(rows.height() - offset);
        let part = rows.slice(offset as i64, length);
        let out = model.predict(&part, &features)?;
        let names: Vec<String> = out.get_column_names().iter().map(|n| n.to_string()).collect();
        let pick = names
            .iter()
            .find(|n| n.as_str() == "probabilities_1")
            .or_else(|| names.iter().find(|n| n.starts_with("score")))
            .or_else(|| names.last())
            .ok_or_else(|| anyhow!("no output"))?
            .clone();
        let column = out.column(pick.as_str())?.cast(&DataType::Float64)?;
        for chunk in column.f64()?.downcast_iter() {{
            scores.extend(chunk.values().iter());
        }}
        offset += length;
    }}
    Ok(DataFrame::new(rows.height(), vec![
        rows.column("id")?.clone(),
        Column::new("score".into(), scores),
    ])?)
}}
'''

SCORERS = {name: ('python', name.split('_')[0]) for name in PY_BODIES}
SCORERS.update({name: ('r', name.split('_')[0]) for name in R_BODIES})
SCORERS.update({f'{m}_rust_onnx': ('rust', m) for m in ('xgb', 'cat', 'mlp')})


def scorer_code(scorer: str) -> str:
    language, model = SCORERS[scorer]
    if language == 'python':
        return PY.format(features=FEATURES, chunk=CHUNK, threads=THREADS,
                         body=PY_BODIES[scorer])
    if language == 'r':
        return R_TEMPLATE.format(features=', '.join(f'"{f}"' for f in FEATURES), chunk=CHUNK,
                                 threads=THREADS, body=R_BODIES[scorer])
    return RUST_TEMPLATE.format(model=model, chunk=CHUNK,
                                features=', '.join(f'"{f}"' for f in FEATURES))


def build(project: str, scorer: str, rows: int):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    from integration_tests.topologies.test_inference import enable_onnx

    language = SCORERS[scorer][0]
    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'ml_{suffix}', repo_path=project)
    loader = Block.create(f'customers_{suffix}', 'data_loader', project, language='python')
    Path(loader.file_path).write_text(
        textwrap.dedent(LOADER.format(features=FEATURES, rows=rows)).strip() + '\n')
    pipeline.add_block(loader)
    block = Block.create(f'score_{scorer}_{suffix}', 'transformer', project,
                         language=lib.MAGE_LANGUAGE['rust' if language == 'rust' else (
                             'r' if language == 'r' else 'pandas')])
    Path(block.file_path).write_text(textwrap.dedent(scorer_code(scorer)).strip() + '\n')
    if language == 'rust':
        enable_onnx(project)
    pipeline.add_block(block, upstream_block_uuids=[loader.uuid])
    return Pipeline.get(pipeline.uuid, repo_path=project), block.uuid


class Energy:
    """GPU energy from NVML's counter; CPU package energy from RAPL when it is readable."""

    RAPL = '/sys/class/powercap/intel-rapl:0/energy_uj'

    def __init__(self):
        self.gpu = None
        try:
            import pynvml

            pynvml.nvmlInit()
            self.gpu = pynvml.nvmlDeviceGetHandleByIndex(0)
            self.pynvml = pynvml
        except Exception:
            self.gpu = None

    def read(self):
        gpu = cpu = None
        if self.gpu is not None:
            gpu = self.pynvml.nvmlDeviceGetTotalEnergyConsumption(self.gpu) / 1000.0
        try:
            cpu = int(Path(self.RAPL).read_text()) / 1e6
        except OSError:
            cpu = None
        return time.perf_counter(), gpu, cpu

    def idle_gpu_watts(self):
        if self.gpu is None:
            return None
        samples = []
        for _ in range(10):
            samples.append(self.pynvml.nvmlDeviceGetPowerUsage(self.gpu) / 1000.0)
            time.sleep(0.1)
        return float(np.median(samples))


@pytest.fixture
def r_ml(monkeypatch, tmp_path):
    if shutil.which('Rscript') is None or not (R_ML / 'rv.lock').exists():
        pytest.skip('R or the r_ml environment is missing')
    library = next((R_ML / 'rv' / 'library').glob('*/*/*'), None)
    if library is None:
        pytest.skip('Sync integration_tests/topologies/r_ml with rv first')
    monkeypatch.setenv('MAGE_R_PROJECT_DIR', str(R_ML))
    monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))
    monkeypatch.setenv('TORCH_HOME', str(library))


@pytest.mark.parametrize('rows', SIZES)
@pytest.mark.parametrize('scorer', list(SCORERS))
def test_model_scoring(mage_project, ml_project, request, scorer, rows):
    language, model = SCORERS[scorer]
    if language == 'r':
        request.getfixturevalue('r_ml')
    if language == 'rust' and shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
    if scorer.endswith('_cuda'):
        import torch

        if not torch.cuda.is_available():
            pytest.skip('No CUDA device')
    pipeline, score_uuid = build(mage_project, scorer, rows)
    expected = reference(ml_project, model, rows)
    energy = Energy()
    idle = energy.idle_gpu_watts()
    order = random.Random(zlib.crc32(f'{scorer}|{rows}'.encode()))
    for repeat in range(REPEATS + 1):
        modes = ['off', 'chains']
        order.shuffle(modes)
        for position, mode in enumerate(modes):
            before = provenance.wait_quiet()
            t0, gpu0, cpu0 = energy.read()
            pipeline_run, seconds = lib.run(pipeline, mode)
            t1, gpu1, cpu1 = energy.read()
            states = lib.statuses(pipeline_run)
            entry = dict(scorer=scorer, language=language, model=model, rows=rows, mode=mode,
                         repeat=repeat, position=position, cold=repeat == 0,
                         seconds=round(seconds, 4), pipeline=pipeline.uuid,
                         block_seconds=lib.block_seconds(pipeline_run), statuses=states,
                         faker_seconds=ml_project['faker_seconds'], gpu_idle_w=idle,
                         gpu_j=None if gpu0 is None else round(gpu1 - gpu0, 3),
                         cpu_j=None if cpu0 is None else round(cpu1 - cpu0, 3),
                         chunk=CHUNK, threads=THREADS)
            problem = None
            if any(s != 'completed' for s in states.values()):
                problem = f'statuses {states}'
            else:
                frame = lib.stored(pipeline, score_uuid, pipeline_run)
                if hasattr(frame, 'to_pandas') and not hasattr(frame, 'iloc'):
                    frame = frame.to_pandas()
                scores = frame.sort_values('id')['score'].to_numpy(dtype='float64')
                difference = np.abs(scores - expected)
                entry.update(
                    max_abs_diff=float(difference.max()),
                    mean_abs_diff=float(difference.mean()),
                    max_rel_diff=float((difference / np.maximum(np.abs(expected), 1e-12)).max()),
                    sha256=hashlib.sha256(scores.tobytes()).hexdigest(),
                    rows_out=int(len(scores)),
                )
                if len(scores) != rows:
                    problem = f'{len(scores)} scores, expected {rows}'
                elif difference.max() > 1e-3:
                    problem = f'scores differ from Python by up to {difference.max():.3g}'
            entry['problems'] = {'scores': problem} if problem else {}
            provenance.record('ml_scoring', entry, before)
            assert problem is None, problem


# Training the same two models in R with tidymodels (Kuhn and Silge, Tidy Modeling with R,
# 2023) and in Python, on the same 20,000 rows. A logistic regression has one answer, so
# the coefficients must agree; XGBoost with the same settings should give close scores.
TRAIN_LOADER = '''
import os

import pandas as pd

from mage_ai.settings.repo import get_repo_path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    frame = pd.read_parquet(os.path.join(get_repo_path(), 'data', 'ml_base.parquet'))
    frame.insert(0, 'id', range(len(frame)))
    return frame
'''

TRAIN_PY = {
    'glm': '''
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

FEATURES = {features!r}


@transformer
def fit(frame, *args, **kwargs):
    x = frame[FEATURES].to_numpy(dtype='float64')
    model = LogisticRegression(penalty=None, solver='newton-cholesky', tol=1e-10,
                               max_iter=100).fit(x, frame['churned'])
    return pd.DataFrame({{
        'term': ['(Intercept)'] + FEATURES,
        'estimate': np.concatenate([model.intercept_, model.coef_[0]]),
    }})
''',
    'xgb': '''
import pandas as pd
import xgboost

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

FEATURES = {features!r}


@transformer
def fit(frame, *args, **kwargs):
    x = frame[FEATURES].to_numpy(dtype='float32')
    model = xgboost.XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.3,
                                  tree_method='hist', n_jobs={threads},
                                  random_state=0).fit(x, frame['churned'])
    return pd.DataFrame({{'id': frame['id'], 'score': model.predict_proba(x)[:, 1]}})
''',
}

TRAIN_R = {
    'glm': '''
library(tidymodels)

#* @transformer
fit_model <- function(df_1, ...) {{
  customers <- df_1 |>
    dplyr::select(-id) |>
    dplyr::mutate(churned = factor(churned, levels = c(0, 1)))
  churn_fit <- workflow() |>
    add_formula(churned ~ .) |>
    add_model(logistic_reg() |> set_engine("glm")) |>
    fit(data = customers)
  churn_fit |>
    tidy() |>
    dplyr::select(term, estimate)
}}
''',
    'xgb': '''
library(tidymodels)

#* @transformer
fit_model <- function(df_1, ...) {{
  customers <- df_1 |>
    dplyr::mutate(churned = factor(churned, levels = c(0, 1)))
  churn_spec <- boost_tree(trees = 200, tree_depth = 6, learn_rate = 0.3) |>
    set_engine("xgboost", nthread = {threads}, tree_method = "hist") |>
    set_mode("classification")
  churn_fit <- workflow() |>
    add_formula(churned ~ . - id) |>
    add_model(churn_spec) |>
    fit(data = customers)
  predict(churn_fit, customers, type = "prob") |>
    dplyr::transmute(id = customers$id, score = .pred_1)
}}
''',
}


def build_training(project: str, model: str, language: str):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'train_{model}_{language}_{suffix}', repo_path=project)
    loader = Block.create(f'customers_{suffix}', 'data_loader', project, language='python')
    Path(loader.file_path).write_text(TRAIN_LOADER.strip() + '\n')
    pipeline.add_block(loader)
    template = (TRAIN_R if language == 'r' else TRAIN_PY)[model]
    block = Block.create(f'fit_{model}_{language}_{suffix}', 'transformer', project,
                         language='r' if language == 'r' else 'python')
    Path(block.file_path).write_text(
        template.format(features=FEATURES, threads=THREADS).strip() + '\n')
    pipeline.add_block(block, upstream_block_uuids=[loader.uuid])
    return Pipeline.get(pipeline.uuid, repo_path=project), block.uuid


@pytest.mark.parametrize('model', ['glm', 'xgb'])
def test_training_in_r_and_python(mage_project, ml_project, r_ml, model):
    pipelines = {language: build_training(mage_project, model, language)
                 for language in ('python', 'r')}
    order = random.Random(zlib.crc32(f'train|{model}'.encode()))
    outputs = {}
    for repeat in range(REPEATS + 1):
        languages = ['python', 'r']
        order.shuffle(languages)
        for position, language in enumerate(languages):
            pipeline, block_uuid = pipelines[language]
            before = provenance.wait_quiet()
            pipeline_run, seconds = lib.run(pipeline, 'off')
            states = lib.statuses(pipeline_run)
            entry = dict(model=model, language=language, repeat=repeat, position=position,
                         cold=repeat == 0, seconds=round(seconds, 4), pipeline=pipeline.uuid,
                         block_seconds=lib.block_seconds(pipeline_run), statuses=states,
                         threads=THREADS)
            problem = None
            if any(s != 'completed' for s in states.values()):
                problem = f'statuses {states}'
            else:
                frame = lib.stored(pipeline, block_uuid, pipeline_run)
                if hasattr(frame, 'to_pandas') and not hasattr(frame, 'iloc'):
                    frame = frame.to_pandas()
                key = 'term' if model == 'glm' else 'id'
                values = frame.sort_values(key)['estimate' if model == 'glm' else 'score']
                outputs.setdefault(language, []).append(values.to_numpy(dtype='float64'))
                entry['sha256'] = hashlib.sha256(outputs[language][-1].tobytes()).hexdigest()
            entry['problems'] = {'training': problem} if problem else {}
            provenance.record('ml_training', entry, before)
            assert problem is None, problem
    python, r = outputs['python'][-1], outputs['r'][-1]
    difference = np.abs(python - r)
    relative = difference / np.maximum(np.abs(python), 1e-12)
    summary = dict(model=model, language='both', summary=True,
                   max_abs_diff=float(difference.max()), mean_abs_diff=float(difference.mean()),
                   max_rel_diff=float(relative.max()),
                   python_deterministic=len({hashlib.sha256(v.tobytes()).hexdigest()
                                             for v in outputs['python']}) == 1,
                   r_deterministic=len({hashlib.sha256(v.tobytes()).hexdigest()
                                        for v in outputs['r']}) == 1,
                   problems={})
    provenance.record('ml_training', summary, provenance.machine_snapshot())
    if model == 'glm':
        # One maximum likelihood answer: both must find it.
        assert relative.max() < 1e-4, summary
