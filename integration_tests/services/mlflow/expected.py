"""
What the MLflow service is seeded with. Standard library only: the tests import it.
"""
import hashlib
import json

EXPERIMENT = 'mage-integration'
MODEL = 'mage-it-regressor'
RUNS = ('train-v1', 'train-v2')
# Version 1 is saved with cloudpickle, as MLflow 2 did by default; version 2 with skops,
# the MLflow 3 default.
SERIALIZATION = {'train-v1': 'cloudpickle', 'train-v2': 'skops'}
ALIASES = {'champion': 1, 'challenger': 2}

# y = 2 * x1 + 3 * x2 + intercept, with the intercept of each run.
INTERCEPTS = {'train-v1': 1.0, 'train-v2': -4.0}
PARAMS = {
    'train-v1': dict(fit_intercept='True', features='x1,x2', rows='20', note='día ñ'),
    'train-v2': dict(fit_intercept='True', features='x1,x2', rows='20', note='second'),
}


def training_rows():
    return [[float(i), float(i % 3)] for i in range(20)]


def target(rows, intercept):
    return [2 * x1 + 3 * x2 + intercept for x1, x2 in rows]


def rmse_history(run: str):
    """One value per step; steps 0 to 9."""
    base = 1.0 if run == 'train-v1' else 2.0
    return [round(base / (step + 1), 6) for step in range(10)]


def text_files():
    """Artifact path to content, for text artifacts."""
    return {
        'config/params.json': json.dumps(dict(alpha=0.5, layers=[64, 32]), sort_keys=True),
        'notes/notas_ñ.txt': 'unicode name and body: ñandú 中文\n',
        'nested/a/b/c/deep.txt': 'deep\n',
        'data/sample.csv': 'id,value\n1,0.5\n2,\n3,"a,b"\n',
    }


def large_file() -> bytes:
    """Five MiB of deterministic bytes."""
    chunk = hashlib.sha256(b'mage').digest()
    return (chunk * (5 * 1024 * 1024 // len(chunk) + 1))[: 5 * 1024 * 1024]
