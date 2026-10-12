"""
Data types across language boundaries: a Python loader makes one column per type, a block
in another language returns its input unchanged, and a Python block after it receives the
result. Each pipeline runs block by block and fused; the stored outputs are compared with
the loader's, column by column.

MAGE_STUDY_RESULTS_DIR keeps one line per language and mode with what survived
(provenance.py).
"""
import os
import shutil
import textwrap
import uuid
from pathlib import Path

import pytest

from integration_tests.topologies import provenance

LOADER = '''
import datetime as dt
from decimal import Decimal

import numpy as np
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    days = pd.to_datetime(
        ['2024-01-01', '1970-01-01', '2038-01-19', None, '1900-03-01', '2024-02-29'],
    )
    return pd.DataFrame({
        'int64': np.array([1, -2, 2**62, 0, -(2**63) + 1, 7], dtype='int64'),
        'int64_null': pd.array([1, -2, None, 0, 5, 7], dtype='Int64'),
        'int32': np.array([1, -2, 2**31 - 1, 0, -(2**31), 7], dtype='int32'),
        'uint64': np.array([0, 1, 2**63, 2**64 - 1, 5, 7], dtype='uint64'),
        'float64': [1.5, float('nan'), float('inf'), -0.0, -float('inf'), 1e308],
        'float64_null': pd.array([1.5, None, 3.25, 0.0, -2.0, 1e-300], dtype='Float64'),
        'float32': np.array([1.5, 2.25, -0.5, 0, 3.4e38, 1e-7], dtype='float32'),
        'bool': [True, False, True, True, False, False],
        'bool_null': pd.array([True, False, None, True, False, None], dtype='boolean'),
        'text': ['plain', 'ñandú', 'crab 🦀', '', 'tab\\there', 'line\\nbreak'],
        'text_null': ['a', None, 'c', None, 'e', ''],
        'category': pd.Categorical(['x', 'y', 'x', None, 'z', 'x']),
        'date': [d.date() if not pd.isna(d) else None for d in days],
        'ts_naive': days,
        'ts_utc': days.tz_localize('UTC'),
        'ts_local': days.tz_localize('America/Costa_Rica'),
        'ts_ns': pd.to_datetime(['2024-01-01 00:00:00.123456789'] * 6),
        'duration': pd.to_timedelta(['1 day', '2 hours', None, '0 s', '1 ms', '-3 min']),
        'decimal': [Decimal('1.10'), Decimal('-2.5'), None, Decimal('0'),
                    Decimal('12345678901234567890.123'), Decimal('3')],
        'list_text': [['a'], [], None, ['b', 'c'], ['ñ'], ['']],
        'list_int': [[1, 2], [], None, [3], [-4, 5, 6], [0]],
        'struct': [{'a': 1, 'b': 'x'}, {'a': 2, 'b': None}, None, {'a': None, 'b': 'z'},
                   {'a': 5, 'b': 'w'}, {'a': 6, 'b': ''}],
        'bytes': [b'a', b'', None, b'\\x00\\xff', b'xyz', b'q'],
    })
'''

IDENTITY = {
    'python': '''
        if 'transformer' not in globals():
            from mage_ai.data_preparation.decorators import transformer


        @transformer
        def same(df, *args, **kwargs):
            return df
    ''',
    'r': '''
        #* @transformer
        same <- function(df_1, ...) {
          df_1
        }
    ''',
    'rust': '''
        use mage::prelude::*;

        fn transform(rows: LazyFrame) -> Result<LazyFrame> {
            Ok(rows)
        }
    ''',
}

AFTER = '''
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def after(df, *args, **kwargs):
    return df
'''


def pipeline_of(project, blocks):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'types_{suffix}', repo_path=project)
    upstream = []
    for name, block_type, language, code in blocks:
        block = Block.create(f'{name}_{suffix}', block_type, project, language=language)
        Path(block.file_path).write_text(textwrap.dedent(code).strip() + '\n')
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        upstream = [block]
    return Pipeline.get(pipeline.uuid, repo_path=project)


def run(pipeline, mode):
    from mage_ai.orchestration import fusion_verify
    from mage_ai.shared.dates import utc_now

    return fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, mode, {}, utc_now()), log=lambda text: None,
    )


def stored(pipeline, block_uuid, pipeline_run):
    variable = pipeline.variable_manager.get_variable_object(
        pipeline.uuid, block_uuid, 'output_0', partition=pipeline_run.execution_partition,
    )
    return variable.read_data()


def describe(value):
    """A comparable text of one value: numpy and pandas scalars, timestamps and arrays are
    compared as the plain Python values they hold."""
    import datetime as dt
    from decimal import Decimal

    import numpy as np
    import pandas as pd

    if value is None:
        return None
    if hasattr(value, 'as_py'):
        value = value.as_py()
    if value is pd.NaT or value is pd.NA:
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else f'ts:{value.value}:{value.tz}'
    if isinstance(value, dt.datetime):
        ts = pd.Timestamp(value)
        return f'ts:{ts.value}:{ts.tz}'
    if isinstance(value, (pd.Timedelta, dt.timedelta)):
        return None if pd.isna(value) else f'td:{pd.Timedelta(value).value}'
    if isinstance(value, Decimal):
        return f'dec:{value.normalize()}'
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, dict):
        return repr({k: describe(v) for k, v in value.items()})
    if isinstance(value, (bytes, str)):
        return repr(value)
    if np.ndim(value) > 0:
        return repr([describe(v) for v in list(value)])
    if isinstance(value, float) and value != value:
        # pandas uses NaN for a missing float; Mage stores it as null.
        return None
    if pd.isna(value) is True:
        return None
    return repr(value)


def element_type(series) -> str:
    """The Python type of the first value that is not null, such as list or ndarray."""
    for value in series:
        if value is not None and not (isinstance(value, float) and value != value):
            return type(value).__name__
    return '-'


def compare(before, after):
    """
    {column: {'result', 'dtype', 'element', 'first'}}; result is same, dtype (same values,
    another type), values (a value changed) or missing.
    """
    if hasattr(before, 'to_pandas') and not hasattr(before, 'iloc'):
        before = before.to_pandas()
    # A Polars output keeps its dtype in the record; its values compare through pandas,
    # since Python's datetime holds microseconds, not nanoseconds.
    after_types = {c: str(after[c].dtype) for c in after.columns}
    if hasattr(after, 'to_pandas') and not hasattr(after, 'iloc'):
        after = after.to_pandas(use_pyarrow_extension_array=True)
    out = {}
    for column in before.columns:
        if column not in after.columns:
            out[column] = {'result': 'missing'}
            continue
        a, b = before[column], after[column]
        values = [describe(v) for v in a], [describe(v) for v in b]
        b_type = after_types.get(column, str(b.dtype))
        same_type = str(a.dtype) == b_type and element_type(a) == element_type(b)
        result = 'values' if values[0] != values[1] else ('same' if same_type else 'dtype')
        entry = {'result': result, 'dtype': [str(a.dtype), b_type],
                 'element': [element_type(a), element_type(b)]}
        if result == 'values':
            for i, (x, y) in enumerate(zip(*values)):
                if x != y:
                    entry['first'] = [i, x, y]
                    break
        out[column] = entry
    return out


@pytest.mark.parametrize('mode', ['off', 'chains'])
@pytest.mark.parametrize('language', ['python', 'r', 'rust'])
def test_types_cross_a_block_in_each_language(mage_project, monkeypatch, tmp_path,
                                              language, mode):
    from mage_ai.orchestration import fusion

    if language == 'rust' and shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
    if language == 'r':
        path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
        if not path or shutil.which('Rscript') is None:
            pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
        monkeypatch.setenv('MAGE_R_PROJECT_DIR', path)
        monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))

    pipeline = pipeline_of(mage_project, [
        ('load', 'data_loader', 'python', LOADER),
        ('same', 'transformer', language, IDENTITY[language]),
        ('after', 'transformer', 'python', AFTER),
    ])
    blocks = list(pipeline.blocks_by_uuid)
    pipeline_run = run(pipeline, {
        'off': fusion.BLOCK_FUSION_OFF, 'chains': fusion.BLOCK_FUSION_CHAINS,
    }[mode])
    statuses = {
        b.block_uuid: getattr(b.status, 'value', b.status) for b in pipeline_run.block_runs
    }
    key = f'{language}/{mode}'
    if any(s != 'completed' for s in statuses.values()):
        provenance.record('types', dict(language=language, mode=mode, statuses=statuses,
                                        pipeline=pipeline.uuid, columns=None))
        pytest.fail(f'{key}: {statuses}')
    before = stored(pipeline, blocks[0], pipeline_run)
    after = stored(pipeline, blocks[2], pipeline_run)
    provenance.record('types', dict(language=language, mode=mode, statuses=statuses,
                                    pipeline=pipeline.uuid, columns=compare(before, after)))
