"""
Pipelines whose blocks run in several Python workers (python_workers/<name>/), run through
the scheduler as people run them: what crosses from one worker to the next and in what form,
what a block gets compared with MageML's own Python, how a failure says what to do, block by
block and fused, and how long a large table takes to cross.

The workers, prepared once for the module:

- pandas2: pandas 2.2 and pyarrow 17, older than MageML's (Feast and older models pin them);
- py313: Python 3.13 with MageML's pandas and pyarrow;
- polars: Polars only, without pandas or pyarrow;
- plain: the standard library only.
"""
import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(shutil.which('uv') is None, reason='uv is not installed')

PANDAS = importlib.metadata.version('pandas')
PYARROW = importlib.metadata.version('pyarrow')
POLARS = importlib.metadata.version('polars')
WORKERS = {
    'pandas2': ('>=3.12', ['pandas==2.2.3', 'pyarrow==17.0.0']),
    'py313': ('>=3.13', [f'pandas=={PANDAS}', f'pyarrow=={PYARROW}']),
    'polars': ('>=3.12', [f'polars=={POLARS}']),
    'plain': ('>=3.12', []),
}

# A table with what people put in one: missing values of every kind, time zones, categories,
# tags and embeddings as lists, a named index of strings.
RICH_FRAME = '''
import pandas as pd


def rich_frame():
    return pd.DataFrame({
        'text': ['a', None, 'c'],
        'count': pd.array([1, None, 3], dtype='Int64'),
        'score': [1.5, float('nan'), 3.0],
        'flag': [True, False, None],
        'seen_at': pd.to_datetime(
            ['2026-01-01 10:00', None, '2026-01-03 00:00']).tz_localize('UTC'),
        'segment': pd.Categorical(['x', 'y', 'x']),
        'tags': [['new', 'web'], None, []],
        'embedding': [[0.25, 0.5], [0.75, 1.0], [1.25, 1.5]],
    }, index=pd.Index(['r1', 'r2', 'r3'], name='key'))
'''
IDENTITY = '''
@transformer
def same(data, *args, **kwargs):
    {check}
    return data
'''


@pytest.fixture(scope='module')
def workers(mage_project, tmp_path_factory):
    """The module's workers in the session project, prepared into a folder of their own."""
    from mage_ai.data_preparation import environments

    previous = os.environ.get(environments.ENVIRONMENTS_DIR_ENV)
    # In os.environ: the scheduler's jobs read it too.
    os.environ[environments.ENVIRONMENTS_DIR_ENV] = str(tmp_path_factory.mktemp('prepared'))
    folders = []
    for name, (python, dependencies) in WORKERS.items():
        folder = Path(mage_project, environments.WORKERS_DIR, name)
        folders.append(folder)
        write_worker(folder, name, python, dependencies)
        environments.prepare(environments.named_workers(mage_project)[name], log=lambda _: None)
    yield mage_project
    for folder in folders:
        shutil.rmtree(folder, ignore_errors=True)
    if previous is None:
        os.environ.pop(environments.ENVIRONMENTS_DIR_ENV, None)
    else:
        os.environ[environments.ENVIRONMENTS_DIR_ENV] = previous


def write_worker(folder: Path, name: str, python: str, dependencies, lock: bool = True):
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'pyproject.toml').write_text(textwrap.dedent(f'''
        [project]
        name = "{name}-worker"
        version = "0"
        requires-python = "{python}"
        dependencies = {json.dumps(dependencies)}

        [tool.mageml.worker]
        label = "{name} worker"
    '''))
    if lock:
        subprocess.run(['uv', 'lock', '--project', str(folder)], check=True, capture_output=True)


def pipeline_of(project: str, blocks, variables=None):
    """
    blocks: (name, type, worker, code[, upstream names]); without upstream names a block
    follows the one before it. worker None is MageML's Python.
    """
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'workers_{suffix}', repo_path=project)
    created = {}
    previous = None
    for name, block_type, worker, code, *upstream in blocks:
        block = Block.create(
            f'{name}_{suffix}', block_type, project, language='python',
            configuration=dict(python_worker=worker) if worker else None,
        )
        Path(block.file_path).write_text(textwrap.dedent(code).strip() + '\n')
        names = upstream[0] if upstream else ([previous] if previous else [])
        pipeline.add_block(block, upstream_block_uuids=[created[n].uuid for n in names])
        created[name] = block
        previous = name
    if variables:
        pipeline.variables = variables
        pipeline.save()
    return Pipeline.get(pipeline.uuid, repo_path=project)


def run(pipeline, mode=None):
    from mage_ai.orchestration import fusion, fusion_verify
    from mage_ai.shared.dates import utc_now

    return fusion_verify.run_in_process(
        fusion_verify._create_run(
            pipeline, mode or fusion.BLOCK_FUSION_OFF, {}, utc_now(),
        ),
        log=lambda text: None,
    )


def by_name(pipeline_run):
    return {b.block_uuid.rsplit('_', 1)[0]: b for b in pipeline_run.block_runs}


def statuses(pipeline_run):
    return {name: getattr(b.status, 'value', b.status)
            for name, b in by_name(pipeline_run).items()}


def error_of(block_run) -> str:
    return ((block_run.metrics or {}).get('error') or {}).get('message') or ''


def output(pipeline, pipeline_run, name, index=0):
    [block_uuid] = [u for u in pipeline.blocks_by_uuid if u.rsplit('_', 1)[0] == name]
    return pipeline.variable_manager.get_variable(
        pipeline.uuid, block_uuid, f'output_{index}', partition=pipeline_run.execution_partition,
    )


def completed(pipeline_run):
    state = statuses(pipeline_run)
    assert set(state.values()) == {'completed'}, (state, {
        name: error_of(b) for name, b in by_name(pipeline_run).items() if error_of(b)})
    return pipeline_run


def assert_same_both_ways(pipeline):
    from mage_ai.orchestration import fusion, fusion_verify

    unfused = completed(run(pipeline, fusion.BLOCK_FUSION_OFF))
    fused = completed(run(pipeline, fusion.BLOCK_FUSION_CHAINS))
    results = fusion_verify.compare_runs(pipeline, unfused, fused)
    assert [r for r in results if r.result not in ('same', 'close')] == [], results
    return fused


def rich_loader():
    return RICH_FRAME + '''

@data_loader
def load(*args, **kwargs):
    return rich_frame()
'''


def test_a_table_keeps_its_types_across_workers_of_other_pandas_and_python_versions(workers):
    import pandas as pd

    namespace = {}
    exec(RICH_FRAME, namespace)
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, rich_loader()),
        ('older', 'transformer', 'pandas2', IDENTITY.format(
            check="import pandas; assert pandas.__version__ == '2.2.3', pandas.__version__")),
        ('newer', 'transformer', 'py313', IDENTITY.format(
            check='import sys; assert sys.version_info[:2] == (3, 13), sys.version')),
    ])
    fused = assert_same_both_ways(pipeline)
    for name in ('older', 'newer'):
        pd.testing.assert_frame_equal(output(pipeline, fused, name), namespace['rich_frame']())
    # Lists come back as lists, not the NumPy arrays Arrow would give pandas.
    newer = output(pipeline, fused, 'newer')
    assert newer['tags'].tolist() == [['new', 'web'], None, []]
    assert type(newer['embedding'].iloc[0]) is list


def test_a_table_arrives_as_what_the_workers_python_has(workers):
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, '''
            import polars as pl

            @data_loader
            def load(*args, **kwargs):
                return pl.DataFrame({'id': [1, 2, 3], 'amount': [10.0, 20.0, 30.0]})
        '''),
        # No Polars in pandas2: the Polars table arrives as pandas.
        ('in_pandas', 'transformer', 'pandas2', '''
            import pandas as pd

            @transformer
            def double(data, *args, **kwargs):
                assert isinstance(data, pd.DataFrame), type(data)
                return data.assign(doubled=data['amount'] * 2)
        '''),
        # No pandas in polars: the pandas table arrives as Polars.
        ('in_polars', 'transformer', 'polars', '''
            import polars as pl

            @transformer
            def total(data, *args, **kwargs):
                assert isinstance(data, pl.DataFrame), type(data)
                return data.select(pl.col('doubled').sum().alias('total'))
        '''),
    ])
    pipeline_run = completed(run(pipeline))
    assert output(pipeline, pipeline_run, 'in_polars').to_dicts() == [{'total': 120.0}]


def test_a_worker_without_a_table_library_says_what_to_add(workers):
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, '''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1]})
        '''),
        ('plain', 'transformer', 'plain', IDENTITY.format(check='pass')),
        ('after', 'transformer', None, IDENTITY.format(check='pass')),
    ])
    pipeline_run = run(pipeline)
    state = statuses(pipeline_run)
    assert (state['load'], state['plain']) == ('completed', 'failed'), state
    assert state['after'] != 'completed', state
    message = error_of(by_name(pipeline_run)['plain'])
    assert 'An upstream block returned a pandas table' in message, message
    assert 'Add pandas and pyarrow, or polars, to the dependencies' in message, message


VALUES = {
    'mapping': "{'name': 'a', 'scores': [1, 2.5], 'nested': {'ok': True, 'none': None}}",
    'numbers': '[1, 2, 3]',
    'text': "'héllo'",
    'integer': '42',
    'nothing': 'None',
    'array': '__import__("numpy").arange(6).reshape(2, 3)',
    'moment': '__import__("datetime").datetime(2026, 1, 2, 3, 4, 5)',
    'pair': "('a', 1)",
    'not_a_number': "float('nan')",
}
DESCRIBE = '''
@transformer
def describe(*values, **kwargs):
    described = []
    for value in values:
        if hasattr(value, 'tolist'):
            value = (type(value).__name__, value.tolist())
        described.append([type(value).__name__, repr(value)])
    return {'values': described}
'''


def described(project, loader_worker, reader_worker):
    loaders = [
        (f'v_{name}', 'data_loader', loader_worker, f'''
            @data_loader
            def load(*args, **kwargs):
                return {code}
        ''', [])
        for name, code in VALUES.items()
    ]
    pipeline = pipeline_of(project, loaders + [
        ('describe', 'transformer', reader_worker, DESCRIBE, [f'v_{n}' for n in VALUES]),
    ])
    return output(pipeline, completed(run(pipeline)), 'describe')['values']


def test_values_that_are_not_tables_reach_the_next_block_as_in_mages_python(workers):
    in_mage = described(workers, None, None)
    # The values a block returns in a worker, read in MageML's Python and in another worker.
    assert described(workers, 'py313', None) == in_mage
    assert described(workers, 'py313', 'pandas2') == in_mage


VARIABLES = '''
@data_loader
def load(*args, **kwargs):
    return {
        'execution_date': type(kwargs['execution_date']).__name__,
        'threshold': kwargs['threshold'],
        'options': kwargs['options'],
        'keys': sorted(kwargs),
    }
'''


def test_the_runs_variables_reach_a_worker_as_in_mages_python(workers):
    variables = dict(threshold=3, options={'mode': 'fast', 'ids': [1, 2]})
    results = {}
    for worker in (None, 'pandas2'):
        pipeline = pipeline_of(workers, [('load', 'data_loader', worker, VARIABLES)], variables)
        results[worker] = output(pipeline, completed(run(pipeline)), 'load')
    in_mage, in_worker = results[None], results['pandas2']
    assert in_worker['execution_date'] == in_mage['execution_date'] == 'datetime'
    assert (in_worker['threshold'], in_worker['options']) == (3, variables['options'])
    # Values that are not JSON (a logger, a context object) stay in MageML's Python; the
    # documentation lists them.
    assert set(in_worker['keys']) <= set(in_mage['keys'])
    missing = sorted(set(in_mage['keys']) - set(in_worker['keys']))
    print('kwargs only in MageML\'s Python:', missing)


def test_a_failing_worker_block_fails_the_run_with_its_error_and_line(workers):
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', 'pandas2', '''
            @data_loader
            def load(*args, **kwargs):
                rows = [{'id': 1}, {'id': None}]
                for number, row in enumerate(rows):
                    if row['id'] is None:
                        raise ValueError(f'row {number} has no id')
                return rows
        '''),
        ('after', 'transformer', None, IDENTITY.format(check='pass')),
    ])
    pipeline_run = run(pipeline)
    state = statuses(pipeline_run)
    assert state['load'] == 'failed' and state['after'] != 'completed', state
    message = error_of(by_name(pipeline_run)['load'])
    assert 'row 1 has no id' in message, message
    # The traceback names the block's file and line.
    assert 'line 6' in message and 'load_' in message, message


def test_a_worker_block_is_retried_and_completes(workers, tmp_path):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    marker = tmp_path / 'attempts'
    pipeline = pipeline_of(workers, [('load', 'data_loader', 'pandas2', f'''
        from pathlib import Path

        @data_loader
        def load(*args, **kwargs):
            marker = Path({str(marker)!r})
            attempts = int(marker.read_text()) + 1 if marker.exists() else 1
            marker.write_text(str(attempts))
            if attempts == 1:
                raise ConnectionError('the source was not ready')
            return {{'attempts': attempts}}
    ''')])
    block = next(iter(pipeline.blocks_by_uuid.values()))
    block.update(dict(retry_config=dict(retries=1, delay=0)))
    pipeline = Pipeline.get(pipeline.uuid, repo_path=workers)
    pipeline_run = completed(run(pipeline))
    assert output(pipeline, pipeline_run, 'load') == {'attempts': 2}


def test_an_unprepared_or_missing_worker_fails_its_block_with_what_to_do(workers):
    from mage_ai.data_preparation import environments

    write_worker(Path(workers, environments.WORKERS_DIR, 'later'), 'later', '>=3.12', [])
    try:
        pipeline = pipeline_of(workers, [
            ('load', 'data_loader', 'later', '''
                @data_loader
                def load(*args, **kwargs):
                    return {'ok': True}
            '''),
            ('after', 'transformer', None, IDENTITY.format(check='pass')),
        ])
        pipeline_run = run(pipeline)
        assert statuses(pipeline_run)['load'] == 'failed'
        message = error_of(by_name(pipeline_run)['load'])
        assert 'not prepared' in message and 'mage workers prepare later' in message, message

        environments.prepare(environments.named_workers(workers)['later'], log=lambda _: None)
        completed(run(pipeline))
    finally:
        shutil.rmtree(Path(workers, environments.WORKERS_DIR, 'later'), ignore_errors=True)

    missing = pipeline_of(workers, [('load', 'data_loader', 'pandsa2', VARIABLES)])
    pipeline_run = run(missing)
    message = error_of(by_name(pipeline_run)['load'])
    assert 'selects Python worker "pandsa2", which does not exist' in message, message
    assert 'pandas2' in message, message


def test_a_fused_stage_crosses_workers_says_where_and_gives_the_same_outputs(workers):
    from mage_ai.orchestration import planning

    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, rich_loader()),
        ('first', 'transformer', 'pandas2', IDENTITY.format(check='pass')),
        ('second', 'transformer', 'pandas2', IDENTITY.format(check='pass')),
        ('third', 'transformer', 'py313', IDENTITY.format(check='pass')),
        ('last', 'transformer', None, IDENTITY.format(check='pass')),
    ])
    plan = planning.plan_pipeline(pipeline)
    names = {u: u.rsplit('_', 1)[0] for u in pipeline.blocks_by_uuid}
    # One scheduler job for the stage; each block still runs in its worker's process, and
    # the plan says where the data moves between two of them.
    assert [[names[u] for u in group] for group in plan.groups] == [
        ['load', 'first', 'second', 'third', 'last']]
    switches = [(names[f['block']], names[f['next']]) for f in plan.findings
                if f.get('code') == 'worker_switch']
    assert switches == [('load', 'first'), ('second', 'third'), ('third', 'last')], switches
    assert_same_both_ways(pipeline)


def test_branches_in_different_workers_join_in_mages_python(workers):
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, '''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': range(100), 'amount': [i * 1.5 for i in range(100)]})
        '''),
        ('left', 'transformer', 'pandas2', '''
            @transformer
            def left(data, *args, **kwargs):
                return data[data['id'] % 2 == 0][['id', 'amount']]
        ''', ['load']),
        ('right', 'transformer', 'py313', '''
            @transformer
            def right(data, *args, **kwargs):
                return data.assign(tripled=data['amount'] * 3)[['id', 'tripled']]
        ''', ['load']),
        ('join', 'transformer', None, '''
            @transformer
            def join(left, right, *args, **kwargs):
                return left.merge(right, on='id')
        ''', ['left', 'right']),
    ])
    joined = output(pipeline, completed(run(pipeline)), 'join')
    assert len(joined) == 50
    assert (joined['tripled'] == joined['amount'] * 3).all()


def test_a_custom_block_and_an_exporter_run_in_workers(workers, tmp_path):
    target = tmp_path / 'scores.csv'
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, '''
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                return pd.DataFrame({'id': [1, 2], 'score': [0.5, 0.75]})
        '''),
        ('summary', 'custom', 'py313', '''
            @custom
            def summary(data, *args, **kwargs):
                return {'rows': len(data), 'mean': float(data['score'].mean())}
        ''', ['load']),
        ('export', 'data_exporter', 'pandas2', f'''
            @data_exporter
            def export(data, *args, **kwargs):
                data.to_csv({str(target)!r}, index=False)
        ''', ['load']),
    ])
    pipeline_run = completed(run(pipeline))
    assert output(pipeline, pipeline_run, 'summary') == {'rows': 2, 'mean': 0.625}
    assert target.read_text().splitlines() == ['id,score', '1,0.5', '2,0.75']


def test_what_a_worker_block_prints_is_in_its_log(workers):
    pipeline = pipeline_of(workers, [('load', 'data_loader', 'plain', '''
        import sys

        @data_loader
        def load(*args, **kwargs):
            print('read 3 rows from the source')
            print('2 rows had no id', file=sys.stderr)
            return {'rows': 3}
    ''')])
    pipeline_run = completed(run(pipeline))
    logs = json.dumps(by_name(pipeline_run)['load'].logs)
    assert 'read 3 rows from the source' in logs, logs[:2000]
    assert '2 rows had no id' in logs, logs[:2000]


ROWS = 1_000_000


def test_a_million_rows_cross_three_workers_in_seconds(workers):
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, f'''
            import numpy as np
            import pandas as pd

            @data_loader
            def load(*args, **kwargs):
                ids = np.arange({ROWS})
                return pd.DataFrame({{
                    'id': ids,
                    'amount': ids * 0.5,
                    'segment': np.where(ids % 3 == 0, 'retail', 'online'),
                    'seen_at': pd.Timestamp('2026-01-01', tz='UTC')
                    + pd.to_timedelta(ids % 86400, unit='s'),
                }})
        '''),
        ('older', 'transformer', 'pandas2', '''
            @transformer
            def older(data, *args, **kwargs):
                return data.assign(doubled=data['amount'] * 2)
        '''),
        ('newer', 'transformer', 'py313', '''
            @transformer
            def newer(data, *args, **kwargs):
                return data[data['segment'] == 'retail']
        '''),
        ('total', 'transformer', 'polars', '''
            import polars as pl

            @transformer
            def total(data, *args, **kwargs):
                return data.group_by('segment').agg(pl.col('doubled').sum(), pl.len())
        '''),
    ])
    started = time.monotonic()
    pipeline_run = completed(run(pipeline))
    seconds = time.monotonic() - started
    [row] = output(pipeline, pipeline_run, 'total').to_dicts()
    assert row['len'] == (ROWS + 2) // 3
    timings = {
        name: (b.completed_at - b.started_at).total_seconds()
        for name, b in by_name(pipeline_run).items() if b.started_at and b.completed_at
    }
    print(f'{ROWS} rows through three workers: {seconds:.1f} s; per block {timings}')
    # Measured 2026-11-18 on an M-series Mac; CI runners take up to about three times longer.
    assert seconds < 120, (seconds, timings)


def test_an_exported_service_passes_the_same_table_between_workers(workers, tmp_path):
    import pandas as pd

    from integration_tests.exported_services import export_service, run_service

    namespace = {}
    exec(RICH_FRAME, namespace)
    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', None, rich_loader()),
        ('older', 'transformer', 'pandas2', IDENTITY.format(check='pass')),
        ('newer', 'transformer', 'py313', IDENTITY.format(check='pass')),
    ])
    directory = export_service(workers, pipeline.uuid, tmp_path)
    manifest = json.loads((directory / 'service.json').read_text())
    assert set(manifest['python_workers']) == {'pandas2', 'py313'}
    # As the image's Dockerfile does: each worker's environment from its lock.
    for name, python in (('pandas2', sys.executable), ('py313', '3.13')):
        folder = directory / 'workers' / name
        subprocess.run(['uv', 'venv', '.venv', '--python', python], cwd=folder, check=True,
                       capture_output=True)
        subprocess.run(['uv', 'sync', '--locked', '--no-dev', '--no-install-project'],
                       cwd=folder, check=True, capture_output=True)
    result = run_service(directory, pipeline.uuid, tmp_path)
    [last] = [b for b in result['blocks'] if b['block'].startswith('newer_')]
    from mage_ai.pipeline_services.runtime import worker

    frame = worker.read_input(last['outputs'][0])
    pd.testing.assert_frame_equal(frame, namespace['rich_frame']())


@pytest.mark.parametrize('ending, expected', [
    ('os._exit(3)', 'Python worker "plain" exited with code 3 before the block returned'),
    # As the system ends a process that runs out of memory.
    ('os.kill(os.getpid(), signal.SIGKILL)', 'Python worker "plain" was stopped by SIGKILL'),
])
def test_a_worker_process_that_dies_says_how(workers, ending, expected):
    pipeline = pipeline_of(workers, [('load', 'data_loader', 'plain', f'''
        import os
        import signal

        @data_loader
        def load(*args, **kwargs):
            print('about to stop')
            {ending}
    ''')])
    pipeline_run = run(pipeline)
    block_run = by_name(pipeline_run)['load']
    assert statuses(pipeline_run)['load'] == 'failed'
    assert expected in error_of(block_run), error_of(block_run)
    if 'SIGKILL' in ending:
        assert 'which the system sends when memory runs out' in error_of(block_run)
    assert 'about to stop' in json.dumps(block_run.logs)


def test_a_failing_test_of_a_worker_block_fails_it(workers):
    pipeline = pipeline_of(workers, [('load', 'data_loader', 'pandas2', '''
        @data_loader
        def load(*args, **kwargs):
            return {'rows': 0}


        @test
        def test_rows(output, *args):
            assert output['rows'] > 0, 'the source returned no rows'
    ''')])
    pipeline_run = run(pipeline)
    assert statuses(pipeline_run)['load'] == 'failed'
    assert 'test_rows (the source returned no rows)' in error_of(by_name(pipeline_run)['load'])


def test_a_block_from_a_mageml_template_says_what_its_worker_lacks(workers):
    # MageML's data loader templates import its connectors, which need MageML's packages.
    pipeline = pipeline_of(workers, [('load', 'data_loader', 'plain', '''
        from mage_ai.io.postgres import Postgres

        @data_loader
        def load(*args, **kwargs):
            return {'ok': True}
    ''')])
    pipeline_run = run(pipeline)
    assert statuses(pipeline_run)['load'] == 'failed'
    message = error_of(by_name(pipeline_run)['load'])
    assert ("MageML's mage_ai.io.postgres needs numpy, which Python worker \"plain\" does not "
            'have') in message, message
    assert "or run this block in MageML's Python" in message, message


def test_an_environment_left_without_its_marker_is_prepared_again(workers):
    from mage_ai.data_preparation import environments

    worker = environments.named_workers(workers)['plain']
    marker = worker.directory / environments.MARKER
    marker.unlink()
    assert not worker.prepared
    environments.prepare(worker, log=lambda _: None)
    assert worker.prepared


def test_the_compile_check_says_a_worker_lacks_what_a_mageml_module_needs(workers):
    from mage_ai.orchestration import compilation

    pipeline = pipeline_of(workers, [('load', 'data_loader', 'plain', '''
        if 'data_loader' not in globals():
            from mage_ai.data_preparation.decorators import data_loader
        from mage_ai.io.postgres import Postgres

        @data_loader
        def load(*args, **kwargs):
            return {'ok': True}
    ''')])
    [block_uuid] = list(pipeline.blocks_by_uuid)
    warnings = compilation._missing_imports(pipeline, [block_uuid])
    assert warnings == [
        f"{block_uuid} imports MageML's mage_ai.io.postgres, which needs numpy; plain worker "
        '(plain) does not have it: add numpy to python_workers/plain/pyproject.toml and run uv '
        "lock, or run the block in MageML's Python."
    ], warnings


def test_a_dynamic_block_in_a_worker_is_refused_at_compile_and_at_run(workers):
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration import compilation

    pipeline = pipeline_of(workers, [
        ('load', 'data_loader', 'plain', '''
            @data_loader
            def load(*args, **kwargs):
                return [[1, 2, 3]]
        '''),
        ('after', 'transformer', None, IDENTITY.format(check='pass')),
    ])
    block = next(b for u, b in pipeline.blocks_by_uuid.items() if u.startswith('load_'))
    block.update(dict(configuration={**block.configuration, 'dynamic': True}))
    pipeline = Pipeline.get(pipeline.uuid, repo_path=workers)
    expected = (f'Block {block.uuid} is dynamic or replicated, which cannot run in Python '
                'worker "plain" yet: choose MageML\'s Python for it in its settings')
    [unit] = [u for u in compilation.units_for(pipeline) if block.uuid in u.block_uuids]
    with pytest.raises(compilation.CompileError, match=re.escape(expected)):
        compilation._check_python(pipeline, unit)

    pipeline_run = run(pipeline)
    assert statuses(pipeline_run)['load'] == 'failed'
    assert expected in error_of(by_name(pipeline_run)['load'])
    assert statuses(pipeline_run)['after'] != 'completed'


def test_twenty_blocks_in_one_worker_run_in_a_warm_process(workers):
    from mage_ai.orchestration import fusion

    blocks = [('b0', 'data_loader', 'pandas2', '''
        import os

        @data_loader
        def load(*args, **kwargs):
            return {'pids': [os.getpid()]}
    ''')]
    for index in range(1, 20):
        blocks.append((f'b{index}', 'transformer', 'pandas2', '''
            import os

            @transformer
            def step(data, *args, **kwargs):
                return {'pids': data['pids'] + [os.getpid()]}
        '''))
    pipeline = pipeline_of(workers, blocks)
    started = time.monotonic()
    pipeline_run = completed(run(pipeline, fusion.BLOCK_FUSION_CHAINS))
    seconds = time.monotonic() - started
    pids = output(pipeline, pipeline_run, 'b19')['pids']
    print(f'20 blocks in one worker: {seconds:.2f} s, {len(set(pids))} processes')
    # One process for the twenty blocks: Python's start and the imports once, not twenty times.
    assert len(set(pids)) == 1, pids
    assert seconds < 20, seconds
