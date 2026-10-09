"""
The Mage project of the scheduler soak test, written to a temporary directory.

Chain pipelines share one set of block files. Blocks record each run in PostgreSQL, in
the schema named by SOAK_SCHEMA, so the test can check that every run executed once.
"""
import textwrap
from pathlib import Path
from typing import Dict, List

import yaml

CONNECT = '''
import os

import psycopg2


def connect():
    return psycopg2.connect(
        host=os.environ['MAGE_TEST_POSTGRES_HOST'],
        port=os.environ['MAGE_TEST_POSTGRES_PORT'],
        dbname=os.environ['MAGE_TEST_POSTGRES_DBNAME'],
        user=os.environ['MAGE_TEST_POSTGRES_USER'],
        password=os.environ['MAGE_TEST_POSTGRES_PASSWORD'],
    )


def record(table, **values):
    schema = os.environ['SOAK_SCHEMA']
    columns = ', '.join(values)
    placeholders = ', '.join(['%s'] * len(values))
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f'INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders}) RETURNING 1',
            list(values.values()),
        )
'''

BLOCKS = {
    ('data_loaders', 'soak_load'): '''
import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    return pl.DataFrame({'n': range(1000)}).with_columns(
        run=pl.lit(kwargs['pipeline_run_id']),
    )
''',
    ('transformers', 'soak_transform'): '''
import random
import time

import polars as pl

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame, *args, **kwargs):
    time.sleep(random.uniform(0, 0.3))
    return frame.lazy().with_columns(doubled=pl.col('n') * 2)
''',
    ('data_exporters', 'soak_export'): CONNECT + '''
import polars as pl

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs):
    if isinstance(frame, pl.LazyFrame):
        frame = frame.collect()
    record(
        'results',
        pipeline_uuid=kwargs.get('pipeline_uuid'),
        pipeline_run_id=kwargs['pipeline_run_id'],
        trigger_name=kwargs['trigger_name'],
        total=int(frame['doubled'].sum()),
        run_in_frame=int(frame['run'][0]),
    )
''',
    ('data_loaders', 'soak_flaky_load'): CONNECT + '''
import polars as pl

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    record('attempts', pipeline_run_id=kwargs['pipeline_run_id'])
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*) FROM {os.environ['SOAK_SCHEMA']}.attempts "
            'WHERE pipeline_run_id = %s',
            (kwargs['pipeline_run_id'],),
        )
        attempts = cursor.fetchone()[0]
    if attempts == 1:
        raise RuntimeError('first attempt fails')
    return pl.DataFrame({'n': range(1000)}).with_columns(
        run=pl.lit(kwargs['pipeline_run_id']),
    )
''',
    ('data_loaders', 'soak_fail_load'): '''
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    raise ValueError('this block always fails')
''',
    ('data_loaders', 'soak_fanout'): '''
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(**kwargs):
    items = list(range(6))
    return [items, [{'block_uuid': f'item_{i}'} for i in items]]
''',
    ('transformers', 'soak_double'): '''
import time

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(value, *args, **kwargs):
    time.sleep(0.5)
    return value * 2
''',
    ('data_exporters', 'soak_sum'): CONNECT + '''
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(values, **kwargs):
    record(
        'results',
        pipeline_uuid=kwargs.get('pipeline_uuid'),
        pipeline_run_id=kwargs['pipeline_run_id'],
        trigger_name=kwargs['trigger_name'],
        total=int(sum(values)),
        run_in_frame=kwargs['pipeline_run_id'],
    )
''',
}

# Sum of 2 * n for n in range(1000), and of 2 * i for i in range(6).
CHAIN_TOTAL = 999_000
FANOUT_TOTAL = 30
FANOUT_BLOCK_RUN_LIMIT = 2


def _block(uuid: str, block_type: str, upstream: List[str], downstream: List[str],
           **extra) -> Dict:
    return dict(
        all_upstream_blocks_executed=True,
        downstream_blocks=downstream,
        language='python',
        name=uuid,
        status='not_executed',
        type=block_type,
        upstream_blocks=upstream,
        uuid=uuid,
        **extra,
    )


def chain(loader: str = 'soak_load', **loader_extra) -> List[Dict]:
    return [
        _block(loader, 'data_loader', [], ['soak_transform'], **loader_extra),
        _block('soak_transform', 'transformer', [loader], ['soak_export']),
        _block('soak_export', 'data_exporter', ['soak_transform'], []),
    ]


def fanout() -> List[Dict]:
    return [
        _block('soak_fanout', 'data_loader', [], ['soak_double'],
               configuration=dict(dynamic=True)),
        _block('soak_double', 'transformer', ['soak_fanout'], ['soak_sum'],
               configuration=dict(reduce_output=True)),
        _block('soak_sum', 'data_exporter', ['soak_double'], []),
    ]


def trigger(name: str, interval: str, start_time: str) -> Dict:
    return dict(
        name=name,
        schedule_type='time',
        schedule_interval=interval,
        start_time=start_time,
        status='active',
    )


def write_project(root: Path, chains: int, start_time: str) -> Dict[str, List[Dict]]:
    """
    Write the project and return each pipeline's triggers. Each chain pipeline runs once
    at start and every minute; the others run at start and every minute too.
    """
    (root / 'pipelines').mkdir(parents=True)
    (root / 'metadata.yaml').write_text(yaml.safe_dump(dict(
        project_uuid='soak',
        variables_dir=str(root / 'mage_data'),
    )))
    (root / 'io_config.yaml').write_text('version: 0.1.1\ndefault: {}\n')
    for (folder, uuid), code in BLOCKS.items():
        (root / folder).mkdir(exist_ok=True)
        (root / folder / '__init__.py').touch()
        (root / folder / f'{uuid}.py').write_text(textwrap.dedent(code).lstrip())

    pipelines = {f'soak_chain_{i}': chain() for i in range(chains)}
    pipelines['soak_retry'] = chain(
        'soak_flaky_load', retry_config=dict(retries=2, delay=1),
    )
    pipelines['soak_fail'] = chain('soak_fail_load')
    pipelines['soak_fanout'] = fanout()

    triggers = {}
    for uuid, blocks in pipelines.items():
        folder = root / 'pipelines' / uuid
        folder.mkdir()
        metadata = dict(blocks=blocks, name=uuid, type='python', uuid=uuid)
        if uuid == 'soak_fanout':
            metadata['concurrency_config'] = dict(block_run_limit=FANOUT_BLOCK_RUN_LIMIT)
        (folder / 'metadata.yaml').write_text(yaml.safe_dump(metadata))
        (folder / '__init__.py').touch()
        triggers[uuid] = [
            trigger('once', '@once', start_time),
            trigger('every_minute', '* * * * *', start_time),
        ]
        set_triggers(root, uuid, triggers[uuid])
    return triggers


def set_triggers(root: Path, pipeline_uuid: str, triggers: List[Dict]) -> None:
    path = root / 'pipelines' / pipeline_uuid / 'triggers.yaml'
    path.write_text(yaml.safe_dump(dict(triggers=triggers)))
