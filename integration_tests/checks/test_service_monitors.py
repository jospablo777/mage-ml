"""
Monitors in an exported service: the engine profiles the block's output after each run and
compares it with its earlier runs, with the code MageML runs monitors with, so the same data
gets the same checks. A failed monitor with fail_block fails the block.
"""
import json
import shutil
import time
import urllib.request
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    checks_service_binary,
    export_service,
    run_service,
)
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build

ORDERS = '''
import numpy as np
import pandas as pd


@data_loader
def load(*args, **kwargs):
    shift = float(kwargs.get('shift', 0))
    rng = np.random.default_rng(int(kwargs.get('seed', 1)))
    return pd.DataFrame({
        'score': rng.normal(shift, 1, 400),
        'segment': rng.choice(['a', 'b', 'c'] if shift == 0 else ['c', 'd'], 400),
    })
'''

KEEP = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''

MONITOR = '''pipeline: {pipeline}
block: {block}
columns: [score, segment]
min_rows: 10
rows: {{min: 10}}
missing: {{max: 0.1}}
fail_block: {fail_block}
'''


@pytest.fixture
def monitors_folder(mage_project):
    folder = Path(mage_project) / 'monitors'
    folder.mkdir(exist_ok=True)
    yield folder
    shutil.rmtree(folder, ignore_errors=True)


def pipeline_with_monitor(mage_project, folder: Path, fail_block: bool) -> str:
    uuid = build(mage_project, [
        dict(uuid='orders', type='data_loader', code=ORDERS),
        dict(uuid='keep', type='transformer', code=KEEP, upstream=['orders']),
    ])
    (folder / f'{uuid}.yaml').write_text(MONITOR.format(
        pipeline=uuid, block=block_uuid(uuid, 'orders'), fail_block=str(fail_block).lower(),
    ))
    return uuid


def mageml_record(uuid: str, **variables):
    """The monitor record of a MageML run of the pipeline."""
    from mage_ai.orchestration.fusion_verify import run_in_process
    from mage_ai.orchestration.triggers.api import trigger_pipeline

    run = run_in_process(trigger_pipeline(uuid, variables=variables), log=lambda _line: None)
    block_run = next(b for b in run.block_runs if b.block_uuid == block_uuid(uuid, 'orders'))
    return block_run.metrics['monitors'][0]


def service_records(out: Path, uuid: str):
    folder = out / 'data' / 'monitors' / uuid / uuid
    return [json.loads(p.read_text()) for p in sorted(folder.glob('*.json'))]


def checks(record):
    return [(c['check'], c['column'], c['result'], c['value']) for c in record['checks']]


@pytest.mark.timeout(1800)
def test_a_service_monitor_finds_the_drift_mageml_finds(mage_project, monitors_folder,
                                                        tmp_path):
    binary = checks_service_binary()
    uuid = pipeline_with_monitor(mage_project, monitors_folder, fail_block=False)
    directory = export_service(mage_project, uuid, tmp_path)
    entry = next(b for b in json.loads((directory / 'service.json').read_text())
                 ['pipelines'][0]['blocks'] if b.get('monitors'))
    assert entry['monitors'][0]['name'] == uuid

    for shift in (0, 3):
        run = run_service(directory, uuid, tmp_path, dict(shift=shift), binary=binary)
        assert {b['status'] for b in run['blocks']} == {'completed'}, run
    first, second = service_records(tmp_path, uuid)
    assert first['result'] == 'hold'  # no baseline yet
    assert second['result'] == 'fail', second['checks']
    drifted = {c['column'] for c in second['checks'] if c['check'] == 'drift'
               and c['result'] == 'fail'}
    assert drifted == {'score', 'segment'}
    logs = '\n'.join(p.read_text() for p in (tmp_path / 'data').rglob('*.log'))
    assert f'Monitor {uuid}: fail (against run' in logs, logs[-3000:]
    assert list((tmp_path / 'data').rglob('monitors.json'))

    # MageML on the same data: the same checks, value for value.
    mageml_record(uuid, shift=0)
    assert checks(mageml_record(uuid, shift=3)) == checks(second)

    # Served again, the engine shows the monitor's runs in its snapshot (the console's
    # source) and its last result in /metrics.
    service = RunningService(binary, directory, tmp_path / 'data')
    service.start()
    try:
        snapshot = None
        for _ in range(120):
            try:
                snapshot = service.get('/v1/snapshot')
                break
            except OSError:
                time.sleep(0.5)
        assert snapshot is not None, service.log.read_text()[-3000:]
        shown = snapshot['pipelines'][0]['monitors'][0]
        assert (shown['name'], shown['result'], shown['history']) == (uuid, 'fail',
                                                                       ['hold', 'fail'])
        assert 'drift' in shown['detail']
        request = urllib.request.Request(
            f'http://127.0.0.1:{service.port}/metrics',
            headers={'Authorization': f'Bearer {TOKEN}'},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            metrics = response.read().decode()
        block = block_uuid(uuid, 'orders')
        assert (f'mage_service_monitor_result{{pipeline="{uuid}",block="{block}",'
                f'monitor="{uuid}"}} 3') in metrics, metrics[-2000:]
        assert 'mage_service_monitor_check{' in metrics
    finally:
        service.stop()


@pytest.mark.timeout(1800)
def test_a_failing_monitor_with_fail_block_fails_the_block_in_the_service(
    mage_project, monitors_folder, tmp_path,
):
    binary = checks_service_binary()
    uuid = pipeline_with_monitor(mage_project, monitors_folder, fail_block=True)
    directory = export_service(mage_project, uuid, tmp_path)

    run_service(directory, uuid, tmp_path, dict(shift=0), binary=binary)
    run = run_service(directory, uuid, tmp_path, dict(shift=3), binary=binary, complete=False)

    statuses = {b['block'].rsplit('_', 1)[0]: b['status'] for b in run['blocks']}
    assert statuses['orders'] == 'failed', run
    assert statuses.get('keep') != 'completed', run
    assert f'Monitor {uuid}: fail' in json.dumps(run)
    logs = '\n'.join(p.read_text() for p in (tmp_path / 'data').rglob('*.log'))
    assert 'Attempt 2 of' not in logs
