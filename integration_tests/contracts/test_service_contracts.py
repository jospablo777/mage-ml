"""
Data contracts in an exported service: the engine checks a block's output against the
contract MageML checks it against, with the same outcome. A broken `fail` contract fails
the block without retries and stops what is downstream; `warn` logs and goes on. Both keep
the report as contract.json.
"""
import json
import shutil
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    contract_service_binary,
    export_service,
    run_service,
)
from integration_tests.mage_runner import execute_pipeline
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build, error_of

SCORES = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    duplicate = kwargs.get('duplicate') in (True, 'true')
    return pd.DataFrame({
        'customer_id': [1, 2, 2 if duplicate else 3],
        'score': [0.25, 0.5, 0.75],
    })
'''

KEEP = '''
@transformer
def keep(rows, *args, **kwargs):
    return rows
'''

CONTRACT = '''
version: 1.4.0
columns:
  customer_id: {type: integer, nullable: false}
  score: {type: float, min: 0, max: 1}
unique: [customer_id]
'''


@pytest.fixture
def contract(mage_project):
    folder = Path(mage_project) / 'contracts'
    folder.mkdir(exist_ok=True)
    (folder / 'scores.yaml').write_text(CONTRACT)
    yield
    shutil.rmtree(folder, ignore_errors=True)


def pipeline(mage_project, enforcement: str) -> str:
    """The scores block has a contract and two retries, which a violation does not use."""
    from mage_ai.data_preparation.models.pipeline import Pipeline

    uuid = build(mage_project, [
        dict(uuid='scores', type='data_loader', code=SCORES,
             config=dict(contract=dict(name='scores', enforcement=enforcement, version='1'))),
        dict(uuid='keep', type='transformer', code=KEEP, upstream=['scores']),
    ])
    pipeline = Pipeline.get(uuid)
    pipeline.get_block(block_uuid(uuid, 'scores')).retry_config = dict(retries=2, delay=0)
    pipeline.save()
    return uuid


def statuses(run) -> dict:
    return {b['block'].rsplit('_', 1)[0]: b['status'] for b in run['blocks']}


@pytest.mark.timeout(1800)
def test_a_broken_contract_fails_the_block_in_the_service_as_in_mageml(
    mage_project, contract, tmp_path,
):
    binary = contract_service_binary()
    uuid = pipeline(mage_project, 'fail')
    directory = export_service(mage_project, uuid, tmp_path)
    contract_check = next(
        b['contract'] for b in json.loads((directory / 'service.json').read_text())
        ['pipelines'][0]['blocks'] if 'contract' in b
    )
    assert contract_check['name'] == 'scores'

    run = run_service(directory, uuid, tmp_path, dict(duplicate='true'), binary=binary,
                      complete=False)
    assert statuses(run)['scores'] == 'failed', run
    assert statuses(run).get('keep') != 'completed', run
    error = json.dumps(run)
    assert 'Contract scores' in error and 'customer_id unique (2 rows)' in error, error
    # No retries: the same data breaks the same rules on every attempt.
    logs = '\n'.join(p.read_text() for p in (tmp_path / 'data').rglob('*.log'))
    assert 'Attempt 2 of' not in logs
    reports = list((tmp_path / 'data').rglob('contract.json'))
    assert reports, 'no contract.json'
    report = json.loads(reports[0].read_text())
    assert report['passed'] is False and report['rows'] == 3

    mageml = execute_pipeline(uuid, duplicate='true')
    assert mageml.status.value == 'failed'
    assert 'broke contract scores' in error_of(mageml, block_uuid(uuid, 'scores'))

    # Data that meets the contract runs through.
    run = run_service(directory, uuid, tmp_path, dict(duplicate='false'), binary=binary)
    assert set(statuses(run).values()) == {'completed'}


@pytest.mark.timeout(1800)
def test_a_warn_contract_logs_the_violation_and_the_run_goes_on(
    mage_project, contract, tmp_path,
):
    binary = contract_service_binary()
    uuid = pipeline(mage_project, 'warn')
    directory = export_service(mage_project, uuid, tmp_path)

    run = run_service(directory, uuid, tmp_path, dict(duplicate='true'), binary=binary)

    assert set(statuses(run).values()) == {'completed'}
    logs = '\n'.join(p.read_text() for p in (tmp_path / 'data').rglob('*.log'))
    assert 'Contract scores' in logs and '"warning"' in logs, logs[-3000:]
    assert execute_pipeline(uuid, duplicate='true').status.value == 'completed'
