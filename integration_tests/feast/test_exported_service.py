"""
A pipeline that reads online features from the Feast feature server and pushes features
back, exported with `mage export service` and run by mage-service without Mage. Its
results must be those of the same pipeline run by Mage (test_pipelines.py).
"""
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

import pytest

from integration_tests.feast.test_feature_server import LATEST, online
from integration_tests.services.feast import dataset

REPO = Path(__file__).resolve().parents[2]
SERVICE = REPO / 'mage_ai' / 'pipeline_services' / 'service'
BINARY = SERVICE / 'target' / 'debug' / 'mage-service'


@pytest.fixture(scope='session')
def service_binary():
    if not BINARY.is_file():
        subprocess.run(['cargo', 'build', '-p', 'mage-service'], cwd=SERVICE, check=True)
    return BINARY


def test_an_exported_pipeline_pulls_and_pushes_features_like_mage(
    mage_project, feast_url, service_binary, tmp_path,
):
    from mage_ai.pipeline_services import export

    captured = export.capture(mage_project, ['feast_online_pandas'], name='features')
    out = export.write(captured, str(tmp_path / 'service'), force=True)
    assert 'requests' in captured.requirements
    offset = 10**9 + secrets.randbelow(10**12)
    when = '2025-05-05T05:05:05+00:00'

    result = subprocess.run(
        [
            str(service_binary), 'run', 'feast_online_pandas', '--json',
            '--var', f'feast_url={feast_url}',
            '--var', f'driver_offset={offset}',
            '--var', f'event_timestamp={when}',
        ],
        capture_output=True, text=True, timeout=300,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_WORKER=str(REPO / 'mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=str(REPO),
        ),
    )
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]
    run = json.loads(result.stdout[result.stdout.index('{'):])
    assert [b['status'] for b in run['blocks']] == ['completed'] * 3

    pushed = online(feast_url, [d + offset for d in dataset.DRIVERS])
    for index, driver in enumerate(dataset.DRIVERS):
        source = dataset.value_at(driver, LATEST)
        assert pushed['conv_rate']['values'][index] == source['conv_rate'] * 2
        assert pushed['avg_daily_trips']['values'][index] == source['avg_daily_trips'] + 1
        assert pushed['city']['values'][index] == source['city'].upper()
    assert set(pushed['conv_rate']['event_timestamps']) == {'2025-05-05T05:05:05Z'}


def test_an_exported_pipeline_writes_offline_features_like_mage(
    mage_project, feast_url, feast_offline, service_binary, tmp_path,
):
    """Historical features as an Arrow stream into Polars, Mage's PostgreSQL exporter and
    materialization, all in the service."""
    import datetime as dt

    from mage_ai.pipeline_services import export

    out = export.write(
        export.capture(mage_project, ['feast_offline_polars'], name='offline-features'),
        str(tmp_path / 'service'), force=True,
    )
    offset = 10**9 + secrets.randbelow(10**12)
    drivers = [d + offset for d in (1001, 1002, 1003)]
    feast_offline.written_drivers.extend(drivers)

    result = subprocess.run(
        [str(service_binary), 'run', 'feast_offline_polars', '--json',
         '--var', f'feast_url={feast_url}', '--var', f'driver_offset={offset}'],
        capture_output=True, text=True, timeout=600,
        env=dict(
            os.environ,
            MAGE_SERVICE_DIR=str(out),
            MAGE_SERVICE_WORKER=str(REPO / 'mage_ai/pipeline_services/runtime/worker.py'),
            MAGE_SERVICE_PYTHON=sys.executable,
            PYTHONPATH=str(REPO),
        ),
    )
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-4000:]

    def derived(driver, hour):
        source = dataset.value_at(
            driver, dt.datetime(2024, 1, 1, hour, 30, tzinfo=dt.timezone.utc),
        )
        return dict(
            conv_rate=source['conv_rate'] + 0.25,
            acc_rate=source['acc_rate'] * 2,
            avg_daily_trips=source['avg_daily_trips'] * 2,
            city=source['city'][::-1],
            active=True if source['active'] is None else source['active'],
        )

    latest = online(feast_url, drivers)
    for index, driver in enumerate((1001, 1002, 1003)):
        for feature, value in derived(driver, 5).items():
            assert latest[feature]['values'][index] == value, (driver, feature)
    rows = feast_offline.load(
        'SELECT count(*) AS n FROM driver_hourly_stats WHERE driver_id = ANY(%(ids)s)',
        verbose=False, params=dict(ids=drivers),
    )
    assert rows['n'].iloc[0] == 18
