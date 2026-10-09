"""
Mage pipelines that pull features from Feast and push features back.

feast_online_pandas reads online features through the feature service into pandas,
derives features for other drivers and pushes them to the online store.
feast_offline_polars reads historical features as an Arrow stream into Polars, derives
rows for other drivers, writes them to the offline store with Mage's PostgreSQL exporter
and materializes them into the online store.
"""
import datetime as dt
import secrets

import requests

from integration_tests.feast.test_feature_server import (
    FEATURES,
    LATEST,
    historical,
    online,
)
from integration_tests.mage_runner import run_pipeline
from integration_tests.services.feast import dataset


def test_online_features_pulled_and_pushed_with_pandas(mage_project, feast_url):
    offset = 10**9 + secrets.randbelow(10**12)
    when = '2025-05-05T05:05:05+00:00'

    run_pipeline('feast_online_pandas', feast_url=feast_url, driver_offset=offset,
                 event_timestamp=when)

    result = online(feast_url, [d + offset for d in dataset.DRIVERS])
    for index, driver in enumerate(dataset.DRIVERS):
        source = dataset.value_at(driver, LATEST)
        assert result['conv_rate']['values'][index] == source['conv_rate'] * 2
        assert result['avg_daily_trips']['values'][index] == source['avg_daily_trips'] + 1
        assert result['city']['values'][index] == source['city'].upper()
        expected_active = None if source['active'] is None else not source['active']
        assert result['active']['values'][index] == expected_active
    assert set(result['conv_rate']['event_timestamps']) == {'2025-05-05T05:05:05Z'}


def test_offline_features_written_and_materialized_with_polars(
    mage_project, feast_url, feast_offline,
):
    offset = 10**9 + secrets.randbelow(10**12)
    drivers = [d + offset for d in (1001, 1002, 1003)]
    feast_offline.written_drivers.extend(drivers)

    run_pipeline('feast_offline_polars', feast_url=feast_url, driver_offset=offset)

    def derived(driver, hour):
        source = dataset.value_at(driver, dt.datetime(2024, 1, 1, hour, 30, tzinfo=dt.timezone.utc))
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

    when = dt.datetime(2026, 1, 1, 2, 45, tzinfo=dt.timezone.utc)
    past = historical(
        feast_url,
        [dict(driver_id=drivers[0], event_timestamp=when.isoformat())],
        features=[f'driver_hourly_stats:{f}' for f in FEATURES],
    ).json()
    assert {f: past[0][f] for f in FEATURES} == derived(1001, 2)
    rows = feast_offline.load(
        'SELECT count(*) AS n FROM driver_hourly_stats WHERE driver_id = ANY(%(ids)s)',
        verbose=False,
        params=dict(ids=drivers),
    )
    assert rows['n'].iloc[0] == 18
    assert requests.get(f'{feast_url}/health', timeout=10).status_code == 200
