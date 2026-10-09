"""
Feast as a microservice with PostgreSQL as registry, offline store and online store.

Features are read from the online store and, through the service's
/get-historical-features route, from the offline store as of given times. They are
written to the online store with /push and /write-to-online-store, and to the offline
store by Mage's PostgreSQL exporter followed by /materialize. Tests write their own
driver ids, so the seeded drivers never change.

Behaviors pinned here because they cause silent errors in pipelines:
- the online store keeps the last write, not the latest event;
- the PostgreSQL offline store cannot take pushes, and a push to the online and offline
  stores fails after writing the online one;
- most invalid requests come back as 500 with a plain string body.
"""
import datetime as dt
import io
import random

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import requests

from integration_tests.services.feast import dataset

FEATURES = ['conv_rate', 'acc_rate', 'avg_daily_trips', 'city', 'active']
LATEST = dataset.START + dt.timedelta(hours=dataset.HOURS - 1)
UTC = dt.timezone.utc


def new_driver() -> int:
    return random.randint(10**8, 10**9)


def online(feast_url, drivers, features=None, **extra):
    """Online features as {feature: [values]}, mapped by the names in the response."""
    body = dict(entities=dict(driver_id=drivers), **extra)
    if features is None and 'feature_service' not in extra:
        features = [f'driver_hourly_stats:{f}' for f in FEATURES]
    if features is not None:
        body['features'] = features
    response = requests.post(f'{feast_url}/get-online-features', json=body, timeout=30)
    response.raise_for_status()
    payload = response.json()
    names = payload['metadata']['feature_names']
    return {
        name: dict(
            values=result['values'],
            statuses=result['statuses'],
            event_timestamps=result['event_timestamps'],
        )
        for name, result in zip(names, payload['results'])
    }


def push(feast_url, rows, to='online'):
    frame = {column: [row[column] for row in rows] for column in rows[0]}
    return requests.post(
        f'{feast_url}/push',
        json=dict(push_source_name='driver_stats_push', to=to, df=frame),
        timeout=30,
    )


def feature_row(driver, when, **values):
    row = dict(
        driver_id=driver,
        event_timestamp=when.isoformat(),
        created=when.isoformat(),
        conv_rate=0.5,
        acc_rate=0.25,
        avg_daily_trips=2**53 + 1,
        city='Zürich',
        active=True,
    )
    row.update(values)
    return row


def test_registry_lists_the_repository(feast_url):
    registry = requests.get(f'{feast_url}/registry', timeout=10).json()

    assert registry['project'] == 'mage_it'
    assert registry['entities'] == [
        dict(name='driver', join_keys=['driver_id'], value_type='INT64'),
    ]
    view = registry['feature_views'][0]
    assert view['name'] == 'driver_hourly_stats' and view['online']
    assert view['features'] == dict(
        conv_rate='Float32', acc_rate='Float64', avg_daily_trips='Int64', city='String',
        active='Bool',
    )
    assert registry['feature_services'] == [
        dict(name='driver_activity', feature_views=['driver_hourly_stats']),
    ]
    assert {s['type'] for s in registry['data_sources']} == {'PostgreSQLSource', 'PushSource'}


def test_online_features_are_the_latest_seeded_values(feast_url):
    result = online(feast_url, dataset.DRIVERS)

    for feature in FEATURES:
        expected = [dataset.value_at(d, LATEST)[feature] for d in dataset.DRIVERS]
        assert result[feature]['values'] == expected, feature
        assert set(result[feature]['statuses']) == {'PRESENT'}
        assert set(result[feature]['event_timestamps']) == {'2024-01-02T23:00:00Z'}


def test_integers_above_2_53_stay_exact(feast_url):
    values = online(feast_url, [1005])['avg_daily_trips']['values']

    assert values == [1005 * 10**13 + dataset.HOURS - 1]
    assert values[0] > 2**53


def test_feature_service_and_full_feature_names(feast_url):
    by_service = online(feast_url, [1001], feature_service='driver_activity')
    full = online(
        feast_url, [1001], features=['driver_hourly_stats:conv_rate'], full_feature_names=True,
    )

    assert set(by_service) == {'driver_id', *FEATURES}
    assert set(full) == {'driver_id', 'driver_hourly_stats__conv_rate'}


def test_results_follow_the_response_names_not_the_request_order(feast_url):
    """Clients must map values through metadata.feature_names."""
    response = requests.post(
        f'{feast_url}/get-online-features',
        json=dict(
            features=[f'driver_hourly_stats:{f}' for f in FEATURES],
            entities=dict(driver_id=[1001]),
        ),
        timeout=30,
    ).json()

    assert response['metadata']['feature_names'][0] == 'driver_id'
    assert response['metadata']['feature_names'][1:] != FEATURES


def test_unknown_entities_are_not_found(feast_url):
    result = online(feast_url, [1001, 424242])

    assert result['conv_rate']['statuses'] == ['PRESENT', 'NOT_FOUND']
    assert result['conv_rate']['values'][1] is None


def test_push_to_the_online_store(feast_url):
    driver = new_driver()
    when = dt.datetime(2025, 3, 1, 12, 30, 15, tzinfo=UTC)

    response = push(feast_url, [feature_row(driver, when, city='São Paulo 東京', active=None)])

    assert response.status_code == 200, response.text
    result = online(feast_url, [driver])
    assert result['avg_daily_trips']['values'] == [2**53 + 1]
    assert result['city']['values'] == ['São Paulo 東京']
    assert result['active']['values'] == [None]
    assert result['conv_rate']['event_timestamps'] == ['2025-03-01T12:30:15Z']


def test_write_to_online_store(feast_url):
    driver = new_driver()
    row = feature_row(driver, dt.datetime(2025, 3, 1, tzinfo=UTC), conv_rate=0.125)

    response = requests.post(
        f'{feast_url}/write-to-online-store',
        json=dict(
            feature_view_name='driver_hourly_stats',
            df={column: [value] for column, value in row.items()},
        ),
        timeout=30,
    )

    assert response.status_code == 200, response.text
    assert online(feast_url, [driver])['conv_rate']['values'] == [0.125]


def test_an_older_event_pushed_later_replaces_the_newer_one(feast_url):
    """
    The online store keeps the last write. A late or retried push of an older event
    takes the place of a newer value.
    """
    driver = new_driver()
    push(feast_url, [feature_row(driver, dt.datetime(2025, 6, 1, tzinfo=UTC), conv_rate=0.75)])

    push(feast_url, [feature_row(driver, dt.datetime(2025, 1, 1, tzinfo=UTC), conv_rate=0.25)])

    result = online(feast_url, [driver])['conv_rate']
    assert result['values'] == [0.25]
    assert result['event_timestamps'] == ['2025-01-01T00:00:00Z']


def test_pushing_to_the_offline_store_is_not_supported(feast_url, feast_offline):
    """The PostgreSQL offline store has no offline_write_batch."""
    driver = new_driver()

    response = push(
        feast_url, [feature_row(driver, dt.datetime(2025, 3, 1, tzinfo=UTC))], to='offline',
    )

    assert response.status_code == 500
    assert response.text in ('', '""')
    count = feast_offline.load(
        f'SELECT count(*) AS n FROM driver_hourly_stats WHERE driver_id = {driver}',
        verbose=False,
    )
    assert count['n'].iloc[0] == 0


def test_a_failed_online_and_offline_push_still_writes_online(feast_url, feast_offline):
    """The client sees an error, the online store changed, the offline store did not."""
    driver = new_driver()

    response = push(
        feast_url,
        [feature_row(driver, dt.datetime(2025, 3, 1, tzinfo=UTC))],
        to='online_and_offline',
    )

    assert response.status_code == 500
    assert online(feast_url, [driver])['conv_rate']['statuses'] == ['PRESENT']
    count = feast_offline.load(
        f'SELECT count(*) AS n FROM driver_hourly_stats WHERE driver_id = {driver}',
        verbose=False,
    )
    assert count['n'].iloc[0] == 0


@pytest.mark.parametrize('body, status', [
    (dict(features=['missing_view:x'], entities=dict(driver_id=[1001])), 404),
    (dict(feature_service='missing', entities=dict(driver_id=[1001])), 404),
    (dict(features=['driver_hourly_stats:missing'], entities=dict(driver_id=[1001])), 500),
    (dict(features=['driver_hourly_stats:conv_rate'], entities=dict(other=[1])), 500),
])
def test_online_read_errors(feast_url, body, status):
    response = requests.post(f'{feast_url}/get-online-features', json=body, timeout=30)

    assert response.status_code == status, response.text


@pytest.mark.parametrize('df, to, status', [
    # A value of the wrong type and missing feature columns are client errors, but the
    # feature server answers 500.
    (dict(feature_row(1, dt.datetime(2025, 1, 1, tzinfo=UTC), conv_rate='high')), 'online', 500),
    (dict(driver_id=1, event_timestamp='2025-01-01T00:00:00Z'), 'online', 500),
    (dict(feature_row(1, dt.datetime(2025, 1, 1, tzinfo=UTC))), 'nowhere', 500),
])
def test_push_errors(feast_url, df, to, status):
    response = requests.post(
        f'{feast_url}/push',
        json=dict(push_source_name='driver_stats_push', to=to,
                  df={k: [v] for k, v in df.items()}),
        timeout=30,
    )

    assert response.status_code == status, response.text


def test_unknown_push_source_is_rejected(feast_url):
    response = requests.post(
        f'{feast_url}/push', json=dict(push_source_name='missing', df={}), timeout=30,
    )

    assert response.status_code == 422


def historical(feast_url, rows, fmt='json', **extra):
    response = requests.post(
        f'{feast_url}/get-historical-features',
        json=dict(entity_rows=rows, format=fmt, **extra),
        timeout=60,
    )
    response.raise_for_status()
    return response


def test_historical_features_are_point_in_time(feast_url):
    times = [
        dataset.START - dt.timedelta(hours=1),
        dataset.START,
        dataset.START + dt.timedelta(minutes=59),
        dataset.START + dt.timedelta(hours=7, minutes=30),
        LATEST + dt.timedelta(days=30),
    ]
    rows = [
        dict(driver_id=driver, event_timestamp=when.isoformat())
        for when in times for driver in (1001, 1004)
    ]

    result = historical(feast_url, rows, feature_service='driver_activity').json()

    for row in result:
        when = dt.datetime.fromisoformat(row['event_timestamp'])
        expected = dataset.value_at(row['driver_id'], when)
        for feature in FEATURES:
            assert row[feature] == (expected[feature] if expected else None), (row, feature)
    assert len(result) == len(rows)


def test_historical_features_as_arrow_keep_types(feast_url):
    rows = [dict(driver_id=d, event_timestamp=LATEST.isoformat()) for d in dataset.DRIVERS]

    content = historical(
        feast_url, rows, fmt='arrow', features=[f'driver_hourly_stats:{f}' for f in FEATURES],
    ).content
    frame = pl.read_ipc_stream(io.BytesIO(content))
    pandas_frame = pa.ipc.open_stream(io.BytesIO(content)).read_pandas(
        types_mapper=pd.ArrowDtype,
    )

    assert frame.schema['avg_daily_trips'] == pl.Int64
    assert frame.schema['conv_rate'] == pl.Float32
    assert frame.schema['active'] == pl.Boolean
    assert frame.schema['event_timestamp'] == pl.Datetime('us', 'UTC')
    assert frame['avg_daily_trips'].to_list() == [
        dataset.value_at(d, LATEST)['avg_daily_trips'] for d in dataset.DRIVERS
    ]
    assert pandas_frame['avg_daily_trips'].tolist() == frame['avg_daily_trips'].to_list()
    assert str(pandas_frame['event_timestamp'].dtype) == 'timestamp[us, tz=UTC][pyarrow]'


def test_historical_request_errors(feast_url):
    missing_time = requests.post(
        f'{feast_url}/get-historical-features',
        json=dict(entity_rows=[dict(driver_id=1001)], features=['driver_hourly_stats:conv_rate']),
        timeout=30,
    )
    both = requests.post(
        f'{feast_url}/get-historical-features',
        json=dict(entity_rows=[dict(driver_id=1001, event_timestamp=LATEST.isoformat())],
                  features=['driver_hourly_stats:conv_rate'], feature_service='driver_activity'),
        timeout=30,
    )

    assert missing_time.status_code == 422
    assert both.status_code == 422


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_offline_rows_written_by_mage_materialize_online(feast_url, feast_offline, engine):
    """
    Mage writes feature rows to the offline store's table, /materialize loads them into
    the online store, and both stores return them.
    """
    drivers = [new_driver(), new_driver()]
    feast_offline.written_drivers.extend(drivers)
    start = dt.datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        dict(
            driver_id=driver,
            event_timestamp=start + dt.timedelta(hours=hour),
            created=start + dt.timedelta(hours=hour),
            conv_rate=hour * 0.5,
            acc_rate=1 / 3,
            avg_daily_trips=2**60 + hour,
            city='Ñuñoa' if hour else '',
            active=None if hour == 1 else bool(hour % 2),
        )
        for driver in drivers for hour in range(3)
    ]
    frame = pd.DataFrame(rows) if engine == 'pandas' else pl.DataFrame(rows)

    feast_offline.export(
        frame, schema_name='public', table_name='driver_hourly_stats', if_exists='append',
        verbose=False,
    )
    response = requests.post(
        f'{feast_url}/materialize',
        json=dict(
            start_ts=(start - dt.timedelta(minutes=1)).isoformat(),
            end_ts=(start + dt.timedelta(hours=3)).isoformat(),
            feature_views=['driver_hourly_stats'],
        ),
        timeout=120,
    )

    assert response.status_code == 200, response.text
    latest = online(feast_url, drivers)
    assert latest['avg_daily_trips']['values'] == [2**60 + 2] * 2
    assert latest['conv_rate']['values'] == [1.0, 1.0]
    assert latest['city']['values'] == ['Ñuñoa', 'Ñuñoa']
    assert latest['active']['values'] == [False, False]

    past = historical(feast_url, [
        dict(driver_id=drivers[0], event_timestamp=(start + dt.timedelta(minutes=30)).isoformat()),
        dict(driver_id=drivers[0], event_timestamp=(start + dt.timedelta(hours=1)).isoformat()),
    ], features=[f'driver_hourly_stats:{f}' for f in FEATURES]).json()
    assert [r['city'] for r in past] == ['', 'Ñuñoa']
    assert [r['active'] for r in past] == [False, None]
    assert [r['acc_rate'] for r in past] == [1 / 3, 1 / 3]
