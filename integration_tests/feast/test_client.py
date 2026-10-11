"""
MageML's Feast client (mage_ai/feature_stores/feast.py) against Feast's own routes.

Each test pushes through stock /push, where a raw request loses values (see
test_push_pull.py), and checks that the client's requests keep them exact.
"""
import datetime as dt
import secrets

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from mage_ai.feature_stores.feast import FeastClient, FeastError

UTC = dt.timezone.utc
FEATURES = [f'driver_hourly_stats:{f}' for f in
            ('conv_rate', 'acc_rate', 'avg_daily_trips', 'city', 'active')]


def new_drivers(count: int = 1):
    start = 10**9 + secrets.randbelow(10**12)
    return list(range(start, start + count))


@pytest.fixture
def feast(feast_url):
    with FeastClient(feast_url, timeout=120) as client:
        yield client


def frame(drivers, when, **columns):
    data = dict(
        driver_id=drivers,
        event_timestamp=when,
        created=when,
        conv_rate=[0.5] * len(drivers),
        acc_rate=[0.25] * len(drivers),
        avg_daily_trips=[1] * len(drivers),
        city=['c'] * len(drivers),
        active=[True] * len(drivers),
    )
    data.update(columns)
    return pa.table({
        name: pa.array(values, pa.timestamp('us', tz='UTC'))
        if name in ('event_timestamp', 'created') else values
        for name, values in data.items()
    })


def test_large_integers_next_to_nulls_stay_exact(feast):
    first, second = new_drivers(2)
    when = [dt.datetime(2025, 1, 1, tzinfo=UTC)] * 2

    feast.push('driver_stats_push', frame(
        [first, second], when, avg_daily_trips=pa.array([2**60 + 1, None], pa.int64()),
    ))

    read = feast.get_online_features({'driver_id': [first, second]}, FEATURES, output='polars')
    # A raw /push of these rows stores 2**60.
    assert read['avg_daily_trips'].to_list() == [2**60 + 1, None]


def test_the_latest_event_wins_whatever_the_row_order(feast):
    driver, = new_drivers()
    newer, older = dt.datetime(2025, 1, 2, tzinfo=UTC), dt.datetime(2025, 1, 1, tzinfo=UTC)

    feast.push('driver_stats_push', frame(
        [driver, driver], [newer, older], conv_rate=[0.75, 0.125],
    ))

    read = feast.get_online_features({'driver_id': [driver]}, FEATURES, output='polars',
                                     with_metadata=True)
    # A raw /push keeps the last row, the older event.
    assert read['conv_rate'].to_list() == [0.75]
    assert read['conv_rate__event_timestamp'][0] == newer


def test_pandas_frames_round_trip_with_their_types(feast):
    drivers = new_drivers(3)
    when = pd.Timestamp('2025-03-01T12:00:00Z')
    written = pd.DataFrame({
        'driver_id': drivers,
        'event_timestamp': [when] * 3,
        'created': [when] * 3,
        'conv_rate': [0.125, None, 0.75],
        'acc_rate': [0.5, 0.5, 0.5],
        'avg_daily_trips': pd.array([2**55 + 3, 7, None], 'Int64'),
        'city': ['San José', None, 'Limón'],
        'active': pd.array([True, None, False], 'boolean'),
    })

    feast.push('driver_stats_push', written)

    read = feast.get_online_features({'driver_id': drivers}, FEATURES)
    assert read['driver_id'].tolist() == drivers
    assert read['avg_daily_trips'].tolist()[:2] == [2**55 + 3, 7]
    assert pd.isna(read['avg_daily_trips'].tolist()[2])
    assert read['city'].tolist()[0] == 'San José' and pd.isna(read['city'].tolist()[1])
    assert read['conv_rate'].tolist()[0] == 0.125


def test_write_to_online_store_and_feature_service(feast):
    driver, = new_drivers()
    feast.write_to_online_store('driver_hourly_stats', pl.DataFrame({
        'driver_id': [driver],
        'event_timestamp': [dt.datetime(2025, 2, 1, tzinfo=UTC)],
        'created': [dt.datetime(2025, 2, 1, tzinfo=UTC)],
        'conv_rate': [0.375], 'acc_rate': [0.5], 'avg_daily_trips': [4],
        'city': ['Cartago'], 'active': [False],
    }))

    read = feast.get_online_features({'driver_id': [driver]}, feature_service='driver_activity',
                                     output='dict')
    assert read['conv_rate'] == [0.375]


def test_feast_errors_name_the_route_and_carry_the_body(feast):
    with pytest.raises(FeastError) as raised:
        feast.get_online_features({'driver_id': [1]}, ['no_such_view:conv_rate'])
    assert raised.value.route == '/get-online-features'
    assert raised.value.status in (404, 500)
    assert raised.value.body
    assert feast.health()
