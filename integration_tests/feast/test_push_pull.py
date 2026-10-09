"""
Pushing features to Feast and pulling them back, value by value.

The first group pins what Feast's own /push does to values; each of these is a silent
change, answered with 200. The second group checks the service's /push/typed route,
which keeps every value exact (see services/feast/typed_push.py). The last group reads
features in bulk and in the shapes pipelines use.
"""
import concurrent.futures
import datetime as dt
import random
import secrets

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import requests

FEATURES = ['conv_rate', 'acc_rate', 'avg_daily_trips', 'city', 'active']
UTC = dt.timezone.utc


def new_drivers(count: int = 1):
    # secrets, not random: pytest-randomly gives every test and every xdist worker the
    # same random seed, so random ids collide across tests.
    start = 10**9 + secrets.randbelow(10**12)
    return list(range(start, start + count))


def rows_to_columns(rows):
    return {column: [row[column] for row in rows] for column in rows[0]}


def row(driver, when='2025-01-01T00:00:00+00:00', **values):
    data = dict(
        driver_id=driver, event_timestamp=when, created=when, conv_rate=0.5, acc_rate=0.25,
        avg_daily_trips=1, city='c', active=True,
    )
    data.update(values)
    return data


def push(feast_url, rows, route='/push/typed', **extra):
    return requests.post(
        f'{feast_url}{route}',
        json=dict(push_source_name='driver_stats_push', df=rows_to_columns(rows), **extra),
        timeout=120,
    )


def pull(feast_url, drivers, features=FEATURES):
    """{feature: values} and {feature: event timestamps}, for each requested driver."""
    response = requests.post(
        f'{feast_url}/get-online-features',
        json=dict(
            features=[f'driver_hourly_stats:{f}' for f in features],
            entities=dict(driver_id=drivers),
        ),
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    names = payload['metadata']['feature_names']
    values = {n: r['values'] for n, r in zip(names, payload['results'])}
    stamps = {n: r['event_timestamps'] for n, r in zip(names, payload['results'])}
    return values, stamps


# Feast's /push ------------------------------------------------------------------------

def test_stock_push_rounds_large_integers_next_to_a_null(feast_url):
    """pandas turns the column into float, so 2**60 + 1 is stored as 2**60."""
    first, second = new_drivers(2)

    push(feast_url, [row(first, avg_daily_trips=2**60 + 1), row(second, avg_daily_trips=None)],
         route='/push').raise_for_status()

    assert pull(feast_url, [first])[0]['avg_daily_trips'] == [2**60]


def test_stock_push_coerces_values_to_the_schema(feast_url):
    truncated, as_text = new_drivers(2)

    push(feast_url, [row(truncated, avg_daily_trips=1.9)], route='/push').raise_for_status()
    push(feast_url, [row(as_text, city=123)], route='/push').raise_for_status()

    assert pull(feast_url, [truncated])[0]['avg_daily_trips'] == [1]
    assert pull(feast_url, [as_text])[0]['city'] == ['123']


def test_stock_push_reads_integer_timestamps_as_nanoseconds(feast_url):
    driver, = new_drivers()
    epoch_seconds = 1735689600  # 2025-01-01T00:00:00Z

    push(feast_url, [row(driver, when=epoch_seconds)], route='/push').raise_for_status()

    assert pull(feast_url, [driver])[1]['conv_rate'] == ['1970-01-01T00:00:01.735689Z']


def test_stock_push_keeps_the_last_row_not_the_latest_event(feast_url):
    driver, = new_drivers()
    rows = [
        row(driver, '2025-01-03T00:00:00+00:00', conv_rate=0.75),
        row(driver, '2025-01-01T00:00:00+00:00', conv_rate=0.25),
    ]

    push(feast_url, rows, route='/push').raise_for_status()

    assert pull(feast_url, [driver])[0]['conv_rate'] == [0.25]


def test_float32_features_come_back_widened(feast_url):
    """conv_rate is Float32: 0.1 is stored as the nearest float32."""
    driver, = new_drivers()

    push(feast_url, [row(driver, conv_rate=0.1, acc_rate=0.1)]).raise_for_status()

    values = pull(feast_url, [driver])[0]
    assert values['conv_rate'] == [0.10000000149011612]
    assert values['acc_rate'] == [0.1]


# /push/typed ----------------------------------------------------------------------------

@pytest.mark.parametrize('value', [
    2**60 + 1, 2**63 - 1, -(2**63), 0, -1, 9007199254740993,
])
def test_large_integers_next_to_nulls_stay_exact(feast_url, value):
    present, missing = new_drivers(2)

    response = push(feast_url, [row(present, avg_daily_trips=value),
                                row(missing, avg_daily_trips=None)])

    assert response.status_code == 200, response.text
    assert response.json()['batches'] == 2
    assert pull(feast_url, [present, missing])[0]['avg_daily_trips'] == [value, None]


def test_null_features_next_to_present_ones(feast_url):
    driver, = new_drivers()

    push(feast_url, [row(driver, conv_rate=None, acc_rate=None, city=None)]).raise_for_status()

    values, stamps = pull(feast_url, [driver])
    assert values['conv_rate'] == [None] and values['avg_daily_trips'] == [1]
    assert stamps['conv_rate'] == ['2025-01-01T00:00:00Z']


def test_an_entity_with_every_feature_null_reads_as_never_written(feast_url):
    """Feast reports NOT_FOUND and the epoch, as for an entity that was never pushed."""
    driver, = new_drivers()

    push(feast_url, [row(driver, conv_rate=None, acc_rate=None, avg_daily_trips=None,
                         city=None, active=None)]).raise_for_status()

    response = requests.post(
        f'{feast_url}/get-online-features',
        json=dict(features=['driver_hourly_stats:conv_rate'], entities=dict(driver_id=[driver])),
        timeout=10,
    ).json()
    assert response['results'][1]['statuses'] == ['NOT_FOUND']
    assert response['results'][1]['event_timestamps'] == ['1970-01-01T00:00:00Z']


@pytest.mark.parametrize('city', ['', ' ', 'São Paulo 東京 🐍', 'a,b;"c"\n\td', 'x' * 10_000])
def test_text_values(feast_url, city):
    driver, = new_drivers()

    push(feast_url, [row(driver, city=city)]).raise_for_status()

    assert pull(feast_url, [driver])[0]['city'] == [city]


@pytest.mark.parametrize('acc_rate', [0.1, 1e-300, 1.7976931348623157e308, -0.0, 1 / 3])
def test_float64_values(feast_url, acc_rate):
    driver, = new_drivers()

    push(feast_url, [row(driver, acc_rate=acc_rate)]).raise_for_status()

    assert pull(feast_url, [driver])[0]['acc_rate'] == [acc_rate]


@pytest.mark.parametrize('sent, stored', [
    ('2025-01-01T05:30:00+05:30', '2025-01-01T00:00:00Z'),
    ('2025-01-01T00:00:00.123456Z', '2025-01-01T00:00:00.123456Z'),
    ('2024-12-31T19:00:00-05:00', '2025-01-01T00:00:00Z'),
])
def test_timestamps_with_offsets_are_stored_in_utc(feast_url, sent, stored):
    driver, = new_drivers()

    push(feast_url, [row(driver, when=sent)]).raise_for_status()

    assert pull(feast_url, [driver])[1]['conv_rate'] == [stored]


def test_naive_timestamps_need_assume_utc(feast_url):
    driver, = new_drivers()
    naive = [row(driver, when='2025-01-01T00:00:00')]

    rejected = push(feast_url, naive)
    accepted = push(feast_url, naive, assume_utc=True)

    assert rejected.status_code == 422
    assert accepted.status_code == 200
    assert pull(feast_url, [driver])[1]['conv_rate'] == ['2025-01-01T00:00:00Z']


def test_the_latest_event_of_an_entity_wins_within_a_push(feast_url):
    driver, = new_drivers()
    rows = [
        row(driver, '2025-01-02T00:00:00+00:00', conv_rate=0.25),
        row(driver, '2025-01-03T00:00:00+00:00', conv_rate=0.75),
        row(driver, '2025-01-01T00:00:00+00:00', conv_rate=0.5),
    ]

    push(feast_url, rows).raise_for_status()

    values, stamps = pull(feast_url, [driver])
    assert values['conv_rate'] == [0.75]
    assert stamps['conv_rate'] == ['2025-01-03T00:00:00Z']


def test_only_newer_keeps_a_newer_stored_value(feast_url):
    driver, = new_drivers()
    push(feast_url, [row(driver, '2025-06-01T00:00:00+00:00', conv_rate=0.75)]).raise_for_status()

    late = push(feast_url, [row(driver, '2025-01-01T00:00:00+00:00', conv_rate=0.25)],
                only_newer=True)
    newer = push(feast_url, [row(driver, '2025-07-01T00:00:00+00:00', conv_rate=0.125)],
                 only_newer=True)

    assert late.json()['skipped_older'] == 1 and newer.json()['pushed'] == 1
    assert pull(feast_url, [driver])[0]['conv_rate'] == [0.125]


def test_without_only_newer_a_late_event_replaces_the_stored_one(feast_url):
    driver, = new_drivers()
    push(feast_url, [row(driver, '2025-06-01T00:00:00+00:00', conv_rate=0.75)]).raise_for_status()

    push(feast_url, [row(driver, '2025-01-01T00:00:00+00:00', conv_rate=0.25)]).raise_for_status()

    assert pull(feast_url, [driver])[0]['conv_rate'] == [0.25]


@pytest.mark.parametrize('column, value, message', [
    ('avg_daily_trips', 1.5, 'not an integer'),
    ('avg_daily_trips', '7', 'not an integer'),
    ('avg_daily_trips', True, 'not an integer'),
    ('avg_daily_trips', 2**63, 'out of range'),
    ('conv_rate', '0.25', 'not a number'),
    ('acc_rate', False, 'not a number'),
    ('city', 123, 'not a string'),
    ('active', 0, 'not a boolean'),
    ('active', 'true', 'not a boolean'),
    ('driver_id', '1001', 'not an integer'),
    ('event_timestamp', 1735689600, 'nanoseconds'),
    ('event_timestamp', '01/02/2025', 'not an ISO 8601'),
])
def test_invalid_values_are_rejected_and_nothing_is_written(feast_url, column, value, message):
    driver, other = new_drivers(2)

    response = push(feast_url, [row(other), row(driver, **{column: value})])

    assert response.status_code == 422
    errors = response.json()['detail']['errors']
    assert [(e['row'], e['column']) for e in errors] == [(1, column)]
    assert message in errors[0]['message']
    assert pull(feast_url, [other])[0]['conv_rate'] == [None]


def test_missing_and_unknown_columns(feast_url):
    driver, = new_drivers()
    data = row(driver)
    del data['city']
    data['unexpected'] = 1

    response = push(feast_url, [data])

    errors = {(e['column'], e['message']) for e in response.json()['detail']['errors']}
    assert errors == {('city', 'missing'), ('unexpected', 'not in the feature view')}


@pytest.mark.parametrize('to', ['offline', 'online_and_offline'])
def test_offline_pushes_are_refused_before_any_write(feast_url, to):
    driver, = new_drivers()

    response = push(feast_url, [row(driver)], to=to)

    assert response.status_code == 501
    assert 'nothing was written' in response.json()['detail']
    assert pull(feast_url, [driver])[0]['conv_rate'] == [None]


def test_unknown_push_source(feast_url):
    response = requests.post(
        f'{feast_url}/push/typed', json=dict(push_source_name='missing', df={}), timeout=10,
    )

    assert response.status_code == 404


def test_a_retried_push_leaves_the_same_values(feast_url):
    drivers = new_drivers(3)
    rows = [row(d, avg_daily_trips=2**55 + d % 7) for d in drivers]

    for _ in range(3):
        push(feast_url, rows).raise_for_status()

    assert pull(feast_url, drivers)[0]['avg_daily_trips'] == [2**55 + d % 7 for d in drivers]


def random_rows(drivers, seed):
    generator = random.Random(seed)
    rows = []
    for driver in drivers:
        null = generator.random() < 0.2
        rows.append(row(
            driver,
            when=(dt.datetime(2025, 1, 1, tzinfo=UTC)
                  + dt.timedelta(seconds=generator.randint(0, 10**7),
                                 microseconds=generator.randint(0, 999_999))).isoformat(),
            conv_rate=None if null else generator.randint(0, 64) / 4,
            acc_rate=None if null else generator.random(),
            avg_daily_trips=None if null else generator.randint(-(2**63), 2**63 - 1),
            city=None if null else ''.join(generator.choice('abñ東 ,"') for _ in range(8)),
            active=None if null else generator.random() < 0.5,
        ))
    return rows


def assert_stored(feast_url, rows):
    drivers = [r['driver_id'] for r in rows]
    values, stamps = pull(feast_url, drivers)
    for feature in FEATURES:
        assert values[feature] == [r[feature] for r in rows], feature
    epoch = dt.datetime(1970, 1, 1, tzinfo=UTC)
    expected_stamps = [
        # An entity whose features are all NULL reads as never written.
        epoch if all(r[f] is None for f in FEATURES)
        else dt.datetime.fromisoformat(r['event_timestamp'])
        for r in rows
    ]
    # Feast writes fractional seconds with 3, 6 or 9 digits, as protobuf JSON does.
    assert [dt.datetime.fromisoformat(s) for s in stamps['conv_rate']] == expected_stamps


def test_a_large_batch_round_trips_exactly(feast_url):
    rows = random_rows(new_drivers(3000), seed=7)

    response = push(feast_url, rows)

    assert response.status_code == 200, response.text
    assert response.json()['pushed'] == 3000
    assert_stored(feast_url, rows)


def test_concurrent_pushes(feast_url):
    batches = [random_rows(new_drivers(200), seed=s) for s in range(8)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(lambda rows: push(feast_url, rows), batches))

    assert [r.status_code for r in responses] == [200] * 8
    for rows in batches:
        assert_stored(feast_url, rows)


def test_an_empty_push(feast_url):
    response = requests.post(
        f'{feast_url}/push/typed',
        json=dict(push_source_name='driver_stats_push',
                  df={c: [] for c in row(1)}),
        timeout=10,
    )

    assert response.json() == dict(rows=0, pushed=0, skipped_older=0, batches=0)


# Pulls ----------------------------------------------------------------------------------

def test_pulls_keep_the_order_and_duplicates_of_the_request(feast_url):
    drivers = new_drivers(3)
    push(feast_url, [row(d, avg_daily_trips=i) for i, d in enumerate(drivers)]).raise_for_status()
    request = [drivers[2], 1, drivers[0], drivers[2], drivers[1]]

    values = pull(feast_url, request, features=['avg_daily_trips'])[0]

    assert values['driver_id'] == request
    assert values['avg_daily_trips'] == [2, None, 0, 2, 1]


def test_string_and_integer_entity_keys_read_the_same_row(feast_url):
    driver, = new_drivers()
    push(feast_url, [row(driver, conv_rate=0.75)]).raise_for_status()

    as_string = requests.post(
        f'{feast_url}/get-online-features',
        json=dict(features=['driver_hourly_stats:conv_rate'],
                  entities=dict(driver_id=[str(driver)])),
        timeout=10,
    ).json()

    assert as_string['results'][1]['values'] == [0.75]


def test_pulled_features_into_pandas_and_polars_frames(feast_url):
    """The response as frames that keep integers above 2**53 next to NULL values."""
    rows = random_rows(new_drivers(500), seed=11)
    push(feast_url, rows).raise_for_status()
    values, _ = pull(feast_url, [r['driver_id'] for r in rows])

    polars_frame = pl.DataFrame(values)
    # Arrow infers int64 with NULL values; pandas would infer float64 first.
    pandas_frame = pa.table(values).to_pandas(types_mapper=pd.ArrowDtype)

    expected = [r['avg_daily_trips'] for r in rows]
    assert polars_frame['avg_daily_trips'].to_list() == expected
    assert [None if pd.isna(v) else v for v in pandas_frame['avg_daily_trips']] == expected
    assert polars_frame['city'].to_list() == [r['city'] for r in rows]

    # pd.DataFrame makes the column float64 before convert_dtypes runs, so the integers
    # above 2**53 are already rounded.
    lossy = pd.DataFrame(values).convert_dtypes(dtype_backend='pyarrow')['avg_daily_trips']
    assert [None if pd.isna(v) else v for v in lossy] != expected


def test_pulling_many_entities_in_one_request(feast_url):
    rows = random_rows(new_drivers(5000), seed=3)
    push(feast_url, rows).raise_for_status()

    values, _ = pull(feast_url, [r['driver_id'] for r in rows], features=['avg_daily_trips'])

    assert values['avg_daily_trips'] == [r['avg_daily_trips'] for r in rows]


def test_concurrent_reads_and_writes_all_succeed(feast_url):
    """
    With conn_type: singleton, Feast's default, the PostgreSQL online store shares one
    connection between request threads, and most of these requests failed with 500.
    The service uses conn_type: pool.
    """
    batches = [random_rows(new_drivers(50), seed=100 + i) for i in range(40)]

    def read(_):
        return requests.post(
            f'{feast_url}/get-online-features',
            json=dict(features=['driver_hourly_stats:conv_rate'],
                      entities=dict(driver_id=[1001, 1002])),
            timeout=60,
        ).status_code

    def write(rows):
        return push(feast_url, rows).status_code

    def write_stock(rows):
        return push(feast_url, [rows[0]], route='/push').status_code

    jobs = [(read, None)] * 120 + [(write, b) for b in batches] + [
        (write_stock, random_rows(new_drivers(1), seed=500 + i)) for i in range(40)
    ]
    random.Random(1).shuffle(jobs)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        statuses = list(pool.map(lambda job: job[0](job[1]), jobs))

    assert statuses.count(200) == len(jobs), sorted(set(statuses))
    for rows in batches:
        assert_stored(feast_url, rows)
