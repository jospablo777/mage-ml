"""
The driver statistics seeded into the Feast offline store.

Standard library only: the tests import this module to compute the values they expect.
"""
import datetime as dt

DRIVERS = [1001, 1002, 1003, 1004, 1005]
START = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
HOURS = 48
CITIES = ['San José', 'Zürich', 'São Paulo', '東京', 'Reykjavík']
COLUMNS = ['driver_id', 'event_timestamp', 'created', 'conv_rate', 'acc_rate',
           'avg_daily_trips', 'city', 'active']


def rows():
    """One row per driver and hour. Values are exact in float32 and float64."""
    for hour in range(HOURS):
        timestamp = START + dt.timedelta(hours=hour)
        for index, driver in enumerate(DRIVERS):
            yield dict(
                driver_id=driver,
                event_timestamp=timestamp,
                created=timestamp + dt.timedelta(minutes=5),
                conv_rate=(index + hour % 4) * 0.25,
                acc_rate=driver / 1000 + hour / 1024,
                avg_daily_trips=driver * 10**13 + hour,  # above 2**53
                city=CITIES[index],
                active=None if hour % 5 == 0 else hour % 2 == 0,
            )


def value_at(driver: int, when: dt.datetime) -> dict:
    """The latest row of a driver at or before a time: Feast's point-in-time join."""
    latest = None
    for row in rows():
        if row['driver_id'] == driver and row['event_timestamp'] <= when:
            latest = row
    return latest
