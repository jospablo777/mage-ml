import datetime
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from bson import datetime as bson_datetime

from mage_integrations.sources.mongodb.tap_mongodb.sync_strategies import common

# UTC-6 all year, so the expected values do not depend on daylight saving time.
LOCAL_ZONE = ZoneInfo('America/Costa_Rica')


class AsUtcTest(unittest.TestCase):
    """
    pymongo returns BSON dates as naive datetimes in UTC. They were read as local time, so
    on a host at UTC-6 a date stored as 12:30 UTC was emitted as 18:30Z, and datetime
    bookmarks moved by the same offset.
    """

    def test_naive_datetimes_are_utc(self):
        value = datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(common.as_utc(value), value.replace(tzinfo=datetime.timezone.utc))

    def test_aware_datetimes_are_converted(self):
        value = datetime.datetime(2024, 5, 1, 6, 30, tzinfo=LOCAL_ZONE)

        self.assertEqual(
            common.as_utc(value),
            datetime.datetime(2024, 5, 1, 12, 30, tzinfo=datetime.timezone.utc),
        )

    def test_the_host_time_zone_does_not_matter(self):
        value = datetime.datetime(2024, 5, 1, 12, 30)
        with patch.dict('os.environ', {'TZ': 'America/Costa_Rica'}):
            import time
            time.tzset()
            try:
                result = common.transform_value(value, ['created_at'])
            finally:
                time.tzset()

        self.assertEqual(result, '2024-05-01T12:30:00.000000Z')

    def test_class_to_string_keeps_datetime_bookmarks_in_utc(self):
        value = datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(
            common.class_to_string(value, 'datetime'),
            '2024-05-01T12:30:00.000000Z',
        )

    def test_transform_value_keeps_bson_datetimes_in_utc(self):
        value = bson_datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(
            common.transform_value({'created_at': value}, []),
            {'created_at': '2024-05-01T12:30:00.000000Z'},
        )

    def test_bookmark_round_trip(self):
        """A bookmark written from a document's date reads back as the same instant."""
        value = datetime.datetime(2024, 5, 1, 12, 30, 0, 123000)

        written = common.class_to_string(value, 'datetime')
        read = common.string_to_class(written, 'datetime')

        self.assertEqual(read, value.replace(tzinfo=datetime.timezone.utc))


if __name__ == '__main__':
    unittest.main()
