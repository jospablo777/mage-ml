import datetime
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from bson import datetime as bson_datetime

from mage_integrations.sources.mongodb.tap_mongodb.sync_strategies import common

# UTC-6 all year, so the expected values do not depend on daylight saving time.
LOCAL_ZONE = ZoneInfo('America/Costa_Rica')


@patch.object(common.tzlocal, 'get_localzone', return_value=LOCAL_ZONE)
class LocalizeTest(unittest.TestCase):
    def test_localize_naive_attaches_the_local_zone(self, _):
        value = datetime.datetime(2024, 5, 1, 12, 30)

        localized = common.localize_naive(value)

        self.assertEqual(localized.tzinfo, LOCAL_ZONE)
        self.assertEqual(localized.replace(tzinfo=None), value)

    def test_localize_naive_rejects_aware_datetimes(self, _):
        value = datetime.datetime(2024, 5, 1, 12, 30, tzinfo=datetime.timezone.utc)

        with self.assertRaises(ValueError):
            common.localize_naive(value)

    def test_class_to_string_converts_datetime_bookmarks_to_utc(self, _):
        value = datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(
            common.class_to_string(value, 'datetime'),
            '2024-05-01T18:30:00.000000Z',
        )

    def test_transform_value_converts_python_datetimes_to_utc(self, _):
        value = datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(
            common.transform_value(value, ['created_at']),
            '2024-05-01T18:30:00.000000Z',
        )

    def test_transform_value_converts_bson_datetimes_to_utc(self, _):
        value = bson_datetime.datetime(2024, 5, 1, 12, 30)

        self.assertEqual(
            common.transform_value({'created_at': value}, []),
            {'created_at': '2024-05-01T18:30:00.000000Z'},
        )

    def test_safe_transform_datetime_rejects_aware_datetimes(self, _):
        value = datetime.datetime(2024, 5, 1, 12, 30, tzinfo=datetime.timezone.utc)

        with self.assertRaises(common.MongoInvalidDateTimeException):
            common.safe_transform_datetime(value, ['created_at'])


if __name__ == '__main__':
    unittest.main()
