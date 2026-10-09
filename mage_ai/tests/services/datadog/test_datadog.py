from unittest.mock import patch

from mage_ai.services import datadog as dd
from mage_ai.tests.base_test import TestCase

# 2023-01-01T00:00:00Z
EPOCH = 1672531200.0
TEST_METRIC = 'mage.test'
TAGS = dict(tag1='tag')


class DatadogTests(TestCase):
    @patch('datadog.api.Event.create')
    def test_create_event(self, mock_event):
        event_name = 'event name'
        event_text = 'event text'
        dd.create_event(event_name, event_text, tags=TAGS)
        mock_event.assert_called_with(title=event_name, text=event_text, tags=TAGS)

    @patch('datadog.api.Metric.send')
    @patch('mage_ai.services.datadog.time.time', return_value=EPOCH)
    def test_gauge(self, mock_time, mock_metric):
        dd.gauge(TEST_METRIC, 5, tags=TAGS)
        mock_metric.assert_called_with(
            host='mage',
            metric=TEST_METRIC,
            points=[(EPOCH, 5)],
            tags=TAGS,
            type='gauge'
        )

    @patch('datadog.api.Metric.send')
    def test_increment(self, mock_metric):
        dd.increment(TEST_METRIC, tags=TAGS)
        mock_metric.assert_called_with(metrics=[{
            'host': 'mage',
            'metric': TEST_METRIC,
            'points': 1,
            'tags': TAGS,
            'type': 'count'
        }])

    @patch('datadog.api.Metric.send')
    @patch('mage_ai.services.datadog.time.time', return_value=EPOCH)
    def test_histogram(self, mock_time, mock_metric):
        dd.histogram(TEST_METRIC, 3, tags=TAGS)
        mock_metric.assert_called_with(
            host='mage',
            metric=TEST_METRIC,
            points=[(EPOCH, 3)],
            tags=TAGS,
            type='histogram'
        )

    @patch('datadog.api.Metric.send')
    @patch('mage_ai.services.datadog.time.time', return_value=EPOCH)
    def test_timing(self, mock_time, mock_metric):
        dd.timing(TEST_METRIC, 100, tags=TAGS)
        mock_metric.assert_called_with(
            host='mage',
            metric=TEST_METRIC,
            points=[(EPOCH, 100)],
            tags=TAGS,
            type='timer'
        )
