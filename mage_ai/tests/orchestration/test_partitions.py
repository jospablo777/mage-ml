"""
Partition declarations and names (orchestration/partitions.py, ADR-0039): what a pipeline
declares, which partitions exist by a given time, and the variables of a partition's run.
"""
from datetime import datetime, timedelta, timezone

from mage_ai.orchestration import partitions
from mage_ai.orchestration.partitions import PartitionError
from mage_ai.tests.base_test import TestCase

UTC = timezone.utc


def daily(**settings):
    return partitions.parse(dict(key='day', daily=dict(start='2026-11-01', **settings)))


class ParseTest(TestCase):
    def test_a_time_declaration(self):
        declaration = partitions.parse(dict(
            key='day', upstream='orders_raw',
            daily=dict(start='2026-11-01', timezone='America/New_York', close_after='2h'),
        ))
        self.assertEqual(declaration.kind, 'daily')
        self.assertEqual(declaration.close_after, timedelta(hours=2))
        self.assertEqual(declaration.upstream, ['orders_raw'])
        self.assertEqual(declaration.start.isoformat(), '2026-11-01T00:00:00-04:00')

    def test_none_without_a_declaration(self):
        self.assertIsNone(partitions.parse(None))

    def test_each_mistake_says_what_to_write(self):
        cases = [
            (dict(daily=dict(start='2026-11-01')), 'key is the name of the variable'),
            (dict(key='day'), 'give one of hourly, daily, weekly, monthly, values.'),
            (dict(key='day', daily=dict(start='2026-11-01'), values=['a']),
             'daily and values were given'),
            (dict(key='day', daily={}), 'daily.start is the first partition'),
            (dict(key='day', daily=dict(start='first of november')), 'daily.start is a date'),
            (dict(key='day', daily=dict(start='2026-11-01', timezone='Mars/Base')),
             'daily.timezone is a time zone'),
            (dict(key='day', daily=dict(start='2026-11-01', close_after='soon')),
             'close_after is a duration'),
            (dict(key='day', daily=dict(start='2026-11-01', end='2026-10-01')),
             'daily.end is after its start'),
            (dict(key='day', daily=dict(start='2026-11-01T13:00')),
             'daily.start is where a partition begins, such as 2026-11-01'),
            (dict(key='day', weekly=dict(start='2026-11-04')),
             'weekly.start is where a partition begins, such as 2026-11-02'),
            (dict(key='region', values=['eu', 'eu']), 'each value is listed once'),
            (dict(key='region', values=['eu/west']), 'a value is letters'),
            (dict(key='day', daily=dict(start='2026-11-01'), every='day'),
             'unknown settings every'),
        ]
        for document, message in cases:
            with self.subTest(document=document):
                with self.assertRaisesRegex(PartitionError, message.replace('(', r'\(')):
                    partitions.parse(document)


class ValuesTest(TestCase):
    def test_closed_partitions_from_the_start(self):
        declaration = daily(close_after='2h')
        now = datetime(2026, 11, 4, 1, 30, tzinfo=UTC)
        # Nov 3 ended at midnight; it closes at 02:00 for late data.
        self.assertEqual(partitions.expected(declaration, now), ['2026-11-01', '2026-11-02'])
        self.assertEqual(partitions.expected(declaration, now + timedelta(hours=1)),
                         ['2026-11-01', '2026-11-02', '2026-11-03'])

    def test_days_follow_the_time_zone_through_daylight_saving(self):
        declaration = daily(timezone='America/New_York')
        # Clocks fall back on 2026-11-01 there: that day has 25 hours (measured in UTC;
        # aware times in one zone subtract as wall-clock times).
        start = partitions.begin(declaration, '2026-11-01').astimezone(UTC)
        end = partitions.end_of(declaration, '2026-11-01').astimezone(UTC)
        self.assertEqual(end - start, timedelta(hours=25))
        self.assertEqual(partitions.value_of(
            declaration, datetime(2026, 11, 2, 3, 30, tzinfo=UTC)), '2026-11-01')
        variables = partitions.run_variables(declaration, '2026-11-02')
        self.assertEqual(variables['execution_date'],
                         datetime(2026, 11, 2, 5, 0, tzinfo=UTC))
        self.assertEqual(variables['day'], '2026-11-02')
        self.assertEqual(variables['execution_partition'], 'partitions/2026-11-02')

    def test_hours_are_utc_so_none_repeats_or_is_missing(self):
        declaration = partitions.parse(dict(key='hour', hourly=dict(
            start='2026-11-01T00:00', timezone='America/New_York')))
        values = partitions.expected(declaration, datetime(2026, 11, 2, 0, 0, tzinfo=UTC))
        self.assertEqual(len(values), 24)
        self.assertEqual(len(set(values)), 24)
        self.assertEqual(values[:2], ['2026-11-01T00', '2026-11-01T01'])

    def test_weeks_begin_on_monday_and_months_on_the_first(self):
        weekly = partitions.parse(dict(key='week', weekly=dict(start='2026-11-02')))
        self.assertEqual(partitions.expected(weekly, datetime(2026, 11, 17, tzinfo=UTC)),
                         ['2026-11-02', '2026-11-09'])
        monthly = partitions.parse(dict(key='month', monthly=dict(start='2026-11-01')))
        self.assertEqual(partitions.expected(monthly, datetime(2027, 1, 1, tzinfo=UTC)),
                         ['2026-11', '2026-12'])
        self.assertEqual(partitions.end_of(monthly, '2026-12'),
                         datetime(2027, 1, 1, tzinfo=partitions.parse(
                             dict(key='m', monthly=dict(start='2026-11-01'))).zone))

    def test_an_end_stops_the_partitions(self):
        declaration = daily(end='2026-11-03')
        self.assertEqual(partitions.expected(declaration, datetime(2027, 1, 1, tzinfo=UTC)),
                         ['2026-11-01', '2026-11-02'])

    def test_listed_values(self):
        declaration = partitions.parse(dict(key='region', values=['eu', 'us']))
        self.assertEqual(partitions.expected(declaration), ['eu', 'us'])
        self.assertEqual(partitions.run_variables(declaration, 'eu'),
                         dict(region='eu', execution_partition='partitions/eu'))
        with self.assertRaisesRegex(PartitionError, "'apac' is not one of the values"):
            partitions.run_variables(declaration, 'apac')

    def test_a_name_that_is_not_a_partition_is_refused(self):
        with self.assertRaisesRegex(PartitionError, "'2026-13-01' is not a daily partition"):
            partitions.begin(daily(), '2026-13-01')
        weekly = partitions.parse(dict(key='week', weekly=dict(start='2026-11-02')))
        with self.assertRaisesRegex(PartitionError, 'is not a weekly partition'):
            partitions.begin(weekly, '2026-11-04')
