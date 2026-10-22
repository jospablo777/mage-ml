from datetime import datetime
from unittest.mock import patch

import pytz

from mage_ai.orchestration.backfills.service import BackfillError, preview_run_dates
from mage_ai.orchestration.db.models.schedules import Backfill
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_backfill, create_pipeline, create_user


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=pytz.UTC)


class BackfillDatesTest(BaseApiTestCase):
    model_class = Backfill

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        self.pipeline = create_pipeline('backfill pipeline', self.repo_path)

    def dates(self, interval_type, units, start, end):
        backfill = create_backfill(
            self.pipeline.uuid, start_datetime=start, end_datetime=end,
            interval_type=interval_type, interval_units=units,
        )
        return [d['execution_date'][:10] for d in preview_run_dates(backfill)]

    def test_months_count_from_the_start_date(self):
        """A month added to the date before drifted: January 31, February 28, March 28."""
        self.assertEqual(
            self.dates(Backfill.IntervalType.MONTH, 1, utc(2026, 1, 31), utc(2026, 4, 30)),
            ['2026-01-31', '2026-02-28', '2026-03-31', '2026-04-30'],
        )

    def test_years_count_from_the_start_date(self):
        self.assertEqual(
            self.dates(Backfill.IntervalType.YEAR, 1, utc(2024, 2, 29), utc(2028, 3, 1)),
            ['2024-02-29', '2025-02-28', '2026-02-28', '2027-02-28', '2028-02-29'],
        )

    def test_days_and_their_intervals(self):
        backfill = create_backfill(
            self.pipeline.uuid, start_datetime=utc(2026, 1, 1), end_datetime=utc(2026, 1, 5),
            interval_type=Backfill.IntervalType.DAY, interval_units=2,
        )
        runs = preview_run_dates(backfill)

        self.assertEqual([r['ds'] for r in runs], ['2026-01-01', '2026-01-03', '2026-01-05'])
        self.assertEqual(runs[0]['interval_seconds'], 2 * 86400)
        self.assertEqual(runs[2]['interval_end_datetime'][:10], '2026-01-07')

    def test_units_that_never_reach_the_end_are_refused(self):
        """Zero or a negative number of units never reached the end date: building the
        dates never finished, and the backfill's page hung the server."""
        for units in (0, -1):
            with self.assertRaisesRegex(BackfillError, 'positive whole number'):
                self.dates(Backfill.IntervalType.DAY, units, utc(2026, 1, 1), utc(2026, 1, 2))

    def test_a_backfill_past_the_run_limit_is_refused(self):
        with patch.dict('os.environ', {'MAGE_BACKFILL_MAX_RUNS': '24'}):
            with self.assertRaisesRegex(BackfillError, 'more than 24 runs'):
                self.dates(Backfill.IntervalType.HOUR, 1, utc(2026, 1, 1), utc(2026, 1, 2))
            self.assertEqual(
                len(self.dates(
                    Backfill.IntervalType.HOUR, 1, utc(2026, 1, 1), utc(2026, 1, 1, 23),
                )),
                24,
            )


class BackfillOperationTests(BaseApiTestCase):
    model_class = Backfill

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        self.pipeline = create_pipeline('backfill api pipeline', self.repo_path)

    async def update(self, **payload):
        backfill = create_backfill(self.pipeline.uuid)
        user = create_user(email=self.faker.email(), roles=1)
        return await self.build_update_operation(
            backfill.id, dict(backfill=payload), user=user,
        ).execute()

    async def test_interval_units_must_be_a_positive_whole_number(self):
        for units in (0, -3, 1.5, 'two', True):
            response = await self.update(interval_type='day', interval_units=units)
            self.assertEqual(response['error']['code'], 400, units)
            self.assertIn('positive whole number', response['error']['message'])

        response = await self.update(interval_type='day', interval_units='2')
        self.assertEqual(response['backfill']['interval_units'], 2)

        response = await self.update(name='renamed')
        self.assertEqual(response['backfill']['interval_units'], 1)

    async def test_a_saved_backfill_with_bad_units_answers_an_error_not_a_hang(self):
        backfill = create_backfill(self.pipeline.uuid, interval_units=0)
        user = create_user(email=self.faker.email(), roles=1)

        response = await self.build_detail_operation(
            backfill.id, user=user, query=dict(include_run_count=['1']),
        ).execute()

        self.assertEqual(response['error']['code'], 400)
        self.assertIn('positive whole number', response['error']['message'])
