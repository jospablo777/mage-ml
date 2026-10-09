"""
Primary key lookups on the base model.

Query.get became legacy in SQLAlchemy 2.0, so lookups go through Session.get.
BlockRun.get takes a pipeline run and a block uuid, which is why the lookup by
primary key needs a name of its own.
"""
import uuid
from random import randrange

from mage_ai.orchestration.db.models.oauth import Role
from mage_ai.orchestration.db.models.schedules import BlockRun
from mage_ai.tests.base_test import DBTestCase


class GetByIdTests(DBTestCase):
    def test_returns_the_row(self):
        role = Role.create(name=f'role_{uuid.uuid4().hex}')

        self.assertEqual(Role.get_by_id(role.id), role)

    def test_returns_none_for_a_missing_row(self):
        self.assertIsNone(Role.get_by_id(2 ** 31 - 1))

    def test_returns_none_for_a_null_key(self):
        """SQLAlchemy warns that a NULL key may raise in a future release."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter('error')
            self.assertIsNone(Role.get_by_id(None))

    def test_get_delegates_to_get_by_id(self):
        role = Role.create(name=f'role_{uuid.uuid4().hex}')

        self.assertEqual(Role.get(role.id), role)

    def test_block_run_keeps_its_own_get(self):
        pipeline_run_id = randrange(2 ** 16, 2 ** 31)
        block_run = BlockRun.create(block_uuid='block_1', pipeline_run_id=pipeline_run_id)

        self.assertEqual(
            BlockRun.get(pipeline_run_id=pipeline_run_id, block_uuid='block_1'),
            block_run,
        )
        self.assertEqual(BlockRun.get_by_id(block_run.id), block_run)


class UpdateTests(DBTestCase):
    def test_a_dict_changed_in_place_is_saved(self):
        from mage_ai.orchestration.db import db_connection

        block_run = BlockRun.create(pipeline_run_id=randrange(10 ** 6), block_uuid='b',
                                    metrics=dict(a=1))
        metrics = block_run.metrics
        metrics['b'] = 2

        block_run.update(metrics=metrics)

        db_connection.session.expire_all()
        self.assertEqual(BlockRun.get_by_id(block_run.id).metrics, dict(a=1, b=2))


class ResetForRetryTests(DBTestCase):
    def test_a_retried_block_run_starts_clean(self):
        from datetime import datetime, timezone

        from mage_ai.orchestration.db import db_connection

        block_run = BlockRun.create(
            pipeline_run_id=randrange(10 ** 6),
            block_uuid='b',
            status=BlockRun.BlockRunStatus.FAILED,
            started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            completed_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            metrics=dict(crashes=2, error=dict(error='boom'), controller=True),
        )

        BlockRun.reset_for_retry([block_run.id])

        db_connection.session.expire_all()
        block_run = BlockRun.get_by_id(block_run.id)
        self.assertEqual(block_run.status, BlockRun.BlockRunStatus.INITIAL)
        self.assertIsNone(block_run.started_at)
        self.assertIsNone(block_run.completed_at)
        # The error and crash count of the earlier attempt are gone; the rest stays.
        self.assertEqual(block_run.metrics, dict(controller=True))
