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
