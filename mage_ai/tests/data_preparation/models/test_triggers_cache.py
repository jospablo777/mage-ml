import os

from mage_ai.data_preparation.models import triggers as triggers_module
from mage_ai.data_preparation.models.triggers import (
    ScheduleType,
    Trigger,
    add_or_update_trigger_for_pipeline_and_persist,
    get_triggers_by_pipeline_with_cache,
    get_triggers_file_path,
)
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks


class TriggersCacheTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        self.path = get_triggers_file_path(self.pipeline.uuid)

    def add(self, name):
        add_or_update_trigger_for_pipeline_and_persist(
            Trigger(
                name=name, pipeline_uuid=self.pipeline.uuid, schedule_type=ScheduleType.API,
                schedule_interval=None, start_time=None,
            ),
            self.pipeline.uuid,
        )

    def names(self):
        found, from_cache = get_triggers_by_pipeline_with_cache(self.pipeline.uuid)
        return sorted(t.name for t in found), from_cache

    def test_a_write_in_the_same_timestamp_tick_is_not_hidden_by_the_cache(self):
        self.add('first')
        self.assertEqual(self.names(), (['first'], False))
        mtime = os.stat(self.path).st_mtime_ns
        self.add('second')
        # As on file systems whose timestamps did not advance between the two writes.
        os.utime(self.path, ns=(mtime, mtime))
        self.assertEqual(self.names()[0], ['first', 'second'])

    def test_an_unchanged_settled_file_comes_from_the_cache(self):
        self.add('first')
        old = os.stat(self.path).st_mtime_ns - 10 * triggers_module.CACHE_SETTLE_NS
        os.utime(self.path, ns=(old, old))
        self.assertEqual(self.names(), (['first'], False))
        self.assertEqual(self.names(), (['first'], True))
        self.add('second')
        self.assertEqual(self.names(), (['first', 'second'], False))
