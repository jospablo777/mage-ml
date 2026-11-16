from mage_ai.orchestration.trigger_rules import (
    Decision,
    TriggerRule,
    continues_after_failure,
    decide,
    validation_error,
)
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import create_pipeline_with_blocks

RUN = Decision.RUN
WAIT = Decision.WAIT
UPSTREAM_FAILED = Decision.UPSTREAM_FAILED
SKIPPED = Decision.CONDITION_FAILED


class DecideTest(DBTestCase):
    """Each rule's decision for the statuses of two upstream blocks."""

    CASES = [
        # upstream statuses, all_success, none_failed_min_one_success, all_done
        (('completed', 'completed'), RUN, RUN, RUN),
        (('completed', None), WAIT, WAIT, WAIT),
        (('completed', 'running'), WAIT, WAIT, WAIT),
        (('completed', 'condition_failed'), SKIPPED, RUN, RUN),
        (('condition_failed', 'condition_failed'), SKIPPED, SKIPPED, RUN),
        (('condition_failed', None), SKIPPED, WAIT, WAIT),
        (('completed', 'failed'), UPSTREAM_FAILED, UPSTREAM_FAILED, RUN),
        (('upstream_failed', 'completed'), UPSTREAM_FAILED, UPSTREAM_FAILED, RUN),
        (('failed', None), UPSTREAM_FAILED, UPSTREAM_FAILED, WAIT),
        (('failed', 'condition_failed'), UPSTREAM_FAILED, UPSTREAM_FAILED, RUN),
    ]

    def test_the_rules(self):
        rules = (
            TriggerRule.ALL_SUCCESS,
            TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            TriggerRule.ALL_DONE,
        )
        for statuses, *expected in self.CASES:
            for rule, decision in zip(rules, expected):
                self.assertEqual(decide(rule, statuses), decision, (rule, statuses))

    def test_a_block_without_upstream_blocks_runs(self):
        for rule in TriggerRule:
            self.assertEqual(decide(rule, []), RUN)


class ValidationTest(DBTestCase):
    def test_rules_and_where_they_apply(self):
        pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        block = list(pipeline.blocks_by_uuid.values())[1]

        for value in (None, '', 'all_success', 'all_done', 'none_failed_min_one_success'):
            self.assertIsNone(validation_error(pipeline, block, value), value)
        self.assertIn('not a trigger rule', validation_error(pipeline, block, 'one_success'))

        block.configuration = dict(dynamic=True)
        self.assertIn('dynamic', validation_error(pipeline, block, 'all_done'))

    def test_a_pipeline_with_an_all_done_block_goes_on_after_a_failure(self):
        pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        self.assertFalse(continues_after_failure(pipeline))
        block = list(pipeline.blocks_by_uuid.values())[-1]
        block.configuration = dict(trigger_rule='none_failed_min_one_success')
        self.assertFalse(continues_after_failure(pipeline))
        block.configuration = dict(trigger_rule='all_done')
        self.assertTrue(continues_after_failure(pipeline))
