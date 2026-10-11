import json
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from mage_ai.data_preparation.models.block import plans


def stored(plan: plans.Plan):
    """A plan as a later stage reads it from storage: its JSON, without the variables."""
    return json.loads(json.dumps(plan))


class ResolveTest(TestCase):
    def test_a_stored_plan_runs_with_the_variables_of_the_run(self):
        """
        A stored plan kept its steps but not the run's variables. A later stage, a retry
        or fusion verification ran it without them: a Rust block that read the variable
        rows failed with "The pipeline has no variable rows".
        """
        variables = dict(rows=3)
        load = plans.Plan('rust', [dict(block_uuid='load', code='')], [], [], variables)
        double = plans.Plan('rust', [dict(block_uuid='double', code='')], [], ['load'], variables)
        pipeline = MagicMock()
        pipeline.get_block.return_value = SimpleNamespace(pipeline=pipeline)
        pipeline.variable_manager.get_variable.return_value = stored(load)
        block = SimpleNamespace(pipeline=pipeline)
        ran = []

        def run(plan, block, execution_partition=None):
            ran.append(plan)
            return SimpleNamespace(outputs=[len(ran)])

        with patch.object(plans, 'run', side_effect=run):
            self.assertEqual(plans.resolve(stored(double), block, 'p', variables), 2)

        self.assertEqual([plan.steps[0]['block_uuid'] for plan in ran], ['load', 'double'])
        self.assertEqual([plan.global_vars for plan in ran], [variables, variables])
