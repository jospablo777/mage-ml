import json
from pathlib import Path

from mage_ai.api.contracts import policy_matrix
from mage_ai.api.policies.BasePolicy import BasePolicy
from mage_ai.tests.base_test import DBTestCase

MATRIX = Path(__file__).resolve().parents[4] / 'contracts' / 'policy_matrix.json'


class PolicyMatrixTest(DBTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.matrix = policy_matrix.build(policy_matrix.make_users())

    def test_the_checked_in_matrix_is_current(self):
        self.assertTrue(
            MATRIX.read_text() == policy_matrix.dumps(self.matrix),
            'contracts/policy_matrix.json differs from the policies: run '
            'scripts/contracts/record.py and commit the file.',
        )

    def test_every_policy_and_action_has_a_row_per_mode(self):
        self.assertEqual(self.matrix['policies'], len(policy_matrix.policy_classes()))
        modes = {}
        for resource, action, row_modes, _ in self.matrix['actions']:
            modes.setdefault((resource, action), []).extend(row_modes)
        self.assertEqual(
            len(modes), self.matrix['policies'] * len(policy_matrix.ACTIONS),
        )
        for key, found in modes.items():
            self.assertEqual(sorted(found), list(policy_matrix.MODES), key)

    def test_rows_hold_what_the_policies_declare(self):
        rows = {
            (resource, action, tuple(modes)): roles
            for resource, action, modes, roles in self.matrix['actions']
        }
        # PipelinePolicy: viewers read, editors with pipeline edit access create.
        self.assertEqual(
            rows[('pipelines', 'list', (0, 1, 2))], ['owner', 'admin', 'editor', 'viewer'],
        )
        self.assertEqual(rows[('pipelines', 'create', (0,))], ['owner', 'admin', 'editor'])
        self.assertEqual(rows[('pipelines', 'create', (1, 2))], [])
        # UserPolicy: only the owner creates users; another user's record needs admin.
        self.assertEqual(rows[('users', 'create', (0, 1, 2))], ['owner'])
        self.assertEqual(rows[('users', 'detail', (0, 1, 2))], ['owner', 'admin'])
        # Signing in is the one action only the anonymous scope has.
        self.assertEqual(rows[('sessions', 'create', (0, 1, 2))], ['anonymous'])

    def test_settings_are_restored(self):
        from mage_ai.api.policies import BasePolicy as module

        before = (module.REQUIRE_USER_AUTHENTICATION, module.DISABLE_NOTEBOOK_EDIT_ACCESS)
        with policy_matrix.settings(1):
            self.assertTrue(module.REQUIRE_USER_AUTHENTICATION)
            self.assertEqual(module.DISABLE_NOTEBOOK_EDIT_ACCESS, 1)
        self.assertEqual(
            (module.REQUIRE_USER_AUTHENTICATION, module.DISABLE_NOTEBOOK_EDIT_ACCESS), before,
        )

    def test_dumps_writes_one_row_per_line(self):
        text = policy_matrix.dumps(self.matrix)
        self.assertEqual(json.loads(text)['actions'], self.matrix['actions'])
        self.assertEqual(
            len(text.splitlines()),
            len(self.matrix['actions']) + len(self.matrix['attributes']) + 9,
        )
        self.assertTrue(issubclass(policy_matrix.policy_classes()[0], BasePolicy))
