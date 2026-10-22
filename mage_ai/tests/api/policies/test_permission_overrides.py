"""
The override conditions of the policies (all lambda _policy: True) belong to the user
permissions mode. Applied without it, they let any signed-in user, one with no role too,
through every rule that had one: global hooks, Git, compute clusters.
"""
from unittest.mock import patch

from mage_ai.api.errors import ApiError
from mage_ai.api.operations.constants import OperationType
from mage_ai.api.policies.ComputeClusterPolicy import ComputeClusterPolicy
from mage_ai.api.policies.GlobalHookPolicy import GlobalHookPolicy
from mage_ai.api.policies.SchedulerPolicy import SchedulerPolicy
from mage_ai.api.policies.VersionControlBranchPolicy import VersionControlBranchPolicy
from mage_ai.orchestration.db.models.oauth import Role
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
@patch('mage_ai.api.utils.REQUIRE_USER_AUTHENTICATION', 1)
class PermissionOverrideTest(AsyncDBTestCase):
    async def allowed(self, policy_class, action, user) -> bool:
        try:
            await policy_class(None, user).authorize_action(action)
            return True
        except ApiError:
            return False

    async def test_each_rule_holds_its_own_condition(self):
        Role.create_default_roles()
        nobody = create_user()
        viewer = create_user(roles_new=[Role.get_role('Viewer')])
        editor = create_user(roles_new=[Role.get_role('Editor')])
        admin = create_user(roles_new=[Role.get_role('Admin')])

        self.assertFalse(await self.allowed(ComputeClusterPolicy, OperationType.LIST, nobody))
        self.assertTrue(await self.allowed(ComputeClusterPolicy, OperationType.LIST, viewer))
        self.assertFalse(
            await self.allowed(VersionControlBranchPolicy, OperationType.UPDATE, viewer),
        )
        self.assertTrue(
            await self.allowed(VersionControlBranchPolicy, OperationType.UPDATE, editor),
        )
        self.assertFalse(await self.allowed(GlobalHookPolicy, OperationType.CREATE, editor))
        # A viewer stopped the scheduler, and with it every scheduled run.
        self.assertTrue(await self.allowed(SchedulerPolicy, OperationType.LIST, viewer))
        self.assertFalse(await self.allowed(SchedulerPolicy, OperationType.CREATE, viewer))
        self.assertTrue(await self.allowed(SchedulerPolicy, OperationType.CREATE, editor))
        self.assertTrue(await self.allowed(GlobalHookPolicy, OperationType.CREATE, admin))

    @patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_PERMISSIONS', True)
    async def test_with_user_permissions_the_overrides_still_apply(self):
        nobody = create_user()
        with patch.object(GlobalHookPolicy, 'action_rule', classmethod(
            lambda cls, action: {'private': [dict(condition=lambda policy: False)]},
        )), patch.object(GlobalHookPolicy, 'current_scope', lambda self: 'private'):
            self.assertTrue(await self.allowed(GlobalHookPolicy, OperationType.CREATE, nobody))
