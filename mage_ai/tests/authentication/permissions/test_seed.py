import asyncio

from mage_ai.authentication.permissions.seed import (
    ROLE_NAME_TO_KEY,
    bootstrap_permissions,
)
from mage_ai.orchestration.db.models.oauth import Permission, Role, RolePermission
from mage_ai.tests.base_test import DBTestCase


class SeedTest(DBTestCase):
    def test_seeding_makes_the_default_roles_with_the_policys_permissions_once(self):
        asyncio.run(bootstrap_permissions(policy_names=['Download']))

        roles = {role.name: role for role in Role.query.all()}
        for name in ROLE_NAME_TO_KEY.values():
            self.assertIn(name, roles)
        downloads = Permission.query.filter(Permission.entity_name == 'Download').all()
        self.assertGreaterEqual(len(downloads), 1)
        owner = roles['Owner default permissions']
        granted = {rp.permission_id for rp in RolePermission.query.filter(
            RolePermission.role_id == owner.id,
        ).all()}
        self.assertTrue(granted)

        counts = (Permission.query.count(), RolePermission.query.count(), len(roles))
        asyncio.run(bootstrap_permissions(policy_names=['Download']))
        self.assertEqual(
            (Permission.query.count(), RolePermission.query.count(), Role.query.count()),
            counts,
        )
