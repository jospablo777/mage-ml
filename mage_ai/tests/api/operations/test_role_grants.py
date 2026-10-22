"""
Admin and owner access go out by an owner's hand only: an admin who is not an owner takes
neither through user roles, permissions, role permissions or a role's lists, which each
handed it out before (role_grants.py). And a role updated without its lists keeps them.
"""
from unittest.mock import patch

from mage_ai.api.operations import constants
from mage_ai.orchestration.constants import Entity
from mage_ai.orchestration.db.models.oauth import (
    Permission,
    Role,
    RolePermission,
    User,
    UserRole,
)
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_user


@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
@patch('mage_ai.api.role_grants.REQUIRE_USER_AUTHENTICATION', 1)
class RoleGrantTests(BaseApiTestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        Role.create_default_roles()

    def setUp(self):
        super().setUp()
        self.admin = create_user(roles_new=[Role.get_role('Admin')])
        self.owner_role = Role.get_role('Owner')
        self.mine = Role.create(name=f'mine_{self.admin.id}')
        UserRole.create(user_id=self.admin.id, role_id=self.mine.id)
        self.owner_access = Permission.create(
            entity=Entity.GLOBAL, access=Permission.Access.OWNER,
        )

    async def call(self, action, resource, user, pk=None, **payload):
        return await self.build_operation(
            action=action, resource=resource, pk=pk, user=user,
            payload={resource[:-1]: payload} if payload else {},
        ).execute()

    async def test_an_admin_takes_no_owner_access_by_any_way(self):
        attempts = [
            ('user_roles', constants.CREATE, None,
             dict(user_id=self.admin.id, role_id=self.owner_role.id)),
            ('permissions', constants.CREATE, None,
             dict(entity=Entity.GLOBAL, access=Permission.Access.OWNER)),
            ('role_permissions', constants.CREATE, None,
             dict(role_id=self.mine.id, permission_id=self.owner_access.id)),
            ('roles', constants.UPDATE, self.mine.id,
             dict(permission_ids=[self.owner_access.id])),
            ('roles', constants.UPDATE, self.owner_role.id, dict(user_ids=[self.admin.id])),
        ]
        for resource, action, pk, payload in attempts:
            with self.subTest(resource=resource, action=action):
                response = await self.call(action, resource, self.admin, pk=pk, **payload)
                self.assertIn('error', response, response)

        admin = User.get_by_id(self.admin.id)
        self.assertFalse(admin.owner)
        self.assertEqual(
            RolePermission.query.filter(RolePermission.role_id == self.mine.id).count(), 0,
        )

    async def test_an_owner_gives_owner_access(self):
        owner = create_user(_owner=True)
        response = await self.call(
            constants.CREATE, 'user_roles', owner,
            user_id=self.admin.id, role_id=self.owner_role.id,
        )
        self.assertNotIn('error', response)
        self.assertTrue(User.get_by_id(self.admin.id).owner)

    async def test_a_role_renamed_alone_keeps_its_users_and_permissions(self):
        owner = create_user(_owner=True)
        editor_access = Permission.create(entity=Entity.GLOBAL, access=Permission.Access.EDITOR)
        RolePermission.create(role_id=self.mine.id, permission_id=editor_access.id)

        response = await self.call(
            constants.UPDATE, 'roles', owner, pk=self.mine.id, name='renamed',
        )

        self.assertNotIn('error', response)
        role = Role.get_by_id(self.mine.id)
        self.assertEqual(role.name, 'renamed')
        self.assertEqual([u.id for u in role.users], [self.admin.id])
        self.assertEqual([p.id for p in role.role_permissions], [editor_access.id])
