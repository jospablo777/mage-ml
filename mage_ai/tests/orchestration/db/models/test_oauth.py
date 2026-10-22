import uuid

from mage_ai.orchestration.constants import Entity
from mage_ai.orchestration.db.models.oauth import Role
from mage_ai.tests.base_test import DBTestCase


class RoleTests(DBTestCase):
    def test_create_default_roles(self):
        Role.create_default_roles()

        owner = Role.query.filter(Role.name == 'Owner').one_or_none()
        self.assertIsNotNone(owner)

    def test_create_default_roles_project(self):
        test_entity_id = uuid.uuid4().hex
        Role.create_default_roles(
            entity=Entity.PROJECT,
            entity_id=test_entity_id,
            name_func=lambda x: f'test_{x}',
        )
        owner = Role.query.filter(Role.name == 'test_Owner').one_or_none()

        self.assertEqual(owner.get_access(entity=Entity.PROJECT, entity_id=test_entity_id), 1)

    def test_create_default_roles_multiple_projects(self):
        test_entity_id = uuid.uuid4().hex
        test_entity_id2 = uuid.uuid4().hex
        Role.create_default_roles(
            entity=Entity.PROJECT,
            entity_id=test_entity_id,
            name_func=lambda x: f'test-2_{x}',
        )
        Role.create_default_roles(
            entity=Entity.PROJECT,
            entity_id=test_entity_id2,
            name_func=lambda x: f'test-2_{x}',
        )
        owner = Role.query.filter(Role.name == 'test-2_Owner').one_or_none()

        self.assertEqual(owner.get_access(entity=Entity.PROJECT, entity_id=test_entity_id), 1)
        self.assertEqual(owner.get_access(entity=Entity.PROJECT, entity_id=test_entity_id2), 1)


class UserAccessTests(DBTestCase):
    def test_a_role_keeps_its_own_permissions_when_others_are_linked_to_it(self):
        from mage_ai.orchestration.db.models.oauth import (
            Permission,
            RolePermission,
            User,
            UserRole,
        )

        project = f'project_{uuid.uuid4().hex}'
        user = User.create(email=f'{uuid.uuid4().hex}@example.com', username=uuid.uuid4().hex)
        role = Role.create(name=f'editors_{uuid.uuid4().hex}')
        UserRole.create(user_id=user.id, role_id=role.id)
        # The role's own permission, as default roles are made: editor on the project.
        Permission.create(entity=Entity.PROJECT, entity_id=project, access=4, role_id=role.id)
        self.assertEqual(user.get_access(Entity.PROJECT, project), 4)

        # A permission linked to the role, as the roles page adds one: viewer of a pipeline.
        linked = Permission.create(entity=Entity.PIPELINE, entity_id='orders', access=8)
        RolePermission.create(role_id=role.id, permission_id=linked.id)

        # Both count: the linked list replaced the role's own, and the editor lost the project.
        self.assertEqual(user.get_access(Entity.PROJECT, project), 4)
        self.assertEqual(user.get_access(Entity.PIPELINE, 'orders'), 8)

        # The user's permissions list shows both too.
        listed = sorted((p.entity_id, p.access) for p in user.permissions())
        self.assertEqual(listed, [('orders', 8), (project, 4)])


class PolicyEntityTests(DBTestCase):
    def test_each_resource_of_a_response_is_checked_for_its_own_entity(self):
        from types import SimpleNamespace

        from mage_ai.api.policies.PipelinePolicy import PipelinePolicy
        from mage_ai.api.resources.PipelineResource import PipelineResource
        from mage_ai.api.result_set import ResultSet
        from mage_ai.data_preparation.repo_manager import get_project_uuid
        from mage_ai.orchestration.db.models.oauth import Permission, User, UserRole

        user = User.create(email=f'{uuid.uuid4().hex}@example.com', username=uuid.uuid4().hex)
        role = Role.create(name=f'viewers_{uuid.uuid4().hex}')
        UserRole.create(user_id=user.id, role_id=role.id)
        Permission.create(
            entity=Entity.PROJECT, entity_id=get_project_uuid(), access=8, role_id=role.id,
        )
        # Nothing on this pipeline, which the project's permission would otherwise give.
        Permission.create(entity=Entity.PIPELINE, entity_id='denied', access=0, role_id=role.id)

        resources = [
            PipelineResource(SimpleNamespace(uuid=name), user) for name in ('allowed', 'denied')
        ]
        ResultSet(resources)
        allowed, denied = (PipelinePolicy(r, user) for r in resources)

        self.assertTrue(allowed.has_at_least_viewer_role())
        # The first pipeline's answer was kept for the whole response, under the check's name.
        self.assertFalse(denied.has_at_least_viewer_role())
