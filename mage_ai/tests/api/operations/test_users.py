from unittest.mock import patch

from mage_ai.api.presenters.UserPresenter import UserPresenter
from mage_ai.orchestration.db.models.oauth import Role, User
from mage_ai.shared.array import find
from mage_ai.tests.api.operations.test_base import BaseApiTestCase
from mage_ai.tests.factory import create_user


@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
class UserOperationTests(BaseApiTestCase):
    model_class = User

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        Role.create_default_roles()

    async def test_execute_create(self):
        email = self.faker.email()
        response = await self.base_test_execute_create(dict(
            email=email,
            password='water_lightning',
            password_confirmation='water_lightning',
            roles_new=[Role.get_role('Owner').id],
        ))
        self.assertEqual(User.get_by_id(response['user']['id']).email, email)

    async def test_execute_create_unauthorized(self):
        async def _func():
            await self.base_test_execute_create(dict(
                email=self.faker.email(),
                password='water_lightning',
                password_confirmation='water_lightning',
            ), user=create_user(), after_create_count=3, before_create_count=2)

        await self.assertRaisesAsync(Exception, _func)

    async def test_execute_delete(self):
        user = create_user(email=self.faker.email())

        await self.base_test_execute_delete(user.id)

    async def test_execute_delete_unauthorized(self):
        user = create_user(email=self.faker.email())

        async def _func():
            await self.base_test_execute_delete(user.id, user=user)

        await self.assertRaisesAsync(Exception, _func)

    async def test_execute_detail(self):
        user = create_user()
        await self.base_test_execute_detail(user.id, dict(
            email=user.email,
            username=user.username,
        ))

    async def test_execute_detail_unauthorized(self):
        user = create_user()

        async def _func():
            await self.base_test_execute_detail(user.id, dict(
                email=user.email,
                username=user.username,
            ), user=create_user())

        await self.assertRaisesAsync(Exception, _func)

    async def test_execute_list(self):
        owner = create_user(_owner=True)

        email1 = self.faker.email()
        email2 = self.faker.email()

        response = await self.base_test_execute_list(
            [
                dict(
                    email=email1,
                    password='water_lightning',
                    password_confirmation='water_lightning',
                    roles_new=[Role.get_role('Editor').id],
                ),
                dict(
                    email=email2,
                    password='water_lightning',
                    password_confirmation='water_lightning',
                    roles_new=[Role.get_role('Admin').id],
                ),
            ],
            [
                'id',
            ],
            user=owner,
        )

        admin = User.get_by_id(find(lambda x: x['email'] == email2, response['users'])['id'])

        await self.base_test_execute_list(
            None,
            UserPresenter.default_attributes,
            skip_creation=True,
            user=admin,
        )

    async def test_execute_list_unauthorized(self):
        async def _func():
            await self.base_test_execute_list([], user=create_user())

        await self.assertRaisesAsync(Exception, _func)

    async def test_execute_update(self):
        user = create_user()
        new_username = self.faker.name()

        await self.base_test_execute_update(
            user.id,
            dict(username=new_username),
            dict(username=new_username),
            user=user,
        )

    async def test_execute_update_unauthorized(self):
        user = create_user()

        async def _func():
            await self.base_test_execute_update(user.id, {}, {}, user=create_user())

        await self.assertRaisesAsync(Exception, _func)


# Its own class: the users it makes changed the counts UserOperationTests compares.
@patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1)
class UserRoleGrantTests(BaseApiTestCase):
    @classmethod
    def setUpClass(self):
        super().setUpClass()
        Role.create_default_roles()

    async def test_an_admin_raises_no_one_to_admin_or_owner_through_role_ids(self):
        from mage_ai.api.operations import constants

        admin = create_user(roles_new=[Role.get_role('Admin')])
        editor = create_user(roles_new=[Role.get_role('Editor')])
        made = [admin, editor]
        self.addCleanup(lambda: [User.get_by_id(u.id).delete() for u in made])

        async def update(target, by, **user):
            return await self.build_operation(
                action=constants.UPDATE, resource='users', pk=target.id,
                payload=dict(user=user), user=by,
            ).execute()

        for target, role in ((admin, 'Owner'), (editor, 'Admin'), (editor, 'Owner')):
            with self.subTest(target=target.id, role=role):
                response = await update(target, admin, role_ids=[Role.get_role(role).id])
                # role_ids had none of the checks roles_new has: an admin made itself Owner.
                self.assertIn('error', response)
        self.assertEqual([r.name for r in User.get_by_id(admin.id).roles_new], ['Admin'])
        self.assertEqual([r.name for r in User.get_by_id(editor.id).roles_new], ['Editor'])

        # Nor through the older roles field, whose first bit is Admin.
        self.assertIn('error', await update(editor, admin, roles=1))

        owner = create_user(_owner=True)
        made.append(owner)
        response = await update(editor, owner, role_ids=[Role.get_role('Admin').id])
        self.assertNotIn('error', response)
        self.assertEqual([r.name for r in User.get_by_id(editor.id).roles_new], ['Admin'])
