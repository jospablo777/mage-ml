from mage_ai.api import role_grants
from mage_ai.api.resources.DatabaseResource import DatabaseResource
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.oauth import Role, User, UserRole


class UserRoleResource(DatabaseResource):
    model_class = UserRole

    @classmethod
    @safe_db_query
    def create(self, payload, user, **kwargs):
        parent = kwargs.get('parent_model')
        role_id = parent.id if isinstance(parent, Role) else payload.get('role_id')
        role_grants.check_roles(user, [Role.get_by_id(role_id)] if role_id else [])
        user_id = payload.get('user_id')
        role_grants.check_users(user, [User.get_by_id(user_id)] if user_id else [])
        return super().create(payload, user, **kwargs)

    @safe_db_query
    def update(self, payload, **kwargs):
        roles = {self.model.role_id, payload.get('role_id', self.model.role_id)}
        users = {self.model.user_id, payload.get('user_id', self.model.user_id)}
        role_grants.check_roles(self.current_user, [Role.get_by_id(i) for i in roles if i])
        role_grants.check_users(self.current_user, [User.get_by_id(i) for i in users if i])
        return super().update(payload, **kwargs)

    @safe_db_query
    def delete(self, **kwargs):
        role_grants.check_roles(self.current_user, [Role.get_by_id(self.model.role_id)])
        role_grants.check_users(self.current_user, [User.get_by_id(self.model.user_id)])
        return super().delete(**kwargs)


UserRoleResource.register_parent_model('role_id', Role)
