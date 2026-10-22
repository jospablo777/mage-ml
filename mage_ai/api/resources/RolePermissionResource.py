from mage_ai.api import role_grants
from mage_ai.api.resources.DatabaseResource import DatabaseResource
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.oauth import Permission, Role, RolePermission
from mage_ai.shared.hash import ignore_keys, merge_dict


class RolePermissionResource(DatabaseResource):
    model_class = RolePermission

    @classmethod
    @safe_db_query
    def create(self, payload, user, **kwargs):
        payload_updated = merge_dict(payload, dict(
            user_id=user.id if user else None,
        ))
        ids = list(payload_updated.get('permission_ids') or [])
        if payload_updated.get('permission_id'):
            ids.append(payload_updated['permission_id'])
        role_grants.check_permissions(
            user, Permission.query.filter(Permission.id.in_(ids)).all() if ids else [],
        )

        resources = []
        if 'permission_ids' in payload_updated:
            permission_ids = payload_updated.get('permission_ids') or []

            for permission_id in permission_ids:
                resources.append(super().create(merge_dict(
                    ignore_keys(payload_updated, ['permission_ids']),
                    dict(permission_id=permission_id),
                ), user, **kwargs))

            return resources[0] if len(resources) >= 1 else self(None, user, **kwargs)

        return super().create(payload_updated, user, **kwargs)


    @safe_db_query
    def delete(self, **kwargs):
        role_grants.check_permissions(
            self.current_user, [Permission.get_by_id(self.model.permission_id)],
        )
        return super().delete(**kwargs)


RolePermissionResource.register_parent_model('role_id', Role)
