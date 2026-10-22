"""
Who may hand out admin and owner access: owners only. The roles page offers non-owners
editor and viewer roles only (RoleResource.collection, limit_roles), and UserResource
refuses an admin who grants Admin or Owner, yet roles, permissions, their links and user
roles are four more ways to the same grant, and an admin could take Owner through any of
them. Each takes the checks below.
"""
from typing import Iterable

from mage_ai.api.errors import ApiError
from mage_ai.orchestration.db.models.oauth import Permission, Role, RolePermission, User
from mage_ai.settings import REQUIRE_USER_AUTHENTICATION

ELEVATED = Permission.Access.OWNER | Permission.Access.ADMIN


def _refuse(message: str) -> None:
    error = ApiError.RESOURCE_INVALID.copy()
    error.update(message=message)
    raise ApiError(error)


def _owner(user) -> bool:
    # Without authentication there is no one to keep out, as in the policies.
    return not REQUIRE_USER_AUTHENTICATION or user is None or bool(user.owner)


def role_access(role: Role) -> int:
    """
    Every access bit the role's permissions hold, its own and those linked to it. Not with
    Role.fetch_permissions: its cached query answered the response's loader after a delete
    with the rows the role had.
    """
    own = Permission.query.filter(Permission.role_id == role.id).with_entities(
        Permission.access,
    )
    linked = Permission.query.join(
        RolePermission, RolePermission.permission_id == Permission.id,
    ).filter(RolePermission.role_id == role.id).with_entities(Permission.access)
    access = 0
    for (value,) in list(own) + list(linked):
        access |= value or 0
    return access


def check_permissions(user, permissions: Iterable[Permission]) -> None:
    """Creating, changing, deleting or linking these permissions."""
    if _owner(user):
        return
    if any((p.access or 0) & ELEVATED for p in permissions if p is not None):
        _refuse('Only owners give, change or take away admin or owner access.')


def check_access(user, access) -> None:
    """A permission about to hold this access."""
    if not _owner(user) and access is not None and int(access) & ELEVATED:
        _refuse('Only owners give, change or take away admin or owner access.')


def check_roles(user, roles: Iterable[Role]) -> None:
    """Giving these roles to users, taking them away, or changing what they hold."""
    if _owner(user):
        return
    if any(role_access(role) & ELEVATED for role in roles if role is not None):
        _refuse('Only owners give, change or take away admin or owner roles.')


def check_users(user, users: Iterable[User]) -> None:
    """Changing these users' roles: an owner's, or another admin's, only by an owner."""
    if _owner(user):
        return
    for target in users:
        if target is None:
            continue
        if target.owner:
            _refuse("Only owners change an owner's roles.")
        if target.is_admin and target.id != user.id:
            _refuse("Only owners change another admin's roles.")
