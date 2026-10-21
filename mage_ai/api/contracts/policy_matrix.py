"""
The policy matrix: what every API policy allows, by role, resource, action and setting,
recorded from the Python policies themselves (ROADMAP P0). The Rust gateway and the
authorization engine that replace them are held to it (conformance per resource).

Each check runs the policy's own authorize_action, authorize_attribute or authorize_query
with user authentication on, for one user per role, on a record of the resource that the
user does not own (a user record other than their own), at project level. Nothing is
inferred from the rule declarations, so a condition the declarations hide (the owner
shortcut, scope checks, an override) is in the result.

Rows, one per line in contracts/policy_matrix.json (scripts/contracts/record.py):
    actions:    [resource, action, [modes], [roles allowed]]
    attributes: [resource, read | write | query, action, [modes], {"role role": [names]}]
A row holds the modes that give the same result; roles that are allowed the same names
share a key, and a role allowed none is left out. Query name * stands for any parameter.

Roles: owner, admin, editor, viewer (the user's roles), none (a signed-in user with no
role) and anonymous (no user, the public scope). Modes are DISABLE_NOTEBOOK_EDIT_ACCESS
values: 0 (edit allowed), 1 (notebook and pipeline edits off) and 2 (pipeline edits off).
With user authentication off, every action and attribute is allowed apart from what the
modes turn off; the matrix does not record that case.
"""
import asyncio
import importlib
import inspect
import os
from contextlib import contextmanager
from typing import Awaitable, Dict, Iterator, List, Optional, Tuple

from mage_ai.api.constants import AttributeType
from mage_ai.api.errors import ApiError
from mage_ai.api.operations.constants import OperationType
from mage_ai.api.result_set import ResultSet

POLICIES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'policies')
ROLES = ('owner', 'admin', 'editor', 'viewer', 'none', 'anonymous')
# User.roles bits: admin 1, editor 2, viewer 4.
ROLE_BITS = {'admin': 1, 'editor': 2, 'viewer': 4, 'none': 0}
MODES = (0, 1, 2)
ACTIONS = (
    OperationType.CREATE,
    OperationType.DELETE,
    OperationType.DETAIL,
    OperationType.LIST,
    OperationType.UPDATE,
)
ALL_QUERIES = '*'
# The modules that read the settings when a policy runs.
SETTING_MODULES = (
    'mage_ai.api.policies.BasePolicy',
    'mage_ai.api.policies.mixins.user_permissions',
    'mage_ai.api.utils',
    'mage_ai.settings.server',
)


class OtherRecord:
    """A record of the resource that is not the user's: a user other than the caller."""

    id = -1
    model = None
    # No parent pipeline: pipeline interactions do not apply.
    pipeline = None

    def __init__(self, result_set: ResultSet):
        self._result_set = result_set

    def result_set(self) -> ResultSet:
        return self._result_set


def policy_classes() -> List[type]:
    """Every policy class in mage_ai/api/policies, by name."""
    from mage_ai.api.policies.BasePolicy import BasePolicy

    classes = []
    for file in sorted(os.listdir(POLICIES_DIR)):
        if not file.endswith('Policy.py') or file == 'BasePolicy.py':
            continue
        name = file[:-len('.py')]
        module = importlib.import_module(f'mage_ai.api.policies.{name}')
        cls = getattr(module, name, None)
        if inspect.isclass(cls) and issubclass(cls, BasePolicy):
            classes.append(cls)
    return classes


@contextmanager
def settings(mode: int) -> Iterator[None]:
    """User authentication on, user permissions off, DISABLE_NOTEBOOK_EDIT_ACCESS=mode."""
    values = dict(
        DISABLE_NOTEBOOK_EDIT_ACCESS=mode,
        REQUIRE_USER_AUTHENTICATION=True,
        REQUIRE_USER_PERMISSIONS=False,
    )
    saved = []
    for name in SETTING_MODULES:
        module = importlib.import_module(name)
        for key, value in values.items():
            if hasattr(module, key):
                saved.append((module, key, getattr(module, key)))
                setattr(module, key, value)
    try:
        yield
    finally:
        for module, key, value in reversed(saved):
            setattr(module, key, value)


def make_users() -> Dict[str, Optional[object]]:
    """One saved user per role; anonymous is no user. Needs the database."""
    from mage_ai.orchestration.db.models.oauth import User

    users = {}
    for role in ROLES:
        if role == 'anonymous':
            users[role] = None
            continue
        user = User(
            email=f'policy-matrix-{role}@example.com',
            _owner=role == 'owner',
            roles=ROLE_BITS.get(role, 0),
            username=f'policy-matrix-{role}',
        )
        user.save()
        users[role] = user
    return users


async def _allowed(check: Awaitable) -> bool:
    try:
        await check
        return True
    except ApiError:
        return False


def _declared(rules: Dict) -> List[str]:
    return sorted(str(key) for key in (rules or {}))


def _grouped(by_role: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Roles with the same names allowed, joined: {'owner admin': [...], 'viewer': [...]}."""
    groups: Dict[Tuple[str, ...], List[str]] = {}
    for role in ROLES:
        names = tuple(by_role.get(role) or ())
        if names:
            groups.setdefault(names, []).append(role)
    return {' '.join(roles): list(names) for names, roles in groups.items()}


async def _resource_rows(
    cls: type,
    users: Dict[str, Optional[object]],
    result_sets: Dict[str, ResultSet],
) -> Tuple[Dict, Dict]:
    """{(resource, action): roles allowed} and {(resource, kind, action): grouped names}."""
    resource = cls.resource_name()
    actions = {}
    attributes = {}
    reads = _declared(cls.read_rules.get(cls.__name__))
    writes = _declared(cls.write_rules.get(cls.__name__))
    queries = _declared(cls.query_rules.get(cls.__name__))

    def policy(role: str, **options):
        return cls(OtherRecord(result_sets[role]), users[role], **options)

    for action in ACTIONS:
        allowed = []
        for role in ROLES:
            if await _allowed(policy(role).authorize_action(action)):
                allowed.append(role)
        actions[(resource, str(action))] = allowed

        for kind, names in (('read', reads), ('write', writes), ('query', queries)):
            if not names:
                continue
            by_role = {}
            for role in ROLES:
                checked = policy(role, api_operation_action=action)
                if kind == 'query':
                    by_role[role] = [
                        ALL_QUERIES if name == AttributeType.ALL else name
                        for name in names
                        if await _allowed(checked.authorize_query({name: '1'}))
                    ]
                else:
                    by_role[role] = [
                        name for name in names
                        if await _allowed(checked.authorize_attribute(kind, name))
                    ]
            attributes[(resource, kind, str(action))] = _grouped(by_role)
    return actions, attributes


def _rows(by_mode: Dict[int, Dict[Tuple, object]]) -> List[List]:
    """One row per key and result, with the modes that give it: [*key, [modes], result]."""
    rows = []
    for key in sorted({key for results in by_mode.values() for key in results}):
        modes_by_result: Dict[str, Tuple[object, List[int]]] = {}
        for mode in MODES:
            result = by_mode[mode].get(key)
            marker = repr(result)
            modes_by_result.setdefault(marker, (result, []))[1].append(mode)
        for result, modes in modes_by_result.values():
            rows.append([*key, modes, result])
    return rows


async def build_async(users: Dict[str, Optional[object]]) -> Dict:
    classes = policy_classes()
    actions = {}
    attributes = {}
    for mode in MODES:
        actions[mode] = {}
        attributes[mode] = {}
        # Per role and mode, one context: its role checks run once (all at project level).
        result_sets = {role: ResultSet([]) for role in ROLES}
        with settings(mode):
            for cls in classes:
                action_rows, attribute_rows = await _resource_rows(cls, users, result_sets)
                actions[mode].update(action_rows)
                attributes[mode].update(attribute_rows)
    return dict(
        actions=_rows(actions),
        attributes=_rows(attributes),
        modes={
            '0': 'DISABLE_NOTEBOOK_EDIT_ACCESS=0, edits allowed',
            '1': 'DISABLE_NOTEBOOK_EDIT_ACCESS=1, notebook and pipeline edits off',
            '2': 'DISABLE_NOTEBOOK_EDIT_ACCESS=2, pipeline edits off',
        },
        policies=len(classes),
        roles=list(ROLES),
    )


def build(users: Dict[str, Optional[object]]) -> Dict:
    return asyncio.run(build_async(users))


def dumps(matrix: Dict) -> str:
    """JSON with one row per line, so a change to a policy is a change to its rows."""
    import json

    def line(value) -> str:
        return json.dumps(value, sort_keys=True, separators=(', ', ': '))

    parts = ['{']
    for key in ('modes', 'policies', 'roles'):
        parts.append(f'  {json.dumps(key)}: {line(matrix[key])},')
    for key, last in (('actions', False), ('attributes', True)):
        rows = matrix[key]
        parts.append(f'  {json.dumps(key)}: [')
        parts += [f'    {line(row)}{"," if i < len(rows) - 1 else ""}' for i, row in
                  enumerate(rows)]
        parts.append('  ]' + ('' if last else ','))
    parts.append('}')
    return '\n'.join(parts) + '\n'
