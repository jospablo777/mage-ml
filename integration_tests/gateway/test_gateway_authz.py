"""
MAGE_GATEWAY_AUTHZ_SHADOW: the gateway decides every API read from the recorded policies
(contracts/policy_matrix.json) and compares its decision with the server's answer. Every
recorded GET, read as the owner, an editor, a viewer, a user with no role and a caller
without a session, is decided as the server decides it.
"""
import json
import time

import pytest
import requests

from integration_tests.gateway.test_gateway import SPEC, run_server

ROLES = dict(editor=4, viewer=8, none=None)


@pytest.fixture(scope='module')
def authz(tmp_path_factory):
    for port, _upstream, folder in run_server(
        tmp_path_factory, MAGE_GATEWAY_AUTHZ_SHADOW='1',
    ):
        yield port, folder


def session_for(gateway, email, password):
    from mage_ai.settings.server import OAUTH2_APPLICATION_CLIENT_ID

    session = requests.Session()
    session.headers['X-API-KEY'] = OAUTH2_APPLICATION_CLIENT_ID
    if email:
        token = session.post(f'http://127.0.0.1:{gateway}/api/sessions', json=dict(
            session=dict(email=email, password=password),
        ), timeout=60).json()['session']['token']
        session.cookies.set('oauth_token', token)
    return session


def add_users(folder):
    """A user per project role, given by a global permission, and one without a role."""
    import sqlite3

    from mage_ai.authentication.passwords import create_bcrypt_hash, generate_salt

    [database] = list(folder.glob('*.db')) or list((folder / 'data').rglob('mage-ai.db'))
    with sqlite3.connect(database) as connection:
        for name, access in ROLES.items():
            connection.execute(
                'INSERT INTO "user" (email, username, password_hash, password_salt, owner) '
                'VALUES (?, ?, ?, \'\', 0)',
                (f'{name}@example.com', name, create_bcrypt_hash('password', generate_salt())),
            )
            if access is None:
                continue
            user = connection.execute(
                'SELECT id FROM "user" WHERE username = ?', (name,),
            ).fetchone()[0]
            connection.execute('INSERT INTO role (name) VALUES (?)', (f'{name}s',))
            role = connection.execute(
                'SELECT id FROM role WHERE name = ?', (f'{name}s',),
            ).fetchone()[0]
            connection.execute(
                'INSERT INTO user_role (user_id, role_id) VALUES (?, ?)', (user, role),
            )
            connection.execute(
                'INSERT INTO permission (entity, entity_id, access, role_id) '
                "VALUES ('GLOBAL', NULL, ?, ?)",
                (access, role),
            )


def test_every_recorded_read_is_decided_as_the_server_decides_it(authz):
    gateway, folder = authz
    add_users(folder)
    sessions = dict(
        owner=session_for(gateway, 'admin@admin.com', 'admin'),
        anonymous=session_for(gateway, None, None),
        **{name: session_for(gateway, f'{name}@example.com', 'password') for name in ROLES},
    )
    spec = json.loads(SPEC.read_text())
    paths = sorted(p for p, ops in spec['paths'].items()
                   if 'get' in ops and '{' not in p and p.startswith('/api/'))
    for session in sessions.values():
        for path in paths:
            session.get(f'http://127.0.0.1:{gateway}{path}', timeout=60)

    def report():
        return requests.get(f'http://127.0.0.1:{gateway}/__gateway/shadow', timeout=10).json()

    previous = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        counts = report().get('authorization', {})
        if counts == previous:
            break
        previous = counts
        time.sleep(1)
    found = report()
    counts = found['authorization']
    total = {k: sum(c[k] for c in counts.values()) for k in ('agree', 'disagree', 'failed')}
    disagreements = [d for d in found['recent_differences'] if 'authorization' in d]
    assert total['disagree'] == 0, json.dumps(disagreements, indent=1)
    assert total['failed'] == 0, counts
    # Most reads are decided: every role, most recorded routes.
    assert total['agree'] >= len(paths) * 3, total
