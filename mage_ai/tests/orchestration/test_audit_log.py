"""
The audit log of API writes: who, what and how it ended, values that could hold secrets
hidden, long ones as their length and hash, repeated saves merged, old events pruned.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import OperationType
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import audit_log
from mage_ai.orchestration.db.models.audit_events import AuditEvent
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import create_user


class SummarizeTest(AsyncDBTestCase):
    def test_values(self):
        code = 'x = 1\n' * 100
        self.assertEqual(audit_log.changes_of('blocks', dict(api_key='k', block=dict(
            name='load', content=code, password='p', config=dict(api_token='t', rows=5),
            tags=['a', 'b'],
        ))), dict(
            name='load',
            content=dict(length=len(code), sha256=audit_log._digest(code)['sha256']),
            password='[hidden]',
            config=dict(api_token='[hidden]', rows=5),
            tags=['a', 'b'],
        ))
        self.assertEqual(
            audit_log.changes_of('secrets', dict(secret=dict(name='db', value='s3cret'))),
            dict(name='[hidden]', value='[hidden]'),
        )


class AuditLogTest(AsyncDBTestCase):
    def setUp(self):
        super().setUp()
        AuditEvent.query.delete()
        self.owner = create_user(_owner=True, email=self.faker.unique.email())
        self.viewer = create_user(email=self.faker.unique.email(), roles=8)

    async def write(self, action, user, **options):
        return await BaseOperation(action=action, user=user, **options).execute()

    def events(self):
        return AuditEvent.query.order_by(AuditEvent.id).all()

    async def test_writes_are_recorded_with_their_outcome(self):
        name = self.faker.unique.name()
        with patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1):
            created = await self.write(
                OperationType.CREATE, self.owner, resource='pipelines',
                payload=dict(pipeline=dict(name=name, type='python')),
            )
            refused = await self.write(
                OperationType.CREATE, self.viewer, resource='pipelines',
                payload=dict(pipeline=dict(name=f'{name} 2', type='python')),
            )
            await self.write(OperationType.DETAIL, self.owner, resource='pipelines',
                             pk=created['pipeline']['uuid'])

        self.assertNotIn('error', created)
        self.assertEqual(refused['error']['code'], 403)
        events = self.events()
        self.assertEqual([(e.action, e.object_type, e.outcome) for e in events], [
            ('create', 'pipelines', 'succeeded'), ('create', 'pipelines', 'refused'),
        ])
        self.assertEqual(events[0].user_name, self.owner.email)
        self.assertEqual(events[0].changes, dict(name=name, type='python'))
        self.assertEqual(events[1].user_id, self.viewer.id)
        self.assertTrue(events[1].error)
        Pipeline.get(created['pipeline']['uuid'], repo_path=self.repo_path).delete()

    async def test_a_sign_in_names_its_user_and_hides_the_password(self):
        audit_log.record('create', 'sessions', payload=dict(session=dict(
            email='ana@example.com', password='wrong',  # pragma: allowlist secret
        )), outcome='failed', error='Email/username and password combination is invalid.')

        event = self.events()[0]
        self.assertEqual((event.user_name, event.user_id, event.outcome),
                         ('ana@example.com', None, 'failed'))
        self.assertEqual(event.changes, dict(email='ana@example.com', password='[hidden]'))

    async def test_saves_within_a_minute_are_one_event(self):
        for rows in (1, 2, 3):
            audit_log.record('update', 'blocks', object_id='load', user=self.owner,
                             payload=dict(block=dict(rows=rows)))
        audit_log.record('update', 'blocks', object_id='other', user=self.owner,
                         payload=dict(block=dict(rows=9)))

        events = self.events()
        self.assertEqual([(e.object_id, e.count, e.changes) for e in events], [
            ('load', 3, dict(rows=3)), ('other', 1, dict(rows=9)),
        ])

    async def test_old_events_are_pruned_and_the_log_can_be_off(self):
        audit_log.record('delete', 'pipelines', object_id='gone', user=self.owner)
        old = self.events()[0]
        old.created_at = datetime.now(timezone.utc) - timedelta(days=400)
        old.save()
        audit_log.record('delete', 'pipelines', object_id='recent', user=self.owner)

        self.assertEqual(audit_log.prune(force=True), 1)
        self.assertEqual([e.object_id for e in self.events()], ['recent'])

        with patch.dict('os.environ', {'MAGE_AUDIT_LOG': 'off'}):
            audit_log.record('delete', 'pipelines', object_id='unlogged', user=self.owner)
        self.assertEqual(len(self.events()), 1)

    async def test_a_recording_failure_does_not_fail_the_request(self):
        with patch.object(audit_log, '_record', side_effect=RuntimeError('database gone')):
            audit_log.record('delete', 'pipelines', object_id='x', user=self.owner)

    async def test_owners_read_the_log_and_viewers_do_not(self):
        audit_log.record('delete', 'pipelines', object_id='a', user=self.owner)
        audit_log.record('update', 'blocks', object_id='b', user=self.viewer,
                         payload=dict(block=dict(rows=1)))

        with patch('mage_ai.api.policies.BasePolicy.REQUIRE_USER_AUTHENTICATION', 1):
            listed = await self.write(OperationType.LIST, self.owner, resource='audit_events')
            filtered = await self.write(OperationType.LIST, self.owner, resource='audit_events',
                                        query=dict(object_type=['blocks']))
            refused = await self.write(OperationType.LIST, self.viewer, resource='audit_events')
            invalid = await self.write(OperationType.LIST, self.owner, resource='audit_events',
                                       query=dict(since=['yesterday']))

        self.assertEqual([e['object_id'] for e in listed['audit_events']], ['b', 'a'])
        self.assertEqual(listed['metadata']['count'], 2)
        self.assertEqual([e['user_name'] for e in filtered['audit_events']], [self.viewer.email])
        self.assertEqual(refused['error']['code'], 403)
        self.assertIn('ISO format', invalid['error']['message'])
