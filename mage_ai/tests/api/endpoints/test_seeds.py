import threading
from unittest.mock import patch

from mage_ai.api.resources.SeedResource import SEED_THREAD_NAME
from mage_ai.authentication.permissions.constants import PermissionAccess
from mage_ai.tests.api.endpoints.mixins import (
    BaseAPIEndpointTest,
    build_create_endpoint_tests,
)


class SeedAPIEndpointTest(BaseAPIEndpointTest):
    def setUp(self):
        super().setUp()
        # Seeding itself is tested in test_seed.py; here, that the endpoint starts it.
        self.seeded = []

        async def bootstrap(policy_names=None):
            self.seeded.append(policy_names)

        patcher = patch('mage_ai.api.resources.SeedResource.bootstrap_permissions', bootstrap)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        # Seeding runs in a thread that reads the database: it ends before the test class
        # closes the session, or it fails on a closed one during a later test.
        for thread in threading.enumerate():
            if thread.name == SEED_THREAD_NAME:
                thread.join(timeout=60)
        super().tearDown()


build_create_endpoint_tests(
    SeedAPIEndpointTest,
    resource='seed',
    authentication_accesses=[PermissionAccess.OWNER],
    permissions_accesses=[PermissionAccess.OWNER],
    build_payload=lambda self: dict(
        roles=True,
        permissions=True,
        policy_names=['Block', 'Pipeline', 'Download'],
    ),
    assert_before_create_count=lambda self: True,
    assert_after_create_count=lambda self: True,
)
