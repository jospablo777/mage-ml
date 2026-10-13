from mage_ai.server.kernels import KernelName, kernel_managers
from mage_ai.tests.api.endpoints.mixins import (
    BaseAPIEndpointTest,
    build_list_endpoint_tests,
    build_update_endpoint_tests,
)


class KernelAPIEndpointTest(BaseAPIEndpointTest):
    pass


build_list_endpoint_tests(
    KernelAPIEndpointTest,
    resource='kernels',
    list_count=0,
    result_keys_to_compare=[
        'alive',
        'id',
        'name',
        'usage',
    ],
)


build_update_endpoint_tests(
    KernelAPIEndpointTest,
    resource='kernels',
    get_resource_id=lambda _self: KernelName.PYTHON3,
    build_payload=lambda self: dict(action_type='restart'),
    get_model_before_update=lambda _self: kernel_managers[KernelName.PYTHON3],
    assert_after_update=lambda _self, _payload, _model: True,
)


class KernelActionTimeoutTest(BaseAPIEndpointTest):
    async def test_a_stuck_restart_fails_the_request_and_the_server_keeps_serving(self):
        """
        A restart that hung, in closing the kernel's ZeroMQ sockets, blocked the server's
        event loop, so no other request was answered until the server was killed.
        """
        import asyncio
        import threading
        import time
        from unittest.mock import patch

        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.tests.factory import create_user

        release = threading.Event()
        self.addCleanup(release.set)

        def stuck_restart(*_args, **_kwargs):
            release.wait(30)

        ticks = 0

        async def other_requests():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.02)
                ticks += 1

        with patch('mage_ai.api.resources.KernelResource.KERNEL_ACTION_TIMEOUT', 0.3), \
                patch('mage_ai.api.resources.KernelResource.switch_active_kernel'), \
                patch('mage_ai.kernels.default.models.restart_kernel', stuck_restart):
            ticker = asyncio.ensure_future(other_requests())
            started = time.monotonic()
            response = await BaseOperation(
                action=OperationType.UPDATE,
                pk=KernelName.PYTHON3,
                payload=dict(kernel=dict(action_type='restart')),
                resource='kernels',
                user=create_user(_owner=True),
            ).execute()
            ticker.cancel()

        self.assertLess(time.monotonic() - started, 5)
        self.assertIn('did not restart within 0.3 seconds', response['error']['message'])
        self.assertGreater(ticks, 5)
