import asyncio
import threading

from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.authentication.permissions.seed import bootstrap_permissions

# The name of the thread that seeds roles and permissions, so a caller can wait for it.
SEED_THREAD_NAME = 'seed-permissions'


class SeedResource(GenericResource):
    @classmethod
    async def create(self, payload, user, **kwargs):
        if payload.get('roles') and payload.get('permissions'):
            policy_names = payload.get('policy_names')
            t = threading.Thread(
                target=asyncio.run,
                args=(bootstrap_permissions(policy_names=policy_names),),
                name=SEED_THREAD_NAME,
            )
            t.start()

        return self({}, user, **kwargs)
