from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.server import health


class ServerHealthResource(GenericResource):
    """
    GET /api/server_health/current: the machine's load, memory and disk, MageML's processes,
    the size of what it stores and six hours of history (server/health.py).
    """

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'ServerHealthResource':
        return cls(dict(health.report(), id='current'), user, **kwargs)
