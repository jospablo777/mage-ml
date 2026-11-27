import asyncio

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource


class IncidentResource(GenericResource):
    """
    GET /api/incidents[?days=7]: the pipelines with a failed run in the last days, still
    failing first (orchestration/incidents.py recent), each with its latest failure's
    blocks and error lines and the run that recovered it, if any.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.orchestration import incidents

        days = (query or {}).get('days')
        if isinstance(days, list):
            days = days[0] if days else None
        try:
            days = int(days) if days else 7
        except (TypeError, ValueError):
            raise ApiError(dict(code=400, message='days is a number of days, such as 7.'))
        if not 1 <= days <= 90:
            raise ApiError(dict(code=400, message='days is between 1 and 90.'))
        rows = await asyncio.to_thread(incidents.recent, days)
        return cls.build_result_set(rows, user, **kwargs)
