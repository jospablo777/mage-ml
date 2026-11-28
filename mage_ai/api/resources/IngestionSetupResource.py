import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class IngestionSetupResource(GenericResource):
    """
    Guided ingestion (data_preparation/ingestion.py, ADR-0045): POST /api/ingestion_setups
    with an action: check_source (source), check_destination (destination, schema), sample
    (source, table), review (tables, deletes, schema, prefix, kind) or create (name, source,
    destination, tables, deletes, schema, prefix), which writes the streaming pipeline. A
    source's `kind` is postgres or mysql. Passwords are used to connect and stored as
    secrets; responses never hold them.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'IngestionSetupResource':
        from mage_ai.data_preparation import ingestion

        action = payload.get('action')
        source = payload.get('source') or {}
        destination = payload.get('destination') or {}
        tables = payload.get('tables') or []
        deletes = payload.get('deletes') or 'delete'
        schema = payload.get('schema') or 'public'
        prefix = payload.get('prefix') or ''
        for value in (source, destination):
            if not isinstance(value, dict):
                raise ApiError(dict(code=400, message='A connection is a mapping.'))
        try:
            if action == 'check_source':
                result = await asyncio.to_thread(ingestion.check_source, source)
            elif action == 'check_destination':
                result = await asyncio.to_thread(ingestion.check_destination, destination, schema)
            elif action == 'sample':
                result = await asyncio.to_thread(ingestion.sample, source, payload.get('table'))
            elif action == 'review':
                result = dict(review=ingestion.review(
                    tables, deletes, schema, prefix,
                    payload.get('kind') or source.get('kind') or 'postgres',
                ))
            elif action == 'create':
                result = await asyncio.to_thread(
                    ingestion.create, get_repo_path(user=user), payload.get('name'), source,
                    destination, tables, deletes, schema, prefix,
                    getattr(user, 'username', None) if user else None,
                )
            else:
                raise ApiError(dict(
                    code=400,
                    message='action is check_source, check_destination, sample, review or '
                            'create.',
                ))
        except ingestion.IngestionError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(dict(result, action=action), user, **kwargs)
