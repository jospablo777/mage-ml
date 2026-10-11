import asyncio
import datetime
from typing import Dict

import jwt

from mage_ai.api.constants import DOWNLOAD_TOKEN_LIFESPAN
from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings import JWT_DOWNLOAD_SECRET
from mage_ai.settings.repo import get_repo_path
from mage_ai.shared.dates import utc_now


def download_token(name: str) -> str:
    """A download token for the zip of an export (server/api/downloads.py serves it)."""
    now = utc_now()
    return jwt.encode(
        {
            'exp': now + datetime.timedelta(seconds=DOWNLOAD_TOKEN_LIFESPAN * 10),
            'file_name': f'{name}.zip',
            'iat': now,
            'service_export': name,
        },
        JWT_DOWNLOAD_SECRET,
        algorithm='HS256',
    )


def _present_job(job: Dict) -> Dict:
    """A job's state, stage and progress, with the export's summary once it completed."""
    presented = {k: v for k, v in job.items() if k != 'result'}
    if job.get('state') == 'completed' and job.get('result'):
        presented.update(job['result'])
        presented['download_token'] = download_token(job['result']['name'])
    return presented


class PipelineServiceExportResource(GenericResource):
    """
    Export pipelines as a service from the app. POST /api/pipeline_service_exports starts
    writing the Docker build context `mage export service` writes, to the project's MageML
    data folder; GET /api/pipeline_service_exports/<name> gives its stage and progress, and
    once it ends what it holds with a token to download it as a zip, or its problems. With
    preview, POST returns what the export would hold without writing anything. GET lists the
    exports, newest first.
    """

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.pipeline_services.exports import list_exports

        found = list_exports(get_repo_path(user=user))
        for item in found:
            item['download_token'] = download_token(item['name'])
        return cls.build_result_set(found, user, **kwargs)

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'PipelineServiceExportResource':
        from mage_ai.data_preparation.models.pipeline import Pipeline
        from mage_ai.pipeline_services.exports import preview_pipelines, start_export

        repo_path = get_repo_path(user=user)
        pipeline_uuids = payload.get('pipeline_uuids') or []
        if not isinstance(pipeline_uuids, list) or not pipeline_uuids or not all(
            isinstance(uuid, str) for uuid in pipeline_uuids
        ):
            raise ApiError(dict(code=400, message='Choose at least one pipeline to export.'))
        missing = [
            uuid for uuid in pipeline_uuids
            if not Pipeline.get(uuid, repo_path=repo_path, check_if_exists=True)
        ]
        if missing:
            raise ApiError(dict(
                code=404, message=f'These pipelines do not exist: {", ".join(missing)}.',
            ))
        max_runs = payload.get('max_runs')
        if max_runs is not None and (not isinstance(max_runs, int) or max_runs < 1):
            raise ApiError(dict(code=400, message='Runs at once must be a whole number above 0.'))

        includes = payload.get('include') or []
        if not isinstance(includes, list) or not all(isinstance(i, str) for i in includes):
            raise ApiError(dict(code=400, message='include must be a list of project paths.'))

        if payload.get('preview'):
            result = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: preview_pipelines(
                    repo_path, pipeline_uuids, name=payload.get('name'), max_runs=max_runs,
                    includes=includes,
                ),
            )
            return cls(result, user, **kwargs)

        try:
            job = start_export(
                repo_path, pipeline_uuids, name=payload.get('name'), max_runs=max_runs,
                includes=includes,
            )
        except ValueError as error:
            raise ApiError(dict(code=400, message=str(error)))
        return cls(_present_job(job), user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'PipelineServiceExportResource':
        from mage_ai.pipeline_services.exports import export_job

        job = export_job(get_repo_path(user=user), pk)
        if not job:
            raise ApiError(dict(
                code=404, message=f'No export named {pk} ran since the server started.',
            ))
        return cls(_present_job(job), user, **kwargs)
