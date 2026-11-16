import base64
import binascii
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.data_preparation import project_archive
from mage_ai.settings.repo import get_repo_path


def _invalid(message: str) -> ApiError:
    return ApiError(dict(ApiError.RESOURCE_INVALID, message=message))


def _content(payload: Dict) -> bytes:
    try:
        return base64.b64decode(str(payload.get('content') or ''), validate=True)
    except (binascii.Error, ValueError):
        raise _invalid('content must be the archive in base64.')


async def _refresh_caches(repo_path: str, uuids) -> None:
    from mage_ai.cache.block import BlockCache
    from mage_ai.cache.pipeline import PipelineCache
    from mage_ai.cache.tag import TagCache
    from mage_ai.data_preparation.models.pipeline import Pipeline

    pipeline_cache = await PipelineCache.initialize_cache()
    block_cache = await BlockCache.initialize_cache()
    tag_cache = await TagCache.initialize_cache()
    for uuid in uuids:
        pipeline = Pipeline.get(uuid, repo_path=repo_path, check_if_exists=True)
        if pipeline is None:
            continue
        pipeline_cache.update_model(pipeline)
        for tag in pipeline.tags or []:
            tag_cache.add_pipeline(tag, pipeline)
        for block in pipeline.blocks_by_uuid.values():
            block_cache.add_pipeline(block, pipeline, repo_path)


class ProjectArchiveResource(GenericResource):
    """
    POST /api/project_archives (data_preparation/project_archive.py), by action:
    export: pipeline_uuids (all when empty) -> content, the zip in base64, and file_name;
    plan: content, mode -> what an import would do, file by file;
    import: content, mode -> the plan and the files written.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'ProjectArchiveResource':
        repo_path = get_repo_path(user=user)
        action = payload.get('action')
        try:
            if action == 'export':
                content = project_archive.export(repo_path, payload.get('pipeline_uuids') or None)
                model = dict(
                    content=base64.b64encode(content).decode('ascii'),
                    file_name='mageml-project.zip',
                    size=len(content),
                )
            elif action in ('plan', 'import'):
                mode = payload.get('mode') or 'skip'
                if action == 'plan':
                    model = dict(plan=project_archive.plan(repo_path, _content(payload), mode))
                else:
                    result = project_archive.import_archive(repo_path, _content(payload), mode)
                    await _refresh_caches(repo_path, [
                        p['uuid'] for p in result['pipelines'] if p['action'] != 'skip'
                    ])
                    model = dict(plan=result, written=result['written'])
            else:
                raise _invalid('action must be export, plan or import.')
        except project_archive.ArchiveError as error:
            raise _invalid(str(error))
        return cls(dict(model, id=action), user, **kwargs)
