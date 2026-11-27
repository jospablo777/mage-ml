import asyncio
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.api.resources.PipelineCompilationResource import _pipeline


def _query_pipeline(query, user):
    value = (query or {}).get('pipeline_uuid')
    if isinstance(value, list):
        value = value[0] if value else None
    return _pipeline(value, user)


class ChangePreviewResource(GenericResource):
    """
    Change previews (orchestration/previews.py, ADR-0044): POST /api/change_previews with
    pipeline_uuid (and baseline_run_id, keys) starts one against a completed run; GET
    /api/change_previews?pipeline_uuid= lists the pipeline's; GET
    /api/change_previews/<id>?pipeline_uuid= returns one with each block's comparison.
    """

    @classmethod
    async def create(cls, payload: Dict, user, **kwargs) -> 'ChangePreviewResource':
        from mage_ai.orchestration import previews
        from mage_ai.orchestration.db.models.schedules import PipelineRun

        pipeline = _pipeline(payload.get('pipeline_uuid'), user)
        baseline = None
        run_id = payload.get('baseline_run_id')
        if run_id is not None:
            if isinstance(run_id, bool) or not isinstance(run_id, int):
                raise ApiError(dict(code=400, message='baseline_run_id is a run id.'))
            baseline = PipelineRun.get(run_id)
            if baseline is None or baseline.pipeline_uuid != pipeline.uuid:
                raise ApiError(dict(code=404, message=f'{pipeline.uuid} has no run {run_id}.'))
        keys = payload.get('keys') or {}
        if not isinstance(keys, dict) or not all(
            isinstance(v, list) and all(isinstance(c, str) for c in v) for v in keys.values()
        ):
            raise ApiError(dict(code=400, message='keys is {block: [column, ...]}.'))
        try:
            preview = await asyncio.to_thread(
                previews.create, pipeline, baseline, keys,
                getattr(user, 'username', None) if user else None,
            )
        except previews.PreviewError as error:
            raise ApiError(dict(code=400, message=str(error)))
        previews.start(pipeline, preview['id'])
        return cls(preview, user, **kwargs)

    @classmethod
    async def collection(cls, query, meta, user, **kwargs):
        from mage_ai.orchestration import previews

        pipeline = _query_pipeline(query, user)
        rows = await asyncio.to_thread(previews.listed, pipeline)
        return cls.build_result_set(rows, user, **kwargs)

    @classmethod
    async def member(cls, pk, user, **kwargs) -> 'ChangePreviewResource':
        from mage_ai.orchestration import previews

        pipeline = _query_pipeline(kwargs.get('query'), user)
        try:
            preview = await asyncio.to_thread(previews.read, pipeline, str(pk))
        except previews.PreviewError as error:
            raise ApiError(dict(code=404, message=str(error)))
        return cls(preview, user, **kwargs)
