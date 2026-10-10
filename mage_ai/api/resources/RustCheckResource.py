import asyncio
import os
import time
from typing import Dict

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.orchestration.db import safe_db_query
from mage_ai.settings.repo import get_repo_path

MAX_CONTENT_BYTES = 1_000_000


class RustCheckResource(GenericResource):
    """
    The compiler's errors and warnings for a Rust block's code, for the editor: POST
    /api/rust_checks with the pipeline, the block and the editor's content. Runs
    `cargo check`, which compiles nothing to run, in a worker thread.
    """

    @classmethod
    @safe_db_query
    async def create(cls, payload: Dict, user, **kwargs) -> 'RustCheckResource':
        from mage_ai.data_preparation.models.block.rust import build as rust_build
        from mage_ai.data_preparation.models.block.rust.source import RustSourceError
        from mage_ai.data_preparation.models.constants import BlockLanguage
        from mage_ai.data_preparation.models.pipeline import Pipeline

        pipeline_uuid = payload.get('pipeline_uuid')
        block_uuid = payload.get('block_uuid')
        content = payload.get('content')
        if not isinstance(content, str) or len(content.encode('utf-8')) > MAX_CONTENT_BYTES:
            raise ApiError(dict(code=400, message='The content must be text up to 1 MB.'))
        repo_path = get_repo_path(user=user)
        pipeline = Pipeline.get(pipeline_uuid, repo_path=repo_path, check_if_exists=True)
        block = pipeline.get_block(block_uuid) if pipeline else None
        if block is None or block.language != BlockLanguage.RUST:
            raise ApiError(dict(code=404, message='The pipeline has no such Rust block.'))

        label = os.path.relpath(block.file_path, block.repo_path or repo_path)
        started = time.monotonic()
        try:
            prepared = rust_build.prepare(
                content, block.type, block.uuid, block.repo_path or repo_path, label=label,
            )
            diagnostics = await asyncio.to_thread(rust_build.check, prepared)
            items = [diagnostic.to_dict() for diagnostic in diagnostics]
        except RustSourceError as error:
            items = [dict(level='error', line=1, column=1, message=str(error), rendered=str(error))]
        except rust_build.RustBuildError as error:
            items = [dict(level='error', line=None, column=None, message=str(error),
                          rendered=str(error))]
        return cls(dict(
            block_uuid=block.uuid,
            diagnostics=items,
            pipeline_uuid=pipeline_uuid,
            seconds=round(time.monotonic() - started, 3),
        ), user, **kwargs)
