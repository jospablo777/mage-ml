"""
Pages of GET /api/block_runs when block runs share their times, as the block runs one run
creates do: each row is on exactly one page, in the same order on every request.
"""
from datetime import datetime, timezone

from mage_ai.api.operations.base import BaseOperation
from mage_ai.api.operations.constants import (
    META_KEY_LIMIT,
    META_KEY_OFFSET,
    OperationType,
)
from mage_ai.orchestration.db.models.schedules import BlockRun
from mage_ai.tests.base_test import AsyncDBTestCase
from mage_ai.tests.factory import (
    create_pipeline_run_with_schedule,
    create_pipeline_with_blocks,
    create_user,
)

AT = datetime(2026, 10, 22, 2, 15, 8, tzinfo=timezone.utc)


class BlockRunPagesTest(AsyncDBTestCase):
    async def page(self, offset, **query):
        response = await BaseOperation(
            action=OperationType.LIST,
            resource='block_runs',
            user=create_user(_owner=True),
            query={k: [v] for k, v in query.items()},
            meta={META_KEY_LIMIT: 2, META_KEY_OFFSET: offset},
        ).execute()
        return [row['id'] for row in response['block_runs']]

    async def test_block_runs_with_equal_times_page_by_their_ids(self):
        create_pipeline_with_blocks('block_run_pages', self.repo_path)
        run = create_pipeline_run_with_schedule(pipeline_uuid='block_run_pages')
        # Only the block runs below, which share every time.
        BlockRun.query.filter(BlockRun.pipeline_run_id == run.id).delete()
        ids = [
            BlockRun.create(
                block_uuid=f'block_{i}', pipeline_run_id=run.id, created_at=AT, started_at=AT,
                completed_at=AT,
            ).id
            for i in range(5)
        ]
        query = dict(pipeline_uuid='block_run_pages')

        pages = [await self.page(offset, **query) for offset in (0, 2, 4)]
        self.assertEqual(sum(pages, []), sorted(ids, reverse=True))

        ascending = [await self.page(o, order_by='created_at asc', **query) for o in (0, 2, 4)]
        self.assertEqual(sum(ascending, []), sorted(ids))
