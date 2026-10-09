import asyncio
import time
from typing import Dict, List

from mage_ai.cache.block import BlockCache
from mage_ai.command_center.blocks.utils import add_application_actions, build_and_score
from mage_ai.command_center.factory import BaseFactory
from mage_ai.shared.hash import merge_dict


class BlockFactory(BaseFactory):
    async def fetch_items(self, **kwargs) -> List[Dict]:
        items = []

        if self.search:
            now = time.time()
            cache = await BlockCache.initialize_cache()
            mapping = cache.get(cache.cache_key)
            print(f'[BlockFactory] Load: {len(mapping)} - {time.time() - now}')

            data_array = mapping.items()
            now = time.time()
            await asyncio.gather(
                *[build_and_score(self, data, items) for data in data_array]
            )
            print(
                f'[BlockFactory] Search {self.search}: '
                f'{len(items)} - {time.time() - now}',
            )

            now = time.time()
            items = await self.rank_items(items)
            items = [merge_dict(
                item_dict,
                add_application_actions(item_dict),
            ) for item_dict in items]
            print(f'[BlockFactory] Rank items: {time.time() - now}')

        return items
