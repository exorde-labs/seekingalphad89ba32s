from seekingalphad89ba32s import query
from exorde_data.models import Item
import pytest


@pytest.mark.asyncio
async def test_query():
    params = {
        "max_oldness_seconds": 12000,
        "maximum_items_to_collect": 2,
        "min_post_length": 10
    }
    async for item in query(params):
        assert isinstance(item, Item)

import asyncio
asyncio.run(test_query())