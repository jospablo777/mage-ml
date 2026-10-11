# MageML tips & tricks

*Tips for working with MageML pipelines. The videos were recorded with upstream Mage.*

<details><summary>Version Control</summary>

MageML stores a version of a file each time you save it, and you can revert to an earlier version.
Read the [file browser docs](developer-ux/file-browser.md) or see the video below for a tutorial.

[YouTube video player](https://www.youtube.com/embed/bz8xLeoKzFo)

</details>

<details><summary>DuckDB integration</summary>

MageML integrates with DuckDB. Watch the video below to learn more or read the [DuckDB docs](../integrations/databases/DuckDB.md).

[YouTube video player](https://www.youtube.com/embed/FmATfXoWudM)

</details>

<details><summary>Stream data to any destination</summary>

Streaming pipelines can write to any of the streaming destinations. Watch the video below to learn how to configure a streaming pipeline.

See the [streaming docs](streaming/introduction.md) for more info.

[YouTube video player](https://www.youtube.com/embed/9STvcYIBXdc)

</details>

<details><summary>Use Conditional Blocks</summary>

Watch this video to learn how to use conditional blocks to add conditional execution and branching logic to a pipeline.

See the [conditional block docs](../design/blocks/conditionals.md) for more info.

[YouTube video player](https://www.youtube.com/embed/D05C7atfCRQ)

</details>

<details><summary>Replicate Blocks</summary>

Watch this video to learn how to use replica blocks to reuse a block's code in more than one place in a pipeline.

See the [replicate block docs](blocks/replicate-blocks.md) for more info.

[YouTube video player](https://www.youtube.com/embed/M_bBCSTNDw4)

</details>

<details><summary>Use Split Pane View</summary>

The MageML pipeline editor can be used in a single, notebook-style interface _or_ in a split-pane view, with block output displayed on the right and blocks themselves on the left.

Split pane view can be enabled by selecting the "split" icon in the pipeline editor, as shown below:

<img src="https://mage-ai.github.io/assets/split-pane-view.png" />

The video below shows a demo.

[YouTube video player](https://www.youtube.com/embed/x4VLyIG6fCc)

</details>

<details><summary>Group & Filter Pipelines</summary>

You can tag, group, and filter your pipelines from the pipelines page. Watch the video below to learn more.

Read more in the [pipeline tagging docs](../pipelines/pipeline-tagging.md).

[YouTube video player](https://www.youtube.com/embed/QG88AWdB-Mo)

</details>

<details><summary>New Tree View</summary>

The tree view of a pipeline:

- Right-click a block to run, rename, insert, or delete blocks.
- Click to add or remove connections, and insert new blocks between existing ones.
- Some block types are grouped, with their lineage shown.

[YouTube video player](https://www.youtube.com/embed/G0OjD38MnXE)

</details>

<details><summary>Dynamic Blocks</summary>

Dynamic blocks create tasks at run time, one for each item a block outputs. They are similar to Airflow's Dynamic Task Mapping.

Use them to run many small tasks in parallel and reduce their outputs afterwards.

Learn more in the [dynamic block docs](../design/blocks/dynamic-blocks.md).

[YouTube video player](https://www.youtube.com/embed/jQZb3lrAb8w)

</details>

<details><summary>API Triggers</summary>

You can trigger pipelines with API calls, for example from a Python request or a cURL command.

API triggers accept runtime variables, so the caller can pass values to the pipeline run.

Read more in the [API trigger docs](../orchestration/triggers/trigger-pipeline-api.md).

[YouTube video player](https://www.youtube.com/embed/cHNTyGW51j8)

</details>

<details><summary>Backfills</summary>

A backfill runs a pipeline for a date range, for example to fill in missing data or to add a new column to past data. Specify the start and end dates.

A backfill creates one pipeline run for each execution date in the range.

Read more in the [backfill docs](../orchestration/backfills/overview.md).

[YouTube video player](https://www.youtube.com/embed/V-wUccaafEo)

</details>

## Run asynchronous code in blocks

```python
import asyncio
from typing import Any, Dict

import aiohttp

@data_loader
def load_data_sync(*args, **kwargs) -> Dict[str, Any]:
    """
    Synchronously load data from a remote API endpoint using aiohttp and asyncio.
    Uses a configurable URL with a fallback to a working test endpoint.
    Includes error handling to gracefully manage connection issues.

    Returns:
        Dict[str, Any]: The JSON response from the API as a dictionary, or error info.
    """
    url: str = kwargs.get('url', 'https://jsonplaceholder.typicode.com/todos/1')

    async def fetch_data(url: str) -> Dict[str, Any]:
        # Create an aiohttp session and fetch the data
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                data: Dict[str, Any] = await response.json()
                return data

    # Check if there's already a running event loop
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # In a running event loop (e.g., the notebook kernel), run the coroutine in another thread
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            result: Dict[str, Any] = executor.submit(asyncio.run, fetch_data(url)).result()
    else:
        # If no running event loop, use asyncio.run
        result: Dict[str, Any] = asyncio.run(fetch_data(url))
    return result
```
