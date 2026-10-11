# Delete block

`DELETE /api/pipelines/:pipeline_uuid/blocks/:block_uuid`

Delete a block from a pipeline. This removes the block, and its replicas in the pipeline,
from the pipeline and keeps the block file in the project. Markdown and scratchpad block
files are deleted when no other pipeline uses them.

By default, blocks with downstream dependencies may not be deleted. To force delete blocks, set the `force` query parameter to the boolean `True`.

The following example deletes a fictional block `winter_wildflower` from the `broken_wave` pipeline.

**Request**

```bash
curl 'http://localhost:6789/api/pipelines/broken_wave/blocks/winter_wildflower?block_type=data_loader&api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2&force=True' \
  -X 'DELETE'
```

**Response**

```json
{
    "block": {
        "all_upstream_blocks_executed": true,
        "color": null,
        "configuration": {},
        "downstream_blocks": [],
        "executor_config": null,
        "executor_type": "local_python",
        "has_callback": false,
        "name": "winter wildflower",
        "language": "python",
        "retry_config": null,
        "status": "not_executed",
        "type": "data_loader",
        "upstream_blocks": [],
        "uuid": "winter_wildflower"
    }
}
```
