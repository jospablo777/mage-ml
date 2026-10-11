# Update pipeline

`PUT /api/pipelines/:uuid`

All default attributes for a pipeline may be updated in addition to the following:

**`add_upstream_for_block_uuid`** (string)

The uuid for a block upstream of the pipeline.

**`blocks`** (array of objects)

Array of block objects. See the [blocks](../blocks/overview.md) section for more details.

**`callbacks`** (array of objects)

Array of [callback](../../../design/blocks/callbacks.md) block objects. Same shape as `blocks`.

**`conditionals`** (array of objects)

Array of [conditional](../../../design/blocks/conditionals.md) block objects. Same shape as `blocks`.

**`extensions`** (array of objects)

Array of [extension](../../../observability/testing/data-validation.md) block objects. Same shape as `blocks`.

**`schedules`** (array of objects)

Array of [schedule](../../../orchestration/triggers/triggering-pipelines.md) trigger objects.

**`tags`** (array of strings)

Tag names to associate with the pipeline.

**Request**

```bash
curl --request PUT \
  --url http://localhost:6789/api/pipelines/arwen_starlight \
  -H 'Content-Type: application/json' \
  -H 'Cookie: oauth_token=some_really_long_string' \
  -H 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  -d '{"pipeline": {"type": "integration"}}'
```

**Response**

```json
{
  "pipeline": {
    "data_integration": null,
    "description": null,
    "executor_config": {},
    "executor_count": 1,
    "executor_type": null,
    "name": "arwen-starlight",
    "notification_config": {},
    "type": "integration",
    "updated_at": null,
    "uuid": "arwen_starlight",
    "spark_config": {},
    "blocks": [],
    "callbacks": [],
    "conditionals": [],
    "widgets": [],
    "extensions": {}
  }
}
```
