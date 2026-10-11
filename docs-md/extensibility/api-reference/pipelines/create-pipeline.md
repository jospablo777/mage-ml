# Create pipeline

`POST /api/pipelines`

**`name`** (string, required)

Human readable name of the pipeline.

**`type`** (string, required)

The type of the pipeline:
`integration`, `pyspark`, `python`, `streaming`
Note that `python` is a standard (batch) pipeline with a python backend, while `pyspark` is a batch pipeline with a spark backend.

**`tags`** (array of strings)

Tag names to associate with the pipeline.

**`clone_pipeline_uuid`** (string)

If supplied, the uuid of a pipeline to clone.

**`extensions`** (array of objects)

Array of [extension](../../../observability/testing/data-validation.md) block objects. Same shape as `blocks`.

**`callbacks`** (array of objects)

Array of [callback](../../../design/blocks/callbacks.md) block objects.  Same shape as `blocks`.

**`conditionals`** (array of objects)

Array of [conditional](../../../design/blocks/conditionals.md) block objects.  Same shape as `blocks`.

**Request**

```bash
  curl --request POST \
  -H 'Content-Type: application/json' \
  -H 'Cookie: oauth_token=some_really_long_string' \
  -H 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  -d '{"pipeline": {"name": "arwen-starlight", "type": "python"}}' \
  --url http://localhost:6789/api/pipelines
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
    "type": "python",
    "updated_at": null,
    "uuid": "arwen_starlight",
    "spark_config": {},
    "blocks": [],
    "callbacks": [],
    "conditionals": [],
    "widgets": []
  }
}
```
