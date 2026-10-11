# Read pipelines

To retrieve all pipelines:

`GET /api/pipelines`

To fetch a single pipeline:

`GET /api/pipelines/:uuid`

**`include_schedules`** (integer)

If `1`, the pipeline object will include associated triggers.

**Request**

```bash
curl --request GET \
  --url 'http://localhost:6789/api/pipelines/arwen_starlight?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  --header 'OAUTH-TOKEN: some_really_long_string'
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
    "widgets": [],
    "extensions": {}
  }
}
```
