# Create backfill

`POST /api/pipelines/:pipeline_uuid/backfills`

**`pipeline_uuid`** (string, required)

Pipeline UUID to create the backfill for.

<b>Body</b>
<br />
<br />

**`backfill`** (object, required)

**`name`** (string, required)

**Request**

```curl
curl --request POST \
  --url 'http://localhost:6789/api/pipelines/example_pipeline/backfills' \
  --header 'OAUTH-TOKEN: some_really_long_string' \
  --header 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  --data '{
    "backfill": {
      "name": "fire wind"
    }
  }'
```

**Response**

```json
{
  "backfill": {
    "id": 10,
    "block_uuid": null,
    "completed_at": null,
    "created_at": "2023-04-01 01:02:18.411997+00:00",
    "end_datetime": null,
    "failed_at": null,
    "interval_type": null,
    "interval_units": null,
    "metrics": null,
    "name": "fire wind",
    "pipeline_schedule_id": null,
    "pipeline_uuid": "example_pipeline",
    "start_datetime": null,
    "started_at": null,
    "status": null,
    "updated_at": "2023-04-01 01:02:18.411997+00:00",
    "variables": null
  }
}
```
