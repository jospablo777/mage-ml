# Read backfills

`GET /api/backfills`

**`_limit`** (integer)

Maximum number of backfills to be returned in the API response.

Example: `20`

**`_offset`** (integer)

Skip the first N backfills, where N equals `_offset`.

Example: `10`

**`pipeline_uuid`** (string)

The pipeline uuid for which to read backfills.

**`include_run_count`** (boolean)

A boolean which indicates whether to return the run count in the response metadata object.

**Request**

```curl
curl --request GET \
  --url 'http://localhost:6789/api/backfills?_limit=20&_offset=0&include_run_count=true&pipeline_uuid=example_pipeline&api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  --header 'OAUTH-TOKEN: some_really_long_string'
```

**Response**

```json
{
  "backfills": [
    {
      "block_uuid": null,
      "completed_at": null,
      "created_at": "2023-03-23 18:27:58.848959+00:00",
      "end_datetime": "2023-03-05 00:00:00+00:00",
      "failed_at": null,
      "id": 9,
      "interval_type": "hour",
      "interval_units": 4,
      "metrics": null,
      "name": "sparkling glitter",
      "pipeline_schedule_id": null,
      "pipeline_uuid": "example_pipeline",
      "start_datetime": "2023-03-01 00:00:00+00:00",
      "started_at": null,
      "status": null,
      "total_run_count": 25,
      "updated_at": "2023-03-23 18:28:22.379725+00:00",
      "variables": null
    }
  ],
  "metadata": {
    "count": 0,
    "next": false
  }
}
```
