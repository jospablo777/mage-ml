# Read pipeline runs

`GET /api/pipeline_schedules/:pipeline_schedule_id/pipeline_runs`

**`pipeline_schedule_id`** (integer, required)

Pipeline schedule ID that pipeline runs should all belong to.

**`_limit`** (integer)

Maximum number of pipeline runs to be returned in the API response.

Example: `20`

**`_offset`** (integer)

Skip the first N pipeline runs, where N equals `_offset`.

Example: `10`

**Request**

```curl
curl --request GET \
  --url 'http://localhost:6789/api/pipeline_schedules/1/pipeline_runs?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  --header 'OAUTH-TOKEN: some_really_long_string'
```

**Response**

```json
{
  "pipeline_runs": [
    ...
  ],
  "metadata": {
    "count": 22,
    "next": false
  }
}
```
