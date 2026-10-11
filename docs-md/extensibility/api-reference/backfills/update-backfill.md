# Update backfill

`PUT /api/pipelines/:pipeline_uuid/backfills/:backfill_id`

All default attributes for a backfill may be updated in addition to the following:

**`start_datetime`** (string)

A datetime formatted string representing the start time of the backfill, e.g. `"2023-09-05 23:56:05"`.

**`end_datetime`** (string)

A datetime formatted string representing the end time of the backfill, e.g. `"2023-09-05 23:56:05"`.

**`interval_type`** (string)

The interval type for the backfill. One of: `minute`, `hour`, `day`, `week`, `month`, or `year`.

**`interval_units`** (integer)

The number of interval units for the backfill.

**`name`** (string)

A new name for the backfill.

**`status`** (string)

The backfill status. One of `initial`, `running`, `completed`, `failed`, `cancelled`.

**`variables`** (object)

An object containing Global Pipeline Variables to be overridden by the backfill. Any Global Variable may be overridden.

![Global Pipeline Vars](https://mage-ai.github.io/assets/pipeline-variables.png)

**Request**

```curl
curl -X PUT \
--url http://localhost:6789/api/pipelines/arwen_starlight/backfills/6 \
-H 'Content-Type: application/json' \
-H 'Cookie: oauth_token=some_really_long_string' \
-H 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
-d '{
  "backfill": {
    "start_datetime": "2023-07-17 23:56:05",
    "end_datetime": "2023-07-18 23:56:05",
    "name": "new_name"
  }
}' 

```
```python
import requests

headers = {
  "Content-Type": "application/json",
  "X-API-KEY": "zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2",
  "Cookie": "oauth_token=some_really_long_string"
}  
json_data = {
  'backfill': {
          "start_datetime": '2023-07-17 23:56:05',
          "end_datetime": '2023-07-18 23:56:05',
          'name':'new_name'
          }
}

r = requests.put('http://localhost:6789/api/pipelines/arwen_starlight/backfills/6', json=json_data, headers=headers)
```

**Response**

```json
{
  "backfill": {
    "id": 6,
    "block_uuid": null,
    "completed_at": null,
    "created_at": "2023-07-18 23:30:26",
    "end_datetime": "2023-07-18 23:56:05",
    "failed_at": null,
    "interval_type": null,
    "interval_units": null,
    "metrics": null,
    "name": "new_name",
    "pipeline_schedule_id": null,
    "pipeline_uuid": "arwen_starlight",
    "start_datetime": "2023-07-17 23:56:05",
    "started_at": null,
    "status": null,
    "updated_at": "2023-07-18 23:31:57",
    "variables": null
  }
}
```
