# Create pipeline schedule

`POST /api/pipelines/:pipeline_uuid/pipeline_schedules`

**`pipeline_uuid`** (string, required)

Pipeline UUID that the pipeline schedule should all belong to.

**`pipeline_schedule`** (object, required)

**`name`** (string, required)

Name of the trigger. It cannot be empty.

**`description`** (string)

**`schedule_interval`** (string)

Required when `schedule_type` is `time`.

**`schedule_type`** (string)

Trigger type: `api`, `event` or `time`.

**`settings`** (object)

Additional settings for the trigger.

**`timeout`** (integer)

Optional run timeout in seconds. Use `null` or omit this field for no timeout.

**`sla`** (integer)

Optional SLA threshold in seconds. Use `null` or omit this field for no SLA.

**`start_time`** (datetime)

Persisted schedule start timestamp. For `@once`, this value determines the single run time;
for recurring time schedules, it anchors when the schedule begins.

**`status`** (string)

**`tags`** (array of strings)

Tag names to associate with the trigger.

**`variables`** (object)

> [!NOTE]
> Use `time` for Schedule triggers, `api` for API triggers and `event` for event triggers.

**Request**

```bash
  curl --request POST \
  -H 'Content-Type: application/json' \
  -H 'Cookie: oauth_token=some_really_long_string' \
  -H 'X-API-KEY: zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  -d '{
    "pipeline_schedule": {
      "name": "Example Pipeline Schedule Name",
      "schedule_type": "time",
      "schedule_interval": "@once",
      "start_time": "2023-03-06 04:53:00",
      "variables": {
        "env": "dev112222",
        "test": 11111
      },
      "sla": 1000,
      "settings": {
        "allow_blocks_to_fail": true,
        "create_initial_pipeline_run": false,
        "skip_if_previous_running": true,
        "timeout": 3600
      },
      "tags": ["core data"]
    }
  }' \
  --url http://localhost:6789/api/pipelines/example_pipeline/pipeline_schedules
```

**Response**

```json
{
  "pipeline_schedule": {
    "id": 55,
    "created_at": "2023-03-08 04:52:54.268096+00:00",
    "updated_at": "2023-04-01 00:52:11.753497+00:00",
    "name": "Example Pipeline Schedule Name",
    "next_pipeline_run_date": null,
    "pipeline_uuid": "example_pipeline",
    "schedule_type": "time",
    "start_time": "2023-03-06 04:53:00+00:00",
    "schedule_interval": "@once",
    "status": "inactive",
    "variables": {
      "env": "dev112222",
      "test": 11111
    },
    "sla": 1000,
    "token": "67d62ed3e66143839f58945bb7d16387",
    "settings": {
      "allow_blocks_to_fail": true,
      "create_initial_pipeline_run": false,
      "skip_if_previous_running": true,
      "timeout": 3600
    },
    "tags": ["core data"]
  }
}
```
