# Update schedule

**`id`** (integer, required)

Pipeline schedule ID

<b>Body</b>
<br />
<br />

**`pipeline_schedule`** (object, required)

**`event_matchers`** (array of objects)

Event matchers of an event trigger. The list replaces the trigger's current matchers.

**`name`** (string)

Name of the trigger. It cannot be empty.

**`schedule_interval`** (string)

Required when `schedule_type` is `time`.

**`schedule_type`** (string)

Trigger type: `api`, `event` or `time`.

**`settings`** (object)

Additional settings for the trigger.

**`timeout`** (integer)

Optional run timeout in seconds. Use `null` or omit this field for no timeout.

**`sla`** (integer)

Optional SLA threshold in seconds. Omit this field to keep the current SLA.

**`start_time`** (datetime)

Persisted schedule start timestamp. For `@once`, this value determines the single run time;
for recurring time schedules, it anchors when the schedule begins.

**`status`** (string)

**`tags`** (array of strings)

Tag names to associate with the trigger.

**`variables`** (object)

```curl
curl --request PUT \
  --url 'http://localhost:6789/api/pipeline_schedules/55?api_key=zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2' \
  --header 'OAUTH-TOKEN: some_really_long_string' \
  --data '{
    "pipeline_schedule": {
      "name": "black voice11111",
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
      }
    }
  }'
```

```json
{
  "pipeline_schedule": {
    "id": 55,
    "created_at": "2023-03-08 04:52:54.268096+00:00",
    "updated_at": "2023-04-01 00:52:11.753497+00:00",
    "name": "black voice11111",
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
    "event_matchers": []
  }
}
```
