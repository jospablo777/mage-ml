# Overview

*Triggers for a pipeline.*

### Pipeline schedule object

**Response**

```json
{
  "created_at": "2023-03-01 20:27:23.645692+00:00",
  "event_matchers": [],
  "id": 41,
  "last_pipeline_run_status": null,
  "name": "dark brook",
  "next_pipeline_run_date": null,
  "pipeline_runs_count": 0,
  "pipeline_uuid": "dry_bird",
  "schedule_interval": "@once",
  "schedule_type": "time",
  "settings": {
    "allow_blocks_to_fail": true,
    "create_initial_pipeline_run": true,
    "skip_if_previous_running": false,
    "timeout": 3600
  },
  "sla": 950400,
  "start_time": "2023-03-01 22:04:00+00:00",
  "status": "inactive",
  "tags": ["core data"],
  "token": "1dca215c01564d48bf0d3db7d5450a20",
  "updated_at": "2023-03-01 22:04:29.761238+00:00",
  "variables": {
    "env": "dev"
  }
}
```

**`created_at`** (string, required)

Date and time the trigger was created.

**`event_matchers`** (array of objects)

Event matchers of an event trigger (`schedule_type` `event`).

**`event_type`** (string, required)

`aws_event`

**`id`** (string, required)

Event matcher ID.

**`name`** (string, required)

Name of event matcher.

**`pattern`** (string)

**`pipeline_schedule_ids`** (array of integers)

**`id`** (integer, required)

Trigger ID.

**`last_pipeline_run_status`** (string)

**`name`** (string, required)

Name of trigger. It cannot be empty.

**`next_pipeline_run_date`** (datetime)

Next scheduled execution date for the trigger, or `null` when no next run is scheduled.

**`pipeline_runs_count`** (integer)

**`pipeline_uuid`** (string, required)

Pipeline UUID the trigger belongs to.

**`schedule_interval`** (string)

`@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@always_on`, or a cron expression

**`schedule_type`** (string, required)

Trigger type: `api`, `event` or `time`.

**`settings`** (object)

Additional settings for the trigger.

**`allow_blocks_to_fail`** (boolean)

If `true`, then blocks in the pipeline will continue to run even if unrelated blocks fail.

**`create_initial_pipeline_run`** (boolean)

If `true`, then an initial pipeline run will be created immediately after enabling the
trigger if the trigger's current execution date is after its start date time but before
current time.

For example, if the current time is `1/4/24 12:00 UTC` and you create a daily trigger, the
current execution date would be `2024-01-04 00:00`. If the start date was set to
`1/3/24 12:00 UTC`, which is before the current execution date, and the `create_initial_pipeline_run`
setting was enabled, a pipeline run with execution date `2024-01-04 00:00` would get created
immediately.

**`skip_if_previous_running`** (boolean)

If `true`, then new pipeline runs won’t be triggered until the most recent pipeline run is
no longer running.

**`timeout`** (integer)

Optional run timeout in seconds. A `null` or omitted value means no timeout.

**`sla`** (integer)

Optional SLA threshold in seconds. If the pipeline run has been running for more than the SLA
value, then the pipeline will fail.

**`start_time`** (datetime)

Date and time when the trigger should start running.

**`status`** (string, required)

`active`, `inactive`

**`tags`** (array)

An array of tag names.

**`token`** (string)

Unique string used in the URL when triggering pipeline via API request.

**`updated_at`** (datetime, required)

Date and time the trigger was last updated.

**`variables`** (object)

Object containing variables for the pipeline that is being overwritten by this trigger.

**`[key]`** (string)

The property name is user defined.
