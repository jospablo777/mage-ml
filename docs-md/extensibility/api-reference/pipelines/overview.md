# Overview

*Data pipeline containing execution settings, resources, block information, and*

## Pipeline object

**Response**

```json
{
  "blocks": [
    {
      "all_upstream_blocks_executed": true,
      "color": null,
      "configuration": {},
      "downstream_blocks": [],
      "executor_config": null,
      "executor_type": "local_python",
      "has_callback": null,
      "name": "load_titanic",
      "language": "python",
      "status": "executed",
      "type": "data_loader",
      "upstream_blocks": [],
      "uuid": "load_titanic",
      "content": "...",
      "metadata": {}
    }
  ],
  "description": null,
  "extensions": {},
  "name": "example_pipeline",
  "schedules": [
    {
      "created_at": "2023-03-14T23:24:17.814478+00:00",
      "id": 59,
      "name": "dry haze",
      "pipeline_uuid": "aged_night",
      "schedule_interval": null,
      "schedule_type": "api",
      "start_time": "2023-03-14T23:25:00+00:00",
      "status": "inactive",
      "updated_at": "2023-03-14T23:25:27.351528+00:00"
    }
  ],
  "type": "python",
  "uuid": "example_pipeline",
  "variables": {
    "env": "prod"
  },
  "widgets": []
}
```

**`blocks`** (array of objects)

Array of block objects. See the [blocks](../blocks/overview.md) section for more details.

**`description`** (string)

Description for the pipeline.

**`extensions`** (array of objects)

Array of extension block objects. Same shape as `blocks`.

**`name`** (string, required)

Human friendly name of the pipeline.

**`schedules`** (array of objects, required)

Array of trigger objects.

**`created_at`** (string, required)

Date and time the trigger was created.

**`id`** (integer, required)

Trigger ID.

**`name`** (string, required)

Name of trigger.

**`pipeline_uuid`** (string, required)

Pipeline UUID the trigger belongs to.

**`schedule_interval`** (string)

`@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@always_on`, or a cron expression

**`schedule_type`** (string, required)

Trigger type: `api`, `event` or `time`.

**`start_time`** (datetime)

Date and time when the trigger should start running.

**`status`** (string, required)

`active`, `inactive`

**`updated_at`** (datetime, required)

Date and time the trigger was last updated.

**`type`** (string, required)

The type of the pipeline:
`integration`, `pyspark`, `python`, `streaming`
Note that `python` is a standard (batch) pipeline with a python backend, while `pyspark` is a batch pipeline with a spark backend.

**`uuid`** (string, required)

Unique identifier for the pipeline.

**`variables`** (object)

Object containing variables for the pipeline.

**`[key]`** (string)

The property name is user defined.

**`widgets`** (array of objects)

Array of widget block objects. Same shape as `blocks`.
