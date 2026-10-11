# Overview

*Status and metrics for an individual run of a pipeline.*

### Pipeline run object

**Response**

```json
{
  "backfill_id": null,
  "block_runs": [
    {
      "block_uuid": "load_titanic",
      "completed_at": "2023-02-16 06:40:49.769087+00:00",
      "created_at": "2023-02-16 06:40:43.575738+00:00",
      "id": 33,
      "metrics": null,
      "pipeline_run_id": 23,
      "started_at": "2023-02-16 06:40:48.816197+00:00",
      "status": "completed",
      "updated_at": "2023-02-16 06:40:48.873526+00:00"
    }
  ],
  "block_runs_count": 4,
  "completed_at": "2023-02-16 06:41:19.249956+00:00",
  "created_at": "2023-02-16 06:40:38.681910+00:00",
  "event_variables": null,
  "execution_date": "2023-02-16 04:41:03.300124+00:00",
  "id": 23,
  "metrics": null,
  "passed_sla": false,
  "pipeline_schedule_id": 1,
  "pipeline_schedule_name": "late forest",
  "pipeline_schedule_token": "260385a38d62466fb1c88fa8c5ca503f",
  "pipeline_schedule_type": "time",
  "pipeline_uuid": "example_pipeline",
  "status": "completed",
  "updated_at": "2023-02-16 06:41:09.154102+00:00",
  "variables": null
}
```

**`backfill_id`** (integer)

Backfill that this run belongs to.

**`block_runs`** (array of objects)

The block runs for this specific pipeline run.

**`block_uuid`** (string, required)

Block UUID this block run belongs to.

**`completed_at`** (datetime)

Date and time the block run finished running successfully.

**`created_at`** (datetime, required)

Date and time the block run was created.

**`id`** (integer, required)

ID of block run.

**`metrics`** (object)

Various metrics related to the block run; e.g. error messages.

**`pipeline_run_id`** (integer, required)

Pipeline run ID that this block run belongs to.

**`started_at`** (datetime)

Date and time the block run started running.

**`status`** (string, required)

`initial`, `queued`, `running`, `completed`, `failed`, `cancelled`, `upstream_failed`, `condition_failed`

**`updated_at`** (datetime, required)

Date and time the block run record was most recently updated.

**`block_runs_count`** (integer, required)

Number of block runs for this pipeline run.

**`completed_at`** (datetime)

Date and time the pipeline run finished running successfully.

**`created_at`** (datetime, required)

Date and time the pipeline run was created.

**`event_variables`** (object)

When pipeline run is triggered from an API request, the API request payload is saved in the
pipeline run’s `event_variables` column.

**`execution_date`** (datetime)

Date and time the pipeline run started running.

**`id`** (integer, required)

ID of pipeline run.

**`metrics`** (object)

Various metrics related to the pipeline run; e.g. number of rows processed in a
data integration pipeline sync.

**`passed_sla`** (boolean)

Whether or not the pipeline run has passed the configured SLA.

**`pipeline_schedule_id`** (integer, required)

Pipeline schedule ID that this pipeline run belongs to.

**`pipeline_schedule_name`** (string, required)

Name of the pipeline schedule this pipeline run belongs to.

**`pipeline_schedule_token`** (string, required)

Unique token of the pipeline schedule that this pipeline run belongs to.

**`pipeline_schedule_type`** (string, required)

Pipeline schedule type that this pipeline run belongs to.

**`pipeline_uuid`** (string, required)

Pipeline UUID that this pipeline run belongs to.

**`status`** (string, required)

`initial`, `running`, `completed`, `failed`, `cancelled`

**`updated_at`** (datetime, required)

Date and time the pipeline run record was most recently updated.

**`variables`** (object)

Object containing variables for the pipeline that is being overwritten by this pipeline run.

**`[key]`** (string)

The property name is user defined.
