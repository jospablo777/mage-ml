# Overview

*Programmatically create multiple pipeline runs to backfill a pipeline.*

### Backfill object

**Response**

```json
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
```

**`block_uuid`** (string)

If the backfill uses custom code to generate the pipeline runs, this block UUID contains the code
that will be used.

**`completed_at`** (datetime)

Date and time the backfill finished running successfully.

**`created_at`** (datetime, required)

Date and time the record was created.

**`end_datetime`** (datetime)

The date and time the backfill should stop on.

**`failed_at`** (datetime)

Date and time the backfill failed.

**`id`** (integer, required)

Unique ID for the record.

**`interval_type`** (string)

`minute`, `hour`, `day`, `week`, `month`, `year`

**`interval_units`** (integer)

The number of `interval_type` to use in between each pipeline run that the backfill creates.

For example, if `interval_type` is `day` and `interval_units` is 3, then the backfill will create
pipeline runs spanning 3 day increments between the `start_datetime` and `end_datetime`.

**`metrics`** (object)

Various metrics related to the pipeline run; e.g. number of rows processed in a
data integration pipeline sync.

**`name`** (string, required)

Name of the record.

**`pipeline_schedule_id`** (integer)

Pipeline schedule that this record belongs to.

**`pipeline_uuid`** (string, required)

Pipeline that this record belongs to.

**`start_datetime`** (datetime)

The date and time the backfill should start on.

**`started_at`** (datetime)

Date and time the backfill started running.

**`status`** (string, required)

`initial`, `running`, `completed`, `failed`, `cancelled`

**`total_run_count`** (integer)

Number of pipeline runs that will be created for this backfill.

**`updated_at`** (datetime, required)

Date and time the record was last updated.

**`variables`** (object)

Object containing variables for the pipeline that is being overwritten by this backfill.

**`[key]`** (string)

The property name is user defined.
