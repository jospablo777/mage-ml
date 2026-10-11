# Schedules

*Schedule pipelines to run periodically*

<img src="https://media.tenor.com/lyU9j2EckEUAAAAd/running-out-of-time-no-time.gif" alt="Schedule pipelines to run periodically" />

You can schedule your pipeline to run on these intervals:

- Once:  The pipeline will run only once
- Hourly: The pipeline will run at the beginning of every hour (i.e 0100, 0200, 0300, etc)
- Daily: The pipeline will run at 0000 UTC / GMT every day
- Weekly: The pipeline will run at 0000 UTC / GMT the first day (Monday) of every week
- Monthly: The pipeline will run at 0000 UTC / GMT on the first day of every month
- Always on: The pipeline will be run as soon as the previous pipeline run is finished
- Custom schedule using
  [CRON Expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)
- Every minute using CRON expression: `* * * * *`
- _Streaming pipeline_: a trigger starts it once, and it keeps consuming until it is stopped

Learn more about how to
[schedule pipelines](../../design/data-pipeline-management.md#create-trigger).

## Extra runtime variables from pipeline run

Default runtime variables from pipeline run:

| Key | Description | Example |
| --- | --- | --- |
| `ds` | The execution date as `YYYY-MM-DD`. | `2023-12-25` |
| `env` | The value is always `prod` in pipeline runs. The value is `dev` when running blocks in notebook. | `prod` |
| `event` | If the pipeline is triggered by event, the event variable contains the event payload. | `{"key1": "value1"}` |
| `execution_date` | A datetime object that the pipeline is executed at. | `datetime.datetime(2023, 12, 25, 12, 25, 20)` |
| `hr` | The hour of the execution date, as two digits. | `12` |
| `execution_partition` | An automatically formatted partition of the pipeline run using the execution date. | `10/20231225T122520` |
| `pipeline_run_id` | The id of the pipeline run. | `10` |
| `trigger_name` | The trigger name of the pipeline run. | `test run 1` |

If your pipeline run belongs to a trigger that is scheduled, then the following extra
variables are available in your Python block’s keyword arguments (e.g. `kwargs`).

| Key | Description | Example |
| --- | --- | --- |
| `interval_start_datetime` | The `datetime` when the pipeline run is scheduled for. | `datetime.datetime(2023, 7, 23, 7, 0, 0, 0)` |
| `interval_end_datetime` | The `datetime` when the next pipeline run is scheduled for. | `datetime.datetime(2023, 7, 24, 7, 0, 0, 0)` |
| `interval_seconds` | The number of seconds between the current pipeline run and the next pipeline run. | `86400` |
| `interval_start_datetime_previous` | The `datetime` when the previous pipeline run was scheduled for. | `datetime.datetime(2023, 7, 22, 7, 0, 0, 0)` |

### SQL block

If you’re using a SQL block, here is an example of how you can access these variables:

```sql
SELECT
    '{{ interval_start_datetime }}' AS interval_start_datetime
    , '{{ interval_end_datetime }}' AS interval_end_datetime
    , '{{ interval_seconds }}' AS interval_seconds
    , '{{ interval_start_datetime_previous }}' AS interval_start_datetime_previous
```
