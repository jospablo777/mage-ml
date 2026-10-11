# Pipeline Trigger Overview

*Configure and manage pipeline triggers in MageML*

## Overview

Pipeline triggers are automated scheduling mechanisms that determine **when and how data pipelines should run** in MageML. A single pipeline can have multiple triggers, each with different configurations for various use cases like production runs, testing, or data backfills.

### Key Benefits of Pipeline Triggers:
- **Automated execution**: Run pipelines without manual intervention
- **Flexible scheduling**: Support for cron expressions and API calls
- **Environment management**: Different trigger configurations for dev, staging, and production
- **Resource optimization**: Control when pipelines consume compute resources
- **Monitoring and SLA**: Built-in performance tracking and alerting

> [!NOTE]
> **Pipeline Schedule** and **Trigger** are the same thing in MageML. You may see either term used throughout the product.

## Trigger Types

> [!NOTE]
> New trigger creation supports Schedule, API and after-run triggers. Event triggers are
> deprecated and may only appear for existing legacy configurations.

### Schedule triggers

Schedule triggers are the most common type, designed for **recurring data pipeline execution** at predetermined intervals. They suit ETL processes, data updates, and regular analytics jobs.

Available scheduling frequencies include:

- **Run exactly once**: Run one time when the schedule starts, then disable the trigger
- **Hourly**: Run at minute 0 of each hour in UTC
- **Daily**: Run once per day at 00:00 UTC
- **Weekly**: Run every Monday at 00:00 UTC
- **Monthly**: Run on the first day of each month at 00:00 UTC
- **Always on**: Start a new run as soon as the previous run completes
- **Custom (cron syntax)**: Use cron expressions for exact clock times and advanced scheduling patterns

> [!NOTE]
> **Custom frequency** allows you to use cron syntax for advanced scheduling patterns.

#### Common cron examples

Here are some common cron patterns you can use with custom frequency:

```bash
# Every 15 minutes
*/15 * * * *

# Every weekday at 9 AM
0 9 * * 1-5

# Every day at 2:30 PM
30 14 * * *

# Every Sunday at midnight
0 0 * * 0

# Every 1st of the month at 6 AM
0 6 1 * *

# Every 5 minutes during business hours (9 AM - 5 PM, weekdays)
*/5 9-17 * * 1-5
```

**Cron expression format:** `[minute] [hour] [day(month)] [month] [day(week)]`

> [!WARNING]
> **Timezone behavior:** If you have the `display_local_timezone` setting enabled, the local cron expression will match your local timezone only if the minute and hour values are single values without any special characters (comma, hyphen, or slash). You can still use cron expressions with special characters for the minute/hour values, but they will be based in UTC time.

> [!NOTE]
> Use online cron expression generators or cron documentation to create more complex scheduling patterns that fit your specific needs.

### API triggers

API triggers enable **on-demand pipeline execution** through HTTP requests. They suit webhook integrations, external system triggers, and manual pipeline execution from external applications. For more details, refer to the [API Trigger documentation](trigger-pipeline-api.md).

**Use cases for API triggers:**
- Webhook integrations with external services
- Manual pipeline execution from external applications
- Event-driven data processing
- Integration with CI/CD pipelines

You can make a POST request to an endpoint provided in the UI when creating or
editing a trigger. The endpoint depends on the MageML domain, the trigger's unique ID and its token.

**API endpoint:**
```bash
POST {MAGE_HOST}/api/pipeline_schedules/3/pipeline_runs/abc123
```

**Request payload:**
Runtime variables can optionally be included in the request payload and are accessible within each pipeline block.

```json
{
  "pipeline_run": {
    "variables": {
      "target": "staging",
      "schema": "public"
    }
  }
}
```

### After-run triggers

After-run triggers start a pipeline when a run of another pipeline ends: features, then
scoring; a load, then its checks. Choose **After another pipeline** as the trigger type,
then the pipeline to follow and which endings start the run: **completes** (the default),
**fails**, or **ends either way**. In `triggers.yaml`:

```yaml
triggers:
- name: after_load_orders
  schedule_type: after_run
  status: active
  settings:
    after_pipeline: load_orders
    after_status: completed   # completed, failed or any
```

- Each run of the pipeline it follows starts one run, when that run completes or fails.
  The run starts on the scheduler's next tick, within the trigger's and the pipeline's
  concurrency limits.
- The run gets `upstream_pipeline`, `upstream_pipeline_run_id` and `upstream_status` as
  variables, so its blocks know which run started it.
- `skip_if_previous_running` skips the run while the pipeline still has one going.
- MageML refuses a trigger that follows its own pipeline, a pipeline that does not exist,
  or one that would make pipelines start each other in a loop.
- [Exported pipeline services](../../production/pipeline-services.md#after-run-triggers) run the
  same triggers.

### Deprecated Event triggers

> [!WARNING]
> Event triggers are deprecated for new trigger creation. Use API triggers for new event-driven
> and webhook-style workflows. Existing legacy event triggers may still appear in older projects.

## How to Create and Manage Pipeline Triggers

### Creating a New Trigger

To create a new trigger for your pipeline:

1. Navigate to your pipeline in the MageML UI
2. Click the **Triggers** tab in the pipeline view
   - Direct URL: `/pipelines/[pipeline_uuid]/triggers`
3. Click the **New trigger** button
4. Select the trigger type (Schedule or API)
5. Configure the trigger settings based on the type you selected
6. Click **Create trigger** to create the trigger

> [!NOTE]
> **Trigger storage**: Triggers created via the UI are stored in the database. For environment synchronization and version control, you can [save triggers in code](configure-triggers-in-code.md) to sync them across different environments.

#### Trigger configuration fields

When creating or editing a trigger, you'll see the following configuration fields:

**Basic settings:**
- **Name**: A descriptive name for your trigger, unique within the pipeline (up to 255 characters).
- **Description**: Optional description to explain the trigger's purpose
- **Tags**: Add or search tags to organize and categorize your triggers.

**Schedule-specific fields:**
- **Frequency**: Choose from preset options (Hourly, Daily, Weekly, Monthly, Once, Always on) or select Custom for CRON syntax when you need more precise timing
- **Start date and time**: When the schedule starts. **Frequency** still controls when runs happen.
- **Landing time**: Instead of scheduling starts at a specific time, schedule runs so the pipeline can complete by a specified time. With landing time on, the start field becomes **Pipeline complete by**.

**Additional options:**
- **Runtime variables**: Override pipeline variables for this specific trigger
- **Run settings**: Configure timeouts, SLA, and failure handling
- **Advanced settings**: Additional trigger-specific configurations

### Viewing and Monitoring Triggers

#### Pipeline-Specific Trigger Management

To view all triggers for a specific pipeline:

1. Navigate to your pipeline in the MageML UI
2. Click the **Triggers** tab
   - Direct URL: `/pipelines/[pipeline_uuid]/triggers`
3. You'll see a comprehensive table listing all configured triggers with the following information:

#### Project-Wide Trigger Dashboard

To view all triggers across your entire project:

1. Go to the MageML homepage
2. Click **Triggers** in the left navigation menu
   - Direct URL: `/triggers`
3. You'll see all triggers from all pipelines in your project

**Trigger list columns:**
- **Active**: Toggle switch showing if the trigger is enabled/disabled
- **Logs**: Icon to access trigger execution logs
- **Name**: The trigger name (clickable link with arrow icon for code-defined triggers)
- **Type**: Trigger type (Schedule, API, or deprecated legacy Event)
- **Frequency**: How often the trigger runs (e.g., @once, @daily)
- **Status**: Current status of the trigger
- **Next**: Scheduled time for the next execution (with info icon for details)
- **Runs**: Total number of pipeline runs initiated by this trigger
- **Tags**: Associated tags for organization
- **Created**: Timestamp when the trigger was created (with info icon for details)

You can click on the logs icon to view detailed execution history and logs for each trigger.

**Enabling/Disabling triggers:**
- **From trigger list**: Use the toggle switch in the "Active" column to enable or disable a trigger
- **From trigger detail page**: Navigate to `/pipelines/[pipeline_uuid]/triggers/[trigger_id]` and use the "Disable trigger" button
- **Disabled triggers**: Will not run automatically but remain configured for future use
- **Enabled triggers**: Will run according to their configured schedule, API endpoint, or legacy event conditions

**Testing triggers:**
- **Run once button**: Click the "Run@once" button in the trigger list or detail page to manually trigger a pipeline run
- **Testing purpose**: Use this to test your trigger configuration before enabling it for automatic execution
- **Immediate execution**: The pipeline will start running immediately when you click the button

**Backfill triggers:**
- **Historical data processing**: Create [backfill triggers](../backfills/overview.md) to process historical data for a specific date range
- **One-time execution**: Backfill triggers run once and then disable themselves after completion
- **Data gap filling**: Use backfills to fill in missing data or reprocess data with updated logic

**Deleting triggers:**
- Click the delete (trash) icon in the Actions column to delete a trigger

## Advanced Trigger Configuration

### Run Settings and Performance Optimization

#### Timeout configuration

* **Set a timeout for each run of this trigger (optional)**
  * Configure a timeout in seconds for pipeline runs initiated by this trigger
  * If a run exceeds the timeout, it will be marked with the specified timeout status
* **Status for runs that exceed the timeout (default: failed)**
  * Choose how to handle runs that exceed the configured timeout
  * Options typically include: failed, cancelled, or warning

#### SLA configuration

* **Configure trigger SLA**
  * Set up Service Level Agreement monitoring for your trigger
  * Define expected completion time with configurable time units (minutes, hours, days)
  * Helps monitor trigger performance against business requirements

### Additional trigger settings

* **Override runtime variables**
  * You can override the [runtime variables](../../getting-started/runtime-variable.md) defined in the pipeline to use different values for different triggers.
  * This option only appears if you have runtime variables defined in the pipeline.
  * **Benefits**: Allows you to run the same pipeline with different configurations for different use cases (e.g., different data sources, environments, or processing parameters) without creating separate pipelines.
* **Keep running pipeline even if blocks fail**
  * When enabled, the pipeline continues running even if individual blocks fail
  * The pipeline will continue running blocks that are not dependent on the failed blocks until completion
* **(Schedule triggers only) Skip run if previous run still in progress**
  * When enabled, the scheduler will not start a new run if a previous run for this trigger is still in progress.
* **(Schedule triggers only) Create initial pipeline run if start date is before current execution period**
  * When enabled, this creates an initial pipeline run if the schedule's execution date is after its start datetime but before the current time.
  * **Example**: If current time is `1/4/24 12:00 UTC` and you create a daily trigger with start date `1/3/24 12:00 UTC`, a pipeline run with execution date `2024-01-04 00:00` is immediately created.

## Frequently Asked Questions

### What is a trigger in MageML?

A trigger is a set of instructions that determine when or how a pipeline should run. New trigger creation supports Schedule and API triggers. Event triggers are deprecated and may only appear for existing legacy configurations.

- **Schedule**: Runs pipelines on a set interval (e.g., hourly, daily, weekly, monthly, or custom cron)
- **API**: Runs pipelines when a specific API endpoint is called, optionally with runtime variables

### How do I create a trigger for my pipeline?

You can create triggers via the MageML UI or by defining them in a `triggers.yaml` file within your pipeline folder. In the UI, go to the pipeline editor, select "Triggers," and click "New trigger." In code, add your trigger configuration to `pipelines/[pipeline_uuid]/triggers.yaml`.

### Can I configure triggers in code and in the UI? How are changes synced?

Yes, you can configure triggers in both the UI and code. Changes made in the UI are synced to the YAML file, and vice versa. However, deleting a trigger from the YAML file does not remove it from the database; you must delete it in the UI.

### Why does my daily trigger run at 2AM instead of my specified time?

By default, @daily triggers run at midnight UTC. If you want your pipeline to run at a specific time, use the "custom" frequency option and enter a cron expression that matches your desired schedule.

### What happens to the triggers of a deleted pipeline?

No, deleting a pipeline does not automatically delete its associated triggers. You need to manually remove triggers in the UI to avoid orphaned triggers.

### How do I prevent a pipeline from running if a previous run is still in progress?

There is a trigger setting called "Skip run if previous run still in progress." Enabling this will prevent the scheduler from starting a new run if the previous one is still active.

### Can I allow a pipeline to continue even if some blocks fail?

Yes, you can enable the "Allow blocks to fail" setting in your trigger configuration. This allows the pipeline to continue running blocks that are not dependent on failed blocks.

### How do I trigger a pipeline via API?

You can set up an API-type trigger, which allows you to start a pipeline by making a POST request to a specific endpoint. You can also pass runtime variables in your request payload.
