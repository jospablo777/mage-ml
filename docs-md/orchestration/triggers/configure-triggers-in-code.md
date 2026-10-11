# Configure triggers in code

*Configure triggers in triggers.yaml under pipeline folder.*

In addition to configuring triggers via UI, you can also configure triggers in code.

You can either manually define the triggers in code or you can go to the triggers detail page in the
UI and click the button labeled <b>Save trigger in code</b> on the left side panel to store the
current trigger’s settings to code.

## How triggers in code are synced to the database and UI

- **Source of truth:** The `triggers.yaml` file is the source of truth for trigger definitions.
- **Sync flow:** A background scheduler process periodically syncs triggers from `triggers.yaml` into the database. Only triggers that pass the **envs filter** (see [Envs filter](#envs-filter)) are created or updated on that instance. After a trigger is synced to the database, it appears in the Triggers UI.
- **New/updates flow both ways:** New or updated triggers in the YAML are pushed to the database when the scheduler runs. Edits you make in the UI to a trigger that's defined in code are written back to `triggers.yaml` so UI and code stay in sync.
- **Deletions are not synced:** Removing a trigger from `triggers.yaml` does **not** remove it from the database. You must delete the trigger in the UI (or via API) to remove it from the database. See [Delete triggers](#delete-triggers).

## Create and configure triggers

Here are the steps to create and configure triggers via code:
1. Create a `triggers.yaml` file under your pipeline folder. The file path should be `pipelines/[pipeline_uuid]/triggers.yaml`.
1. Enter your trigger configs into the `triggers.yaml` file.
    1. Content structure
```yaml
        triggers:
        - name: test_trigger
          schedule_type: time
          start_time: 2023-01-01
          status: active
        - name: xxx
          ...
        - name: xxx
          ...
        ```
    1. **Required trigger fields**
        - `name`: Unique identifier of the trigger (per pipeline).
        - `schedule_type`: `time`, `api` or `after_run` for new triggers. `event` is deprecated and should only be used to maintain existing legacy event triggers.
        - For **time (schedule)** triggers: `schedule_interval` and `start_time` are required.
            - `schedule_interval`: `@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@always_on`, or a Cron expression (e.g. `* * * * *`).
            - `start_time`: Persisted schedule start timestamp. For `@once`, this is the single run time (e.g. `2023-01-01` or `2023-01-01 00:00:00`).
        - For **api** triggers: `schedule_interval` and `start_time` are typically not used for execution but may still be present in the config.
        - For **after_run** triggers: `settings.after_pipeline` names the pipeline whose runs start this one, and `settings.after_status` which endings do (`completed`, `failed` or `any`). See [After-run triggers](triggering-pipelines.md#after-run-triggers).
    1. **Optional trigger fields**
        - `status`: `active` or `inactive`. Defaults to `inactive` if omitted.
        - `variables`: A dictionary of [runtime variables](../../getting-started/runtime-variable.md) for this trigger.
        - `sla`: SLA in seconds for this trigger.
        - `settings`: A dictionary of [advanced settings](triggering-pipelines.md#additional-trigger-settings):
            - `skip_if_previous_running`: `true`/`false`. Do not start a new run if the previous run is still in progress.
            - `allow_blocks_to_fail`: `true`/`false`. Allow the pipeline to continue running non-dependent blocks if some blocks fail.
            - `create_initial_pipeline_run`: `true`/`false`. Create an initial run when the start time is in the past (schedule triggers).
        - `envs`: List of environments where this trigger should be synced and run: `dev`, `staging`, `prod`, `test`. See [Envs filter](#envs-filter) below.

1. Save the `triggers.yaml` file. On the next scheduler run, trigger configs are synced to the database (subject to the envs filter) and then show in the Triggers UI.

### Example triggers config:

```yaml
triggers:
- name: test_example_trigger
  schedule_type: time
  schedule_interval: "@daily"
  start_time: 2023-01-01
  status: active
- name: test_example_trigger_2
  schedule_type: time
  schedule_interval: "@hourly"
  start_time: 2023-03-01
  status: inactive
- name: test_example_trigger_with_extra_settings
  schedule_type: time
  schedule_interval: "@hourly"
  start_time: 2023-03-01
  status: inactive
  settings:
    skip_if_previous_running: true
    allow_blocks_to_fail: true
  envs:
  - prod
  - dev
```

## Envs filter

The optional `envs` field limits which instances or workspaces sync and run the trigger. Each instance/workspace has an environment set by the `ENV` [environment variable](../../development/variables/environment-variables.md).

- **When `envs` is not set or empty:** The trigger is synced to **all** instances/workspaces. Every environment will get the trigger in the database and can run it.
- **When `envs` is set:** The trigger is synced **only** to instances/workspaces whose `ENV` value is in the `envs` list. Other instances will not create or update this trigger in their database, so they will not run it.
  - Allowed values are `dev`, `staging`, `prod`, and `test`. An `ENV` of `production` counts as `prod`; any other unknown value counts as `dev`.

**If you add an `envs` filter to a trigger that already exists everywhere:** Triggers that were previously synced (e.g. when `envs` was empty) remain in the database on instances that no longer match the new `envs` list. The sync process only creates or updates triggers; it does not delete them. To stop such a trigger from existing (and running) on those instances, you must **delete the trigger manually** in the UI on each affected instance or workspace.

## Modify triggers

If an existing trigger is defined in the YAML file, changes flow both ways: updates in the UI are written back to `triggers.yaml`, and updates in the YAML are synced to the database (and reflected in the UI when you reload). Only triggers that pass the [envs filter](#envs-filter) are synced to the database on each instance.

## Delete triggers

The `triggers.yaml` file is synced into the database automatically, but **deletions work differently**:

- **Delete one trigger:** Removing a trigger from the YAML file does not delete it from the database.
  Delete it in the UI on each instance where it exists: in the Pipeline triggers table
  (`/pipelines/[pipeline_uuid]/triggers`), click the trash can icon. Deleting a trigger in the UI
  also removes it from `triggers.yaml`. If an [envs filter](#envs-filter) previously created the
  trigger on other instances, delete it manually on those instances as well; sync does not remove
  triggers from their databases.

As a reversible alternative, set the trigger's `status` to `inactive` in `triggers.yaml`. Status
changes are automatically synced to the database and UI.
