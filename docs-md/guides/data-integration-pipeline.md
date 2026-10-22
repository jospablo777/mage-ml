# How to build a data integration pipeline

*The steps to build a data integration pipeline.*

## Demo video

[Video](https://user-images.githubusercontent.com/1066980/196870752-6d1b0219-8fc0-419c-99d7-677bdcf3fc0a.mp4)

---

## Requirements

Data integration sources and destinations need the `integrations` extra, which installs the
`mage-integrations` package and its connectors. The Docker image built from the MageML
repository's Dockerfile includes it. Otherwise add it:

```bash
uv add "mage-ml[integrations,postgres]"
```
```bash
python -m pip install -r https://jospablo777.github.io/mage-ml/install/v0.2.0/integrations.txt
```
```bash
uv sync --locked --no-default-groups --extra postgres --extra integrations
```

---

## Add new data integration pipeline

1. Open MageML in your browser and click the **`[+ New pipeline]`** button.
2. Click the dropdown menu option **Data integration**.

---

## Configure source

1. Click the dropdown menu under **Select source** and choose the option you
   want to load data from (e.g. Amplitude).
2. Depending on the chosen source, you’ll need to enter credentials and options
   into the section labeled **Configuration**. For example, if you chose
   Amplitude, you’ll need to enter credentials like this:

```yaml
   account: ...
   database: ...
   password: "{{ env_var('PASSWORD') }}"
   schema: ...
   table: ...
   username: ...
   warehouse: ...
   ```

   **Best practices**: you can interpolate values in the configuration using the
   following syntax:

   1. `"{{ env_var('SECRET_KEY') }}"`: this will extract the value from the
      `SECRET_KEY` key in your environment variables.
   2. `"{{ variables('SECRET_KEY') }}"`: this will extract the value from the
      `SECRET_KEY` key in your
      [runtime variables](../getting-started/runtime-variable.md).

   You can also add a prefix to output tables by configuring the following variables in the `source` configuration yaml:

```yaml
   _patterns:
     destination_table: MY_PREFIX_{{ variables('stream') }}
   ```

3. After you enter in all the credentials, click the button
   **`[View and select streams]`** under the section labeled **Select stream**.
4. In the dialog that opens, choose the stream (aka table) or streams you want
   to load data from.

### Configure stream

After selecting a stream (aka table), you’ll need to configure the schema.

Configuring the schema informs your pipeline on which fields to synchronize, how
to determine if a record is unique, and what to do if there are conflicts (aka
duplicate records).

Here are the steps you can optionally go through:

1. Selected field(s):
   - Check the box next to the field name to include the field in your
     synchronization.
   - Uncheck the ones you don’t want to sync.
2. Field type(s)
   - Each field will have a default field type.
   - Add additional field types or remove them if they don’t fit your needs.
3. Unique field(s)
   - On the right of the field names, there is a box you can check that will
     determine which field(s) need to have unique values.
   - If the box is un-checkable, that means you cannot use that field as a
     unique field.
4. Bookmark field(s)
   - Under the column labeled **Bookmark**, check the box to use the field as a
     way to keep track of progress during synchronization.
   - Upon every synchronization, these columns are used to pick up from where
     the previous synchronization left off. In addition, if a synchronization
     fails midway, these bookmark columns are used to track the record that was
     most recently successful.
5. Replication method
   - `FULL_TABLE`: synchronize the entire set of records from the source. Rows are
     added to the destination table; to replace its contents, see
     [Replace the table on a full table sync](#replace-the-table-on-a-full-table-sync).
   - `INCREMENTAL`: synchronize the records starting after the most recent
     bookmarked record from the previous synchronization run.
6. Unique conflict method: choose how to handle duplicate records
   - `IGNORE`: skip the new record if it’s a duplicate of an existing record.
   - `UPDATE`: update the existing record with the new record’s properties.

Once you've configured the schema for the stream, you can optionally check
**Run stream in parallel**. Parallel streams will be attempted to be synchronized
in parallel if there are enough worker resources available. This may speed up the
sync process if you have a large number of streams.

### Editing bookmark property values
   - You can edit the bookmark values for your next sync by editing them
   in the `Bookmark property values` table under the
   **Manually edit bookmark property values** setting in the **Settings** section.
   - The **Manually edit bookmark property values** setting will not appear until
   at least one column is selected as a bookmark field.
   - In order to enable the toggle that displays the `Bookmark property values`
   table, you need to select a destination first.
   - Click the toggle and enter the new value for your bookmark property, and then
   click `Save`. You MUST click the `Save` button in order for the bookmark value
   to be updated.

---

## Configure destination

1. Click the dropdown menu under **Select destination** and choose the option
   you want to export data to (e.g. Snowflake).
2. Depending on the chosen source, you’ll need to enter credentials and options
   into the section labeled **Configuration**. For example, if you chose
   Snowflake, you’ll need to enter credentials like this:

```yaml
   account: ...
   database: ...
   password: "{{ env_var('PASSWORD') }}"
   schema: ...
   table: ...
   username: ...
   warehouse: ...
   ```

   Best practices: you can interpolate values in the configuration using the
   following syntax:

   1. `"{{ env_var('PASSWORD') }}"`: this will extract the value from the
      `PASSWORD` key in your environment variables.
   2. `"{{ variables('PASSWORD') }}"`: this will extract the value from the
      `PASSWORD` key in your runtime variables.

### Replace the table on a full table sync

A `FULL_TABLE` stream adds its rows to the destination table. Rows deleted at the source
stay, and without unique columns every sync adds another copy. To empty the table before
each sync, add this to the destination configuration:

```yaml
truncate_before_full_table_sync: true
```

The table is emptied once per run, before the stream's first batch is written; batches of
the same stream never empty each other's rows. `INCREMENTAL` and `LOG_BASED` streams are
not affected, and a table that does not exist yet is created as usual.

| Destination | Statement |
| --- | --- |
| BigQuery, Doris, MySQL, Oracle, PostgreSQL, Redshift, Snowflake, SQL Server | `TRUNCATE TABLE` |
| Teradata | `DELETE FROM ... ALL` |
| Trino | `DELETE FROM` |

With this setting, other destinations fail the sync with a message that they cannot
truncate their table.
The table is empty from the truncate until the first batch commits; readers that must
never see it empty need a staging table and a swap instead.

---

## Run pipeline and start sync

Once you’re done configuring your pipeline, open the pipeline’s **Triggers**
page from the pipeline navigation.

While you’re still in the editor, the breadcrumbs in your header could look like this:
`Pipelines / pipeline name / Editor`.

Once you’re on the pipeline triggers page, create a
[new scheduled trigger](../design/data-pipeline-management.md#create-trigger)
and choose the `@once` interval. For more schedules, read the
[other options here](../orchestration/triggers/schedule-pipelines.md).

### Monitoring pipeline

After you create a scheduled trigger, click the **`[Enable trigger]`** button at
the top of the page. You’ll see a new pipeline run appear shortly on the screen.

You can [click the logs](../observability/logging.md) for that pipeline
run to view the progress of your synchronization. String config values 8 characters
or longer are redacted in the logs and shown as asterisks.

---

## Support

If you get stuck or run into problems, open an issue in
[GitHub issues](https://github.com/jospablo777/mage-ml/issues).
