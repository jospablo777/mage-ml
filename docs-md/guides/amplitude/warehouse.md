# Amplitude events in a warehouse

*Build a data integration pipeline that keeps Amplitude's events, daily users and tracking plan in PostgreSQL, hour by hour.*

You build a pipeline that copies an Amplitude project's events, daily active users and event
types into PostgreSQL, and keeps them current with an hourly trigger. It takes about fifteen
minutes. The same steps work for BigQuery, Snowflake, ClickHouse or any other destination.

## Before you start

- MageML installed (`uv pip install "mage-ml[integrations]"`) and a project (`mage init demo`).
- Docker, for PostgreSQL.
- An Amplitude project's API key and secret key, or the local Amplitude below.

## 1. Start PostgreSQL and a local Amplitude

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=mage -p 5432:5432 postgres:17
mage examples amplitude-server
```

The second command keeps running: leave it in its own terminal. With a real project, skip
it and use your keys and region in step 3.

## 2. Create the pipeline

```bash
cd demo
export AMPLITUDE_API_KEY=test-api-key AMPLITUDE_SECRET_KEY=test-secret-key
mage start .
```

In the browser, **New** > **Data integration**, and name it `amplitude_to_postgres`.

## 3. Pick and set up the source

Under **Source**, select **Amplitude**. The source's documentation opens under
**Documentation: amplitude**, next to its settings. Replace the settings with:

```yaml
api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
secret_key: "{{ env_var('AMPLITUDE_SECRET_KEY') }}"
region: US
start_date: "2024-01-01"
host: http://127.0.0.1:18765
```

- `start_date` is how far back the first sync reads. Leave it empty for the last 30 days.
- `host` points at the local Amplitude; delete it for a real project, and set `region: EU`
  if the project stores its data in the EU.
- The keys come from the environment, so no secret is written in the pipeline.

Press **Test connection**. It says **Connected successfully!**, or what is wrong: wrong keys,
the wrong region, or a host that does not answer.

## 4. Pick the streams

**View and select streams** lists every stream with what it holds. Select `events`,
`active_users` and `event_types`, then **Confirm 3 streams**.

Each stream comes set up: `events` syncs incrementally by `server_upload_time` and is keyed
by `uuid`, metrics by `date` and `segment`, and the tracking plan in full. Rows are
upserted by their keys, so an event Amplitude sends again replaces its row. Open the
**events** tab and press **Load sample data**: the newest events show in the **Data** panel.

## 5. Set up PostgreSQL as the destination

Under **Destination**, select **PostgreSQL**:

```yaml
database: postgres
host: localhost
port: 5432
schema: amplitude
username: postgres
password: "{{ env_var('PGPASSWORD') }}"
```

Stop MageML, `export PGPASSWORD=mage`, and start it again so the variable is set.

## 6. Run it every hour

**Triggers** > **New trigger** > **Schedule**, frequency **hourly**, and enable it. Or run
it once now with **Run@once**.

The first run reads from `start_date` to the newest hour Amplitude has finished. Check the
tables:

```bash
docker exec postgres psql -U postgres -c \
  "select event_type, count(*) from amplitude.events group by 1 order by 2 desc"
docker exec postgres psql -U postgres -c "select * from amplitude.active_users order by date"
```

Each later run continues from the last hour the previous one exported, and reads the last
three days of metrics again, as late events change them. A run that fails goes on from the
last request it finished.

## 7. Read a range again

To read a past range again, for example after fixing a destination, add it to the source's
settings and run once:

```yaml
backfill_start_date: "2024-06-01T00:00"
backfill_end_date: "2024-06-07T23:00"
```

The run reads that range and upserts it; the position of the hourly runs does not move
back. Remove the two settings afterwards, or every run reads the range again. A trigger's
start and end dates and MageML's backfills are not used for events: Amplitude finishes an
hour two hours late, and a run of a fixed interval would miss them.

## Clean up

```bash
docker rm -f postgres
```

Stop the local Amplitude with Ctrl+C.

## Next

- [Features in Python blocks](python-blocks.md): the same data as frames.
- [Amplitude source](../../data-integrations/sources/amplitude.md): cohorts, cohort members,
  `event_types` filtering and the other settings.
