# Partitions

*Run and track a pipeline one day, hour or region at a time, and rerun only the partitions that need it.*

A partition is one slice of a pipeline's work that runs and is tracked on its own: one day,
one hour, one region. Each partition runs as an ordinary pipeline run with the partition as a
variable, and MageML keeps which partitions are missing, failed, done, or stale because the
code or the data they read changed since they ran. After a code change, only the stale
partitions run again.

## Declare them

In the pipeline's `metadata.yaml`:

```yaml
partitions:
  key: day                  # the variable each partition's run gets
  daily:                    # or hourly, weekly, monthly
    start: 2026-01-01
    timezone: America/New_York
    close_after: 2h         # wait this long after a day ends, for late data
  upstream: [orders_raw]    # optional: pipelines with the same partitions it reads
  schedule: on_close        # optional: run each partition when it closes
  catch_up: 30              # at most this many closed partitions queued at once
```

or a fixed list:

```yaml
partitions:
  key: region
  values: [eu, us, apac]
```

| Setting | |
| --- | --- |
| `key` | The variable a partition's run gets, such as `kwargs['day']` in a block |
| `hourly`, `daily`, `weekly`, `monthly` | Time partitions from `start` (a date, or a date and time for hours), to `end` if given |
| `timezone` | Where days, weeks and months begin (default UTC). Hours are always UTC hours: a local hour happens twice on the autumn daylight saving day and not at all in spring |
| `close_after` | How long after its end a time partition exists, such as `2h` or `1d` (default none) |
| `values` | Fixed partitions, such as regions or tenants |
| `upstream` | Pipelines with the same partitions whose output this one reads |
| `schedule` | `on_close`: the scheduler queues each time partition when it closes, so a daily pipeline runs each day for the day before. Left out, partitions run when you ask |
| `catch_up` | With `on_close`, how many closed partitions that never ran are queued at once, newest first (default 30). After downtime the partitions that closed meanwhile are queued; when `on_close` is first turned on, older partitions wait for Run missing |

A partition is named `2026-11-01T13` (hourly), `2026-11-01` (daily), `2026-11-02` (weekly: its
Monday), `2026-11` (monthly), or its value. A time partition exists once it has closed: the
day `2026-11-01` with `close_after: 2h` exists from 02:00 on the 2nd.

## A partition's run

A partition's run is a pipeline run with:

- the key as a variable: `kwargs['day'] == '2026-11-01'`;
- `execution_date` at the partition's start (midnight in its time zone, in UTC), for templates
  written for it;
- its outputs in one folder, `partitions/<partition>`, whichever trigger or retry ran it, so
  a rerun replaces them.

A partition has at most one run waiting or running: asking for another returns that one.

## States

| State | When |
| --- | --- |
| missing | It exists and has no run yet |
| queued, running | Its latest run is waiting or running |
| failed | Its latest run failed or was cancelled |
| done | Its latest run completed with the current code, and no upstream partition ran after it |
| stale | Its latest run completed, but the code changed since, or an upstream pipeline's same partition completed after it |

The code is the digest [run records](../development/run-records.md) keep: the pipeline's folder,
its blocks and the project's shared code. A change to shared code makes every partition
stale, since MageML cannot know which blocks import it. When a change does not change results
(a comment, a log line), **accept the current code** for those partitions: they are done again
without running.

Nothing reruns by itself: running a year of days costs time and money, so stale and failed
partitions run when you ask. `schedule: on_close` only queues partitions that never ran.

## Run and inspect them

The pipeline's **Partitions** tab shows the counts as its headline ("1 failed, 5 stale,
7 missing of 57 partitions"), a grid of the partitions colored by state (a calendar of weeks
for daily partitions, a row per day for hourly ones), each linking to its latest run, and the
actions: Run missing, Run stale, Run failed and Accept current code. Hovering a stale
partition says why. The Compile panel shows a declaration's errors and upstream pipelines
whose partitions differ.

The same from a terminal:

```bash
mage partitions my_project daily_sales                     # every partition and its state
mage partitions my_project daily_sales --state stale       # only the stale ones, and why
mage partitions my_project daily_sales --run missing       # queue runs of the missing ones
mage partitions my_project daily_sales --run stale
mage partitions my_project daily_sales --run 2026-11-01,2026-11-02
mage partitions my_project daily_sales --accept-code stale
```

```text
daily_sales: 30 partitions of day (2 missing, 1 failed, 3 stale, 24 done).
  2026-11-01  stale  run 412  (code changed)
  2026-11-02  stale  run 413  (orders_raw ran again)
```

Queued runs start as the pipeline's concurrency allows.

The API gives the same: `GET /api/pipeline_partitions/<pipeline uuid>` returns each
partition with its state, why it is stale and its latest run, and the count of each state.
`POST /api/pipeline_partitions` with `{"pipeline_partition": {"pipeline_uuid": ...,
"action": "run" | "accept_code", "select": "missing" | "stale" | "failed" | [partitions]}}`
acts on them and returns the states after. Running them needs an editor, as triggering a
pipeline does.

## Partitions and backfills

Backfills work as before. A backfill makes runs for a date range and keeps no state; partitions
keep each slice's state, so you see what is missing or stale and run only that.
