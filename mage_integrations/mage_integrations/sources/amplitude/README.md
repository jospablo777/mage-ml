# Amplitude

Events, daily metrics, the tracking plan and cohorts of an Amplitude project.

## Settings

| Key | What it is | Example |
| --- | --- | --- |
| `api_key` | The project's API key: Amplitude > Settings > Projects > your project > General. | `"{{ env_var('AMPLITUDE_API_KEY') }}"` |
| `secret_key` | The project's secret key, on the same page. | `"{{ env_var('AMPLITUDE_SECRET_KEY') }}"` |
| `region` | Where the project's data is stored: `US` (amplitude.com) or `EU` (analytics.eu.amplitude.com). | `US` |
| `start_date` | How far back the first sync reads. Empty: 30 days. | `2024-01-01` |
| `cohort_ids` | Cohorts whose users the `cohort_members` stream reads (the id in the cohort's URL). | `[abc123]` |

Optional:

| Key | What it is | Default |
| --- | --- | --- |
| `event_types` | Only these events, such as `[Purchase, Sign Up]`. | all events |
| `window_hours` | Hours of events per Export API request. | `24` |
| `metrics_lookback_days` | Days of metrics read again on each sync, since late events change them. | `3` |
| `cohort_properties` | `true` to add the users' properties to `cohort_members`, or a list of property names. | `false` |
| `availability_delay_hours` | Hours Amplitude takes to finish an hour of events. | `2` |

Test the connection: it checks the keys and the region.

## Streams

| Stream | What it holds | Sync |
| --- | --- | --- |
| `events` | Every event, with its event and user properties (Export API). | Incremental, by the hour Amplitude received it |
| `active_users`, `new_users` | Users per day. | Incremental; the last days again |
| `average_session_length`, `sessions_per_user` | Sessions per day. | Incremental; the last days again |
| `event_types`, `event_properties`, `user_properties` | The tracking plan: names, types, descriptions. | Full table |
| `annotations` | Chart annotations (releases, campaigns). | Full table |
| `cohorts` | Behavioral cohorts: name, size, owners. | Full table |
| `cohort_members` | The users of the cohorts in `cohort_ids`. Each sync downloads them; Amplitude allows 500 downloads a month. | Full table |

## How events sync

- The first sync reads from `start_date`; each sync after it continues from the last hour it
  exported, up to the newest hour Amplitude has finished (about two hours ago). A trigger's
  interval is not used, so no hour is skipped whatever the schedule.
- The position is saved after each window of `window_hours`: a sync that fails goes on from
  there next time.
- Rows are upserted by `uuid` (events) and by date and segment (metrics), so an event
  Amplitude sends again, or a day of metrics read again, replaces its row.
- To read a range again, set `start_date` and `end_date` in the source's query (runtime
  variables of a trigger or backfill). The saved position does not move back.
- Times are ISO 8601 in UTC.

## Errors

- Wrong keys or the wrong region stop the sync with that message: a project stored in the
  EU needs `region: EU`.
- Amplitude's rate limits (429) and server errors are retried, waiting as Amplitude asks.
- An hour larger than the Export API sends (4 GB) stops the sync: export such a project to
  Amazon S3 or a warehouse with an Amplitude destination instead.

## Other ways in

- Python blocks: the Amplitude data loader template, or `mage_ai.io.amplitude.Amplitude`
  (`load_events`, `active_users`, `cohort_members`, ...).
- Streams: the Amplitude stream template receives events from Amplitude's Webhook
  destination as they happen.
