# Amplitude events as they happen

*Receive each Amplitude event seconds after it happens with the Webhook destination: build the stream on samples, run it on the engine, and deploy it as a service.*

You build a stream that receives every event Amplitude forwards through its Webhook
destination, turns it into one flat row, and writes it to PostgreSQL. Amplitude retries an
event for up to four hours when it gets no answer; the stream writes each event once. It
takes about fifteen minutes.

## Before you start

- MageML and the stream engine: `uv pip install "mage-ml[streaming]" mageml-engine`.
- Docker, for PostgreSQL.
- For the last step, an Amplitude plan with destinations (Plus, Growth or Enterprise) and an
  HTTPS address for the service.

## 1. Create the stream

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=mage -p 5432:5432 postgres:17
mage init demo
mage start demo
```

**New** > **Streaming**, name it `amplitude_events`, and start from **Amplitude (webhook)**.
The pipeline has three blocks and a sample of five events:

| Block | What it does |
| --- | --- |
| `amplitude_events_amplitude` | Source: `connector_type: amplitude`, the service's webhook for Amplitude |
| `amplitude_events_transform` | One flat row per event: `uuid`, `event_type`, `event_time`, `user_id`, `device_id`, `user_plan`, and each event property as `property_<name>` |
| `amplitude_events_print` | A sink that prints; replaced in step 3 |

## 2. Develop on the sample

Open the **Samples** tab and run the transformer on `example` (Cmd+Enter, Ctrl+Enter on
Linux and Windows): five events in, five rows out. Change the transformer, for example to
keep only purchases, and run it again; nothing is sent anywhere.

## 3. Write to PostgreSQL

Replace the printing sink with:

```yaml
connector_type: postgres
host: localhost
port: 5432
database: postgres
username: postgres
password: "{{ env_var('PGPASSWORD') }}"
schema: amplitude
table: events_live
unique_constraints: [uuid]
unique_conflict_method: UPDATE
```

## 4. Run it and send events as Amplitude does

Open the **Run** tab and press **Start on the local engine**. MageML asks before a run
writes to a real sink; confirm `amplitude_events_print`'s replacement, the PostgreSQL sink.
In a second terminal:

```bash
export PGPASSWORD=mage
mage examples amplitude-webhook --pipeline amplitude_events --project demo --count 200
```

The command posts 200 events as the Webhook destination does: one per request, with
Amplitude's format and headers, and about one in twenty sent twice, as Amplitude retries
them. The Run tab shows 200 messages in and 200 out, and the table has one row per event:

```bash
docker exec postgres psql -U postgres -c \
  "select count(*), count(distinct uuid) from amplitude.events_live"
```

The source uses each event's `uuid` as its idempotency key: an event sent again within a day
(`dedupe_window_seconds`) is answered as delivered and written nothing.

## 5. Run it as a service

```bash
mage service new amplitude --pipelines amplitude_events
```

The service reads its settings from its environment files. Its ingest token,
`MAGE_SERVICE_INGEST_TOKEN`, is the one Amplitude sends; it can only send messages, so
Amplitude never holds the service's own token. Pick a long random value and keep the
secrets in `services/amplitude/.env.dev`, which Git ignores:

```bash
echo "MAGE_SERVICE_INGEST_TOKEN=$(openssl rand -hex 24)" > services/amplitude/.env.dev
echo "PGPASSWORD=mage" >> services/amplitude/.env.dev
mage service run amplitude
```

Deploy it behind HTTPS with the same variables set as secrets (see
[Services](../../production/services-page.md)).

## 6. Connect Amplitude

In Amplitude: **Data** > **Destinations** > **Webhook**.

1. **Webhook URL**: `https://<service>/v1/streams/amplitude_events/messages`
2. **Headers**: `Authorization` with the value `Bearer <MAGE_SERVICE_INGEST_TOKEN>`
3. **Send events**: on, with the default payload (Amplitude's event format). The template's
   transformer expects it.
4. **Send users**: optional. User payloads have no `event_type`, and the transformer skips
   them; change it to keep them.
5. Enable the destination.

The service answers 200 only after PostgreSQL took the event. Any other answer makes
Amplitude retry, and the `uuid` keeps the retry from writing twice.

To check the service before Amplitude does:

```bash
mage examples amplitude-webhook --url https://<service>/v1/streams/amplitude_events/messages \
  --token "$MAGE_SERVICE_INGEST_TOKEN" --count 10
```

## Clean up

```bash
docker rm -f postgres
```

## Next

- [Events through Kafka](kafka.md): when Amplitude already writes to Kafka.
- [Stream services](../../production/stream-services.md): sinks, dead letters, scaling.
