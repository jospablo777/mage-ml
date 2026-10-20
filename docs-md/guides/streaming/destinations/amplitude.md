# Amplitude

*Send a stream's rows to Amplitude as events or user properties, each event once.*

The Amplitude sink sends each batch's rows to an Amplitude project with its Batch Event
Upload API, from the local engine or an exported service. It runs in the engine; add it
from **Data exporter** > **Amplitude**.

## Basic config

```yaml
connector_type: amplitude
api_key: "{{ env_var('AMPLITUDE_API_KEY') }}"
region: US                       # or EU
event_type: Order Completed      # unless rows have an event_type column
columns:                         # the row's columns for Amplitude's fields
  user_id: customer_id
  time: ordered_at
key_columns: [order_id]          # what makes an event the same event
```

Each row is one event with `user_id` or `device_id`, an event type, and a time (ISO 8601,
or epoch seconds or milliseconds). Amplitude's event fields stay as they are, and every
other column becomes an event property.

## Optional fields

```yaml
user_properties: true    # set each row's user properties instead of sending events
min_id_length: 3         # Amplitude refuses ids under 5 characters by default
url: https://proxy.example.com      # another address, such as a proxy
allow_private: true      # for a proxy or `mage examples amplitude-server` in your network
```

## Each event once

Every event has an `insert_id`: the row's, else a SHA-256 of `key_columns`, else of the
event. Amplitude drops an `insert_id` it took in the last 7 days, so a batch replayed after
a crash or a retry adds nothing twice.

See [Stream services](../../../production/stream-services.md#amplitude-sink) for retries and errors,
and [Send data to Amplitude](../../amplitude/send.md) for a tutorial.
