# Amplitude

Sends rows to an Amplitude project: as events, or as user properties. Use it to put
warehouse data next to product data: orders, subscriptions and refunds as events; plan,
lifetime value or a model's scores as user properties, to segment and build cohorts on.

## Settings

| Key | What it is | Example |
| --- | --- | --- |
| `api_key` | The project's API key: Amplitude > Settings > Projects > your project > General. Sending needs no secret key. | `"{{ env_var('AMPLITUDE_API_KEY') }}"` |
| `region` | Where the project's data is stored: `US` or `EU`. | `US` |
| `event_types` | The event name of streams without an `event_type` column. | `{orders: Order Completed}` |
| `user_property_streams` | Streams whose rows set user properties instead of sending events. | `[customer_scores]` |

Optional: `min_id_length` (Amplitude refuses a `user_id` or `device_id` under 5
characters; set it lower for shorter ids) and `host` (a proxy, or
`mage examples amplitude-server`).

Test the connection: it checks the API key and the region without sending an event.

## Events

Each row is an event. It needs:

- `user_id` or `device_id`;
- `event_type`: a column, or the stream's name in `event_types`;
- `time`, recommended: a timestamp, an ISO 8601 string, or epoch seconds or milliseconds.
  Without it, Amplitude uses the time it receives the event.

Amplitude's event fields are sent as they are: `session_id`, `country`, `city`,
`platform`, `app_version`, `price`, `quantity`, `revenue`, `productId`, `revenueType`,
`ip`, `user_properties`, `groups` and the others. Every other column becomes an event
property.

## User properties

A stream in `user_property_streams` sets user properties: each row has `user_id` or
`device_id`, and every other column is a property to set. Empty values are left as they
are in Amplitude. A `user_properties` column of operations, such as
`{"$add": {"purchases": 1}}`, is sent as it is.

## Sent once

Each event has an `insert_id`: the row's `insert_id` column, else a hash of the stream's
key properties, else a hash of the event. Amplitude drops an `insert_id` it saw in the last
7 days, so a sync that runs again, or a request sent again after an error, adds nothing
twice. Set key properties (such as `order_id`) on event streams: then an order sent again
with a changed column is still the same event.

## Limits and errors

- Requests of up to 2,000 events. Throttling (429) waits 15 seconds and more, as Amplitude
  asks; server errors are retried.
- A user or device over 500,000 events in 24 hours stops the sync with its ids.
- An event Amplitude refuses (400) stops the sync with the field at fault and the first
  event that has it, such as an id under 5 characters.
- A wrong API key, or a project in the other region, stops the sync with that message.
