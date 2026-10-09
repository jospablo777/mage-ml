# Pushing and pulling features with Feast

Behaviors of Feast 0.66 found by `integration_tests/feast/`, with PostgreSQL as registry,
offline store and online store, Feast running as a service and Mage calling it over HTTP.
Feast requires pandas below 3, so it cannot run inside Mage's environment.

Each entry gives the effect, the test that pins it and what to do. The test service in
`integration_tests/services/feast/` applies every recommendation; its `typed_push.py` is
a route a Feast deployment can add next to Feast's own `/push`.

## Writing features

### Integers above 2**53 are rounded when the column holds a NULL

Feast's `/push` and `/write-to-online-store` build a pandas frame from the JSON body. An
Int64 feature whose column holds a NULL anywhere in the request becomes float64, so
`2**60 + 1` is stored as `2**60` and the response is 200. Feast's conversion rounds the
values even when the frame has a nullable `Int64` column.

A request in which every Int64 column is either complete or entirely NULL is converted
exactly. `/push/typed` splits each request into such groups.

Test: `test_stock_push_rounds_large_integers_next_to_a_null`,
`test_large_integers_next_to_nulls_stay_exact`.

### Values are coerced to the schema without an error

Through `/push`, `1.9` sent for an Int64 feature is stored as `1`, `123` sent for a
String feature is stored as `'123'`, `'7'` is accepted for an Int64 feature and `0` for a
Bool feature. Other mismatches, such as `'0.25'` for a Float64 feature, fail with 500.

`/push/typed` checks every value against the feature view and answers 422 with the row,
column and reason of each error, and writes nothing.

Test: `test_stock_push_coerces_values_to_the_schema`,
`test_invalid_values_are_rejected_and_nothing_is_written`.

### Integer timestamps are read as nanoseconds

`event_timestamp: 1735689600`, epoch seconds for 2025-01-01, is stored as
`1970-01-01T00:00:01.735689Z`. Send ISO 8601 strings with an offset. Naive strings are
read as UTC; `/push/typed` rejects them unless the request sets `assume_utc`.

Test: `test_stock_push_reads_integer_timestamps_as_nanoseconds`,
`test_timestamps_with_offsets_are_stored_in_utc`, `test_naive_timestamps_need_assume_utc`.

### The online store keeps the last write, not the latest event

Within one request the last row of an entity wins, whatever its event time. Across
requests an older event pushed later, by a retry or a late batch, replaces a newer
value. `/push/typed` sorts each request by event time, and with `only_newer` it skips
rows older than the stored ones.

Test: `test_stock_push_keeps_the_last_row_not_the_latest_event`,
`test_an_older_event_pushed_later_replaces_the_newer_one`,
`test_only_newer_keeps_a_newer_stored_value`.

### The PostgreSQL offline store does not take pushes

`/push` with `to: offline` fails with 500 and an empty body, because the PostgreSQL
offline store has no `offline_write_batch`. With `to: online_and_offline` the online
store is written first, then the request fails: the client sees an error, a retry
writes the online store again, and the offline store never gets the rows.
`/push/typed` answers 501 before writing anything.

To add rows to the offline store, write them to its table with Mage's PostgreSQL
exporter and call `/materialize` for their time range, as `feast_offline_polars` does.

Test: `test_pushing_to_the_offline_store_is_not_supported`,
`test_a_failed_online_and_offline_push_still_writes_online`,
`test_offline_pushes_are_refused_before_any_write`,
`test_offline_rows_written_by_mage_materialize_online`.

### An entity with every feature NULL reads as never written

Its features come back `NOT_FOUND` with the epoch as event time. When at least one
feature has a value, the NULL ones come back `PRESENT` with the event time.

Test: `test_an_entity_with_every_feature_null_reads_as_never_written`.

### Float32 features come back widened

`0.1` written to a Float32 feature reads back as `0.10000000149011612`. Use Float64 for
values compared for equality.

Test: `test_float32_features_come_back_widened`.

## Reading features

- Results follow `metadata.feature_names`, not the order of the requested features.
  Map values by name. Entity order and duplicates follow the request.
  Test: `test_results_follow_the_response_names_not_the_request_order`,
  `test_pulls_keep_the_order_and_duplicates_of_the_request`.
- Integer values above 2**53 arrive exact in the JSON. `pd.DataFrame(response_columns)`
  turns an integer column with NULL into float64 before `convert_dtypes` can help; use
  `pa.table(columns).to_pandas(types_mapper=pd.ArrowDtype)` or `pl.DataFrame(columns)`.
  Test: `test_pulled_features_into_pandas_and_polars_frames`.
- Unknown feature views and services return 404 with a JSON body. An unknown feature, a
  missing entity key, a wrong value type or a missing column return 500 with a plain
  string; a client that retries on 5xx retries these requests too.
  Test: `test_online_read_errors`, `test_push_errors`.
- Feast's feature server has no route for historical features. The test service adds
  `/get-historical-features`, which returns point-in-time values as JSON or an Arrow
  stream from `to_arrow()`: `to_df()` turns integers with NULL into float. The
  PostgreSQL offline store returns entity timestamps without a time zone; they are UTC.
  Test: `test_historical_features_are_point_in_time`,
  `test_historical_features_as_arrow_keep_types`.

## Running the service

With `conn_type: singleton`, the default, the PostgreSQL online store shares one
connection between the feature server's request threads. With 16 concurrent requests,
119 of 150 reads and 91 of 120 pushes failed with 500 (`can't change 'autocommit' now:
connection in transaction status INTRANS`). With `conn_type: pool` and `max_conn: 20`
all 300 requests succeeded. Set the pool in `feature_store.yaml`:

```yaml
online_store:
  type: postgres
  conn_type: pool
  min_conn: 1
  max_conn: 20
```

Test: `test_concurrent_reads_and_writes_all_succeed`.

The PostgreSQL stores require TLS by default; set `sslmode` for servers without it.
