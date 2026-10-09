# MongoDB with pandas and Polars

Problems found by `integration_tests/mongodb/` in Mage's MongoDB client
(`mage_ai/io/mongodb.py`) and in the MongoDB source and destination of
`mage_integrations`, against MongoDB 8 as a single-node replica set. `mage-ml[mongodb]`
installs the client's driver.

## Client export

| Frame value | Before | Now |
| --- | --- | --- |
| `Decimal` | `InvalidDocument` | `Decimal128`, 34 digits exact |
| `date` | `InvalidDocument` | midnight UTC; BSON has no date type |
| `timedelta` | `InvalidDocument` | total seconds |
| NumPy array, set, tuple | `InvalidDocument` | array; a set is sorted |
| `uuid.UUID` | `ValueError` | standard BSON binary UUID |
| empty frame or list | `TypeError` | writes nothing |
| Polars list of integers | floats | integers |

BSON dates hold milliseconds, so MongoDB drops the microseconds of every client's
datetimes. Test: `test_bson_dates_hold_milliseconds`.

The user and password were put into the connection URI unescaped, so a password with
`@`, `:` or `/` failed to connect. Test: `test_credentials_with_reserved_characters`.

Every export inserted, so a rerun duplicated the documents. `export` now takes:

- `unique_constraints` with `unique_conflict_method='UPDATE'`, which replaces the
  document with the same values in those fields, or `'IGNORE'`, which keeps it. A row
  with no value in a key field raises, since all such rows would match one document.
  Within one export, the last row with a key wins.
- `if_exists='replace'`, which writes to a staging collection with the target's indexes
  and renames it over the target. Readers see the old documents or the new ones, and a
  failed export leaves the target unchanged. A transaction would end after 60 seconds by
  default. Collection options, such as validators, are not copied.

Test: `test_upsert_on_unique_fields`, `test_replace_swaps_the_documents_and_keeps_indexes`,
`test_a_failed_replace_leaves_the_collection`.

## Client load

`load()` without options builds a pandas frame from the documents, as before: a field that
is missing from some documents, or null, turns integers into float, so `2**53 + 1`
becomes `2**53`. `load(exact_types=True)` returns pyarrow-backed columns and
`load(polars=True)` a Polars frame: integers stay integers, `Decimal128` is decimal, dates
are UTC timestamps, ObjectId and UUID are text, and embedded documents are structs. A
field with values of several types is JSON text. `projection` selects fields.
Test: `test_exact_loads`, `test_round_trip`.

## Source

- **Dates were shifted by the host's UTC offset.** pymongo returns BSON dates as naive
  datetimes in UTC, and the tap read them as local time. On a host at UTC-6, 12:00 UTC was
  emitted as 18:00Z. Datetime bookmarks moved by the same offset, so the next incremental
  sync skipped the documents written in that window. Test:
  `test_sync_emits_dates_in_utc`, `test_incremental_sync_keeps_documents_after_the_bookmark`.
- **Discovered types did not match the records.** Embedded documents, arrays and
  Decimal128 were discovered as text, while the sync emitted objects, lists and numbers;
  destinations failed the records in schema validation. A field with values of several
  types got one of them. Each field now gets the types of all its values. Test:
  `test_discovery_types_match_the_records`.
- **The sync wrote a second, partial schema.** The tap wrote a SCHEMA message built from
  the rows it had read, which replaced the stream's schema at the destination. Mage writes
  the schema from the catalog; the tap no longer writes its own.
- `direct_connection: true` connects to the given member of a replica set whose member
  addresses are not reachable, such as one in a container.

## Destination

- **It failed on its first message.** singer-sdk 0.54 renamed
  `SingerReader._assert_line_requires` and `_process_lines`; the targets built on Mage's
  `Target` class (MongoDB, Elasticsearch, OpenSearch, Salesforce) raised
  `AttributeError` on the first SCHEMA message. Test: `test_source_to_destination`.
- **Only the first key property identified a document**, so records that differed in a
  later key replaced each other. It upserts on every key, in one bulk write per batch where
  it made a round trip per record. Test: `test_destination_upserts_on_every_key`.
- **Records whose `_id` was not an ObjectId were skipped** without an error. An `_id` that
  is an ObjectId's hex string becomes an ObjectId; any other `_id` is kept. Test:
  `test_destination_keeps_ids_that_are_not_object_ids`.
- Fields with several types were refused; a value of one of the declared types is kept.
- Integers in `number` fields were converted to float, rounding values above 2**53.
- A client was created for every batch and never closed.
