# Elasticsearch

## Basic config

```yaml
connector_type: elasticsearch
host: http://localhost
index_name: elastic_index
# When using Data streams this must be set to create as they are Append-only indexes
# _op_type: None / index, create or update
# _source: None (the message is the document) / a key to wrap each message under
# The message field that becomes the document id, so a replay replaces documents
# _id: order_id
# # Whether to verify SSL certificates to authenticate.
# verify_certs: true
# ca_cert: "certificate.cert"
```

## Authentication

```yaml
verify_certs: true
api_key: api_key_for_elastic_search
ca_cert: "certificate.cert"
```

- **verify_certs**: Whether to verify SSL certificates to authenticate.

## In an exported service

[Exported services](../../../production/stream-services.md#elasticsearch-and-opensearch) run this sink
in Rust through the bulk API, with the same settings. Secrets (`api_key`) come from the
environment, as `"{{ env_var('NAME') }}"`, and `ca_cert` is the PEM text, not a file path.

## Documents and ids

Each message is the document. With `_id`, the named message field becomes the document's
id, so writing a message again replaces its document instead of adding one; with
`_op_type: update`, the message's fields update the document (a partial update). Before
MageML 0.2 the bulk writes stored each message as `{"_doc": message, "id": ...}` and never
set the document id; set `_source: _doc` to keep that shape for an existing index.
