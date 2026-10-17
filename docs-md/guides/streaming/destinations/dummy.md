# Dummy

Dummy sink will print the message optionally and discard the message. This dummy sink will be useful when users want to trigger other pipelines or 3rd party services using the ingested data in transformer.

## Config

```yaml
connector_type: dummy
print_msg: true
```

## In an exported service

[Exported services](../../../production/stream-services.md#what-runs) run this sink: it prints each message to the service's output.
