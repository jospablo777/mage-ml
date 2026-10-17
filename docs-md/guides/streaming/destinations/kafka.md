# Kafka

## Basic config

```yaml
connector_type: kafka
bootstrap_server: "localhost:9092"
topic: topic_name
batch_size: 100
timeout_ms: 500
```

## SSL authentication

```yaml
security_protocol: "SSL"
ssl_config:
  cafile: "CARoot.pem"
  certfile: "certificate.pem"
  keyfile: "key.pem"
  password: password
  check_hostname: true
```

## SASL authentication

SASL_SSL with PLAIN or SCRAM:
```yaml
security_protocol: "SASL_SSL"
sasl_config:
  mechanism: "PLAIN"  # or "SCRAM-SHA-256", "SCRAM-SHA-512"
  username: username
  password: password
ssl_config:
  cafile: "pemfilename.pem"  # Optional but recommended
```

SASL_SSL with OAUTHBEARER (OAuth 2.0):
```yaml
security_protocol: "SASL_SSL"
sasl_config:
  mechanism: "OAUTHBEARER"
  oauth_token_url: "https://keycloak.example.com/auth/realms/myrealm/protocol/openid-connect/token"
  oauth_client_id: "kafka-client"
  oauth_client_secret: "your-client-secret"
  oauth_scope: "kafka"  # Optional
ssl_config:
  cafile: "pemfilename.pem"
  check_hostname: true
```

> [!NOTE]
> **OAUTHBEARER Authentication:**
>
> - The `oauth_token_url`, `oauth_client_id`, and `oauth_client_secret` are **required** when using `OAUTHBEARER` mechanism.
> - The `oauth_scope` parameter is optional.
> - OAuth tokens are automatically cached and refreshed before expiry (60 seconds before expiration by default).
> - It's recommended to use `SASL_SSL` (not `SASL_PLAINTEXT`) with OAUTHBEARER for secure token transmission.
> - Compatible with OAuth 2.0 providers like Keycloak, Okta, and other identity servers that support client credentials flow.

```yaml
security_protocol: "SASL_SSL"
sasl_config:
  mechanism: "PLAIN"
  username: username
  password: password
```

## Data format

The Kafka data exporter accepts scalar values

```
message = <Any>
```

or dictionaries with a specific format as messages:

```
message = {
    'data': <Any>,
    'metadata': <Optional[Dict]>
}
```

A scalar value is written to the topic as the value, with no key. A dictionary is written as JSON:
its `data` field when it has one, otherwise the whole dictionary.
The following message is valid:

```python
message = {'data': {'bees': 23, 'ants': 30}}
```

Kafka supports [structuring and partitioning](https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html#kafka.KafkaProducer.send) your data.
The Kafka data exporter can create these elements if the messages received contain a metadata dictionary:

```python
message['metadata'] = {
    'dest_topic': str
    'key': str
    'time': int
}
```

The following message shows the configuration of all possible elements data and metadata:

```python
message = {
    'data': {
        'bees': 23,
        'ants': 30,
    },
    'metadata': {
        'dest_topic': 'census',
        'key': 'key',
        'time': 1693396630163,  # timestamp with ms precision
    }
}
```

If not all elements are configured, default values are assumed.

- The default topic has to be configured in the yaml file.

```yaml
connector_type: kafka
bootstrap_server: "localhost:9092"
topic: topic_name
```

- The default time timestamp is the current time of execution.

- MageML detects the broker's protocol version. Set `api_version` only for brokers that cannot report it; `0.10.2`, the old default, fails against Kafka 4, which removed protocol versions older than 2.1.

## In an exported service

[Exported services](../../../production/stream-services.md#kafka-and-azure-event-hubs) run this sink with an idempotent producer (`acks=all`).
