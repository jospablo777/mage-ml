# Amazon SQS

## Config

```yaml
connector_type: amazon_sqs
queue_name: queue_name
batch_size: 10   # Between 1 and 10
wait_time_seconds: 1
message_deletion_method: AFTER_RECEIVED   # AFTER_RECEIVED or MANUAL

serde_config:
  serialization_method: JSON
```

## Authentication
Here are the options to authenticate with AWS SQS.
1. Add the following keys and values to your runtime environment variables:
   * `AWS_ACCESS_KEY_ID`
   * `AWS_SECRET_ACCESS_KEY`
   * `AWS_SESSION_TOKEN` if you're using temporary AWS credentials
   * `AWS_DEFAULT_REGION`
1. If you deploy MageML on AWS infrastructure, grant its runtime an IAM role with access to the
   queue. On an AWS ECS cluster, attach IAM policies for SQS to the ECS task execution role.

If you get the `botocore.exceptions.NoRegionError` error, try setting the `AWS_DEFAULT_REGION`
environment variable.

## Pass raw message to transformer

```yaml
serde_config:
  serialization_method: RAW_VALUE
```

## Message deletion method

We support two ways to delete messages:
1. Delete the message in the data loader automatically after deserializing the message body.
    * Set `message_deletion_method: AFTER_RECEIVED` in the source config.
    * The input of the transformer is the list of deserialized message body.
1. Manually delete the message in transformer after processing the message.
    * Set `message_deletion_method: MANUAL` in the source config.
    * The input of the transformer is the list of dictionary with the structure
```python
      {
        'parsed_msg_body': {'k1': 'v1', ...},
        'raw_message': sqs.Message(...)
      }
      ```

      You can use the following example code to process the message and delete it after.
```python
      @transformer
      def transform(messages: List[Dict], *args, **kwargs):
          processed_msgs = []
          for msg in messages:
              processed_msg = msg['parsed_msg_body']
              # Add your own logic to process the message
              processed_msgs.append(processed_msg)
              msg['raw_message'].delete()
          return processed_msgs
      ```

## In an exported service

[Exported services](../../../production/stream-services.md#amazon-sqs) run this source in the Rust
engine. A message is deleted once every sink took its batch, so a failure loses nothing;
`MANUAL` deletion is refused there, since blocks get each body, not the SQS message.
