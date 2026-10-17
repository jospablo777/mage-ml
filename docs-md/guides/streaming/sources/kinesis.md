# Kinesis

## Config

```yaml
connector_type: kinesis
stream_name: stream_name
batch_size: 100
```

## Authentication
Here are the options to authenticate with the AWS Kinesis.
1. Add the following keys and values to your environment variables
  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`
  * `AWS_REGION`
2. If you deploy MageML on AWS ECS cluster, you can use ECS execution task role to authenticate.
   You can grant the ECS task permissions to access other AWS services by attaching IAM policies
   to this ECS task execution role.

If you get the `botocore.exceptions.NoRegionError` error, try setting the `AWS_DEFAULT_REGION`
environment variable.

## In an exported service

[Exported services](../../../production/stream-services.md) do not run this connector yet: exporting a pipeline that uses it stops and says so. It runs in MageML as described above.
