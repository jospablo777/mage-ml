# Amazon DynamoDB

*Use MageML to extract data from Amazon DynamoDB tables using AWS credentials or IAM roles.*

## Overview

Amazon DynamoDB is a managed NoSQL database service. MageML can use DynamoDB as a source to extract data from your tables into your pipelines.

## Configuration

To connect MageML to DynamoDB, add a source block with the following configuration in your data integration pipeline.

| Key                     | Description                                                                                   | Example Value                                 |
|-------------------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------|
| `aws_access_key_id`     | Your AWS access key ID. *(Not required if using `role_arn` and MageML is running on AWS.)*    | `AKIA...`                                     |
| `aws_secret_access_key` | Your AWS secret access key. *(Not required if using `role_arn` and MageML is running on AWS.)*| `abc123xyz789...`                             |
| `aws_region`            | AWS region where your DynamoDB tables are located. Defaults to `us-west-2`.                   | `us-west-2`                                   |
| `role_arn`              | (Optional) ARN of an IAM role MageML should assume when accessing DynamoDB.                     | `arn:aws:iam::123456789012:role/mage-role`    |

If running MageML on EC2, EKS, or ECS, you can skip `aws_access_key_id` and `aws_secret_access_key` by granting access via IAM roles (via instance profiles or IRSA).

## Permissions

Ensure your AWS IAM user or role has the following permissions:

- `dynamodb:Scan`
- `dynamodb:Query`
- `dynamodb:DescribeTable`
- `dynamodb:GetItem` *(if using key-based lookups)*
- `dynamodb:ListTables` *(optional, for dynamic table discovery)*

These permissions are required for MageML to:
- Read data from DynamoDB tables
- Apply filters or projections
- Automatically detect schema or validate table access
