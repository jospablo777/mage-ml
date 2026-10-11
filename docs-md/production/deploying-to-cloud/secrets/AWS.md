# AWS Secrets Manager

*Create secrets in AWS Secrets Manager and use them in MageML pipelines.*

### **Creating a New Secret in AWS Secrets Manager**

Before you can use AWS Secrets Manager with MageML, you need to create a new secret in the AWS Secrets Manager console. Follow these steps:

1. Open the AWS Secrets Manager [console](https://console.aws.amazon.com/secretsmanager/)
2. Click "Store a new secret."
3. On the "Choose Secret Type" page, select the appropriate secret type for your use case, and then click "Next."
4. On the "Configure Secret" page, provide the necessary details for your secret, such as the key-value pairs or the plaintext value, and then click "Next."
5. (Optional) On the "Configure Rotation" page, you can enable automatic rotation for your secret. Automatic rotation periodically updates the secret value, ensuring that your sensitive data remains secure. Configure the rotation settings according to your requirements, and then click "Next."
6. Review the secret details on the "Review" page, and if everything looks correct, click "Store."

Your new secret is now stored in AWS Secrets Manager. Make a note of the secret's ARN (Amazon Resource Name), as you'll need it to access the secret from MageML.

For more detailed information on managing and storing secrets in AWS Secrets Manager, refer to the official [AWS Documentation](https://docs.aws.amazon.com/secretsmanager/latest/userguide/managing-secrets.html)

### Working with Secrets in MageML

MageML works with AWS secrets in two ways: directly accessing them through Python code, and referencing them in YAML configuration files using placeholders that MageML resolves at runtime.

**Python:**

To use secrets in Python code, read these [**instructions**](../../../design/data-loading.md#aws-secret-loader).

**YAML:**

<details><summary>Add Secrets Through MageML UI</summary>

MageML allows you to add your secrets directly through the UI. Here’s how:

1. You can manage secrets by clicking on the **Secrets** icon in the side navigation, or from the pipeline edit page by selecting the **Secrets** tab in the sidekick.
2. Enter each secret by clicking the **New** or **New secret** button:
    - **AWS_ACCESS_KEY_ID:** A unique identifier that forms part of the credentials required to authenticate with AWS services. (not needed if using IAM role to authenticate)
    - **AWS_SECRET_ACCESS_KEY:** The secret key that, combined with the access key ID, is used to cryptographically sign requests to AWS services. (not needed if using IAM role to authenticate)
    - **AWS_DEFAULT_REGION:**  The default AWS region to use for service API calls when not explicitly specified.(or **AWS_REGION_NAME**)
![Secrets UI](https://mage-ai.github.io/assets/secrets_assets/secrets_aws.png)
3. Make the changes as pictured below to represent your secrets in the io_config.yml file
![io_config](https://mage-ai.github.io/assets/secrets_assets/io_config_aws.png)
4. Each secret is now stored and referenced for configuration to AWS

By following these steps, you can store and reference your AWS credentials and configuration through the MageML UI and use AWS services like Secrets Manager in your data pipelines.

</details>

<details><summary>Add Secrets Through Environment Variables</summary>

Add Secrets directly into your environment variables through the io_config.yml file or your docker compose file. You can also add secrets in a .env file using a similar strategy.

To use secrets in YAML files (e.g. in data integration pipelines):

1. Add the following keys and values to your environment variables
    1. **AWS_ACCESS_KEY_ID:** A unique identifier that forms part of the credentials required to authenticate with AWS services. (not needed if using IAM role to authenticate)
    2. **AWS_SECRET_ACCESS_KEY:** The secret key that, combined with the access key ID, is used to cryptographically sign requests to AWS services. (not needed if using IAM role to authenticate)
    3. **AWS_DEFAULT_REGION:**  The default AWS region to use for service API calls when not explicitly specified.(or **AWS_REGION_NAME**)
2. Store a secret in AWS Secrets Manager.
3. Use the following syntax in your YAML file to interpolate secret values from AWS Secrets Manager:

```yaml
"{{ aws_secret_var('some_name_for_secret') }}"
```

For Example

```yaml
api_key: "{{ aws_secret_var('API_KEY') }}"
access_token: "{{ aws_secret_var('ACCESS_TOKEN') }}"
```

4. Make the changes as pictured below to represent your secrets in the io_config.yml file
![io_config](https://mage-ai.github.io/assets/secrets_assets/io_config_aws.png)
5. Each secret is now stored and referenced for configuration to AWS

By following these steps, you can store and reference your AWS credentials and configuration through environment variables and use AWS services like Secrets Manager in your data pipelines.

</details>

### **Working with AWS Secrets Manager**
`aws_secret_var` accepts a secret's name or its full ARN. For example, to reference a secret stored in AWS Secrets Manager by its ARN, use the following syntax in your YAML file:

```yaml
AWS_SECRET_ACCESS_KEY: "{{ aws_secret_var('arn:aws:secretsmanager:<AWS Region>:<AWS Account ID>:secret:<AWS_SECRET_ACCESS_KEY Name>') }}"
```

The parts of the ARN:

1. **AWS_SECRET_ACCESS_KEY:** The secret value you want get from AWS Secrets Manager
2. **arn:** This is a prefix that identifies the resource as an ARN.
3. **aws:** This is the partition that the resource is in. In this case, it's the AWS partition.
4. **secretsmanager:** This is the service namespace, which identifies the AWS service. In this case, it's the AWS Secrets Manager service.
5. **AWS Region:** This is the AWS region where the secret is stored (i.e. us-west-2).
6.  **AWS Account ID:** This is the 12-digit AWS account ID that the secret belongs to.
7. **secret** This is the resource type, which indicates that the resource is a secret.
8. **AWS_SECRET_ACCESS_KEY Name:** This is the friendly name or ID of the specific secret you want to reference. It can include alphanumeric characters, hyphens, and underscores.

In the context of the YAML file, this ARN is being used with the syntax to reference the secret value stored in AWS Secrets Manager. The YAML file itself does not contain the actual secret value; instead, it references the secret by its ARN.
