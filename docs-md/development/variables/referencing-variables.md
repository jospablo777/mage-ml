# Accessing variables in MageML

*Access variables and secrets in your code.*

MageML interpolates variables specific to your [pipeline](../../getting-started/runtime-variable.md) or [project](environment-variables.md). The following syntax is specific to `yaml` and `SQL` files.

Here's a list of the different variables and functions you can use in your code:

| Syntax                              | Description                                                                           |
| ----------------------------------- | ------------------------------------------------------------------------------------- |
| `{{ env_var('secret') }}`           | Get secret from [environment variables](environment-variables.md). |
| `{{ variables('secret') }}`         | Get secret from [runtime variables](../../getting-started/runtime-variable.md).                |
| `{{ mage_secret_var('secret') }}`   | Get secret from [MageML secrets](secrets.md).                      |
| `{{ aws_secret_var('secret') }}`    | Get secret from [AWS Secrets Manager](../../production/deploying-to-cloud/secrets/AWS.md).     |
| `{{ azure_secret_var('secret') }}`  | Get secret from [Azure Key Vault](../../production/deploying-to-cloud/secrets/Azure.md).       |
| `{{ json_value(json_obj, 'key') }}` | Extract value from a JSON string.                                                     |
| `{{ n_days_ago(N) }}`               | Get the date `N` days ago (data integration pipelines only).                          |
