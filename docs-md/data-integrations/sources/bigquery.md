# BigQuery

## Configuration

To set up the BigQuery source, provide the following configuration parameters:

| Key                         | Description                                                                                                                | Sample Value                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `path_to_credentials_json_file` | Path to your Google service account credentials JSON file. If MageML is running on GCP, you can leave this `null` to use the instance’s service account. | `/path/to/service_account_credentials.json` |
| `dataset`                   | BigQuery dataset you want to read data from.                                                                               | `example_dataset`                      |
| `credentials_info`           | An alternative way to specify Google service account credentials directly as a dictionary.                                | Structure shown below                  |

<br/>

`credentials_info` structure:
```yaml
auth_provider_x509_cert_url: str
auth_uri: str
client_email: str
client_id: str
client_x509_cert_url: str
private_key: str
private_key_id: str
project_id: str
token_uri: str
type: str
```
<br />

### Required permissions

Your BigQuery account should at least have the below permissions to use the BigQuery source
```
bigquery.datasets.create
bigquery.jobs.create
bigquery.readsessions.create
bigquery.readsessions.getData
```

Your account should also have "BigQuery Data Editor" role for the BigQuery dataset you specified in the config.
