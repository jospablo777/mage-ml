# Google Sheets

*Read and write Google Sheets from MageML with the Google Sheets API through the gspread library.*

<div align="center">
<img src="https://github.com/mage-ai/mage-ai/assets/59450879/0a10316e-1b0d-4727-858f-c743cc1e296c" alt="true" width="55%" />
</div>

## Configuration

To use Google Sheets with MageML, enable two APIs in your GCP project and add credentials to MageML.

### Enable relevant APIs

Enable these APIs in GCP:

- Google Drive API
- Google Sheets API

Share the Google Sheet with the service account MageML uses.

You can follow [this guide](https://docs.gspread.org/en/latest/oauth2.html) to enable relevant services.

### Add credentials

Adding credentials for Sheets is the same as adding credentials for other Google services, i.e. BigQuery, GCP.

1. Create a service account in your GCP project and download its JSON key.
2. Open `io_config.yaml` at the root of your project and enter one of these under the
   `default` key (or another profile):

```yaml
version: 0.1.1
default:
  # Option 1: path to the service account key file
  GOOGLE_SERVICE_ACC_KEY_FILEPATH: "/path/to/your/service/account/key.json"

  # Option 2: the contents of the key
  # GOOGLE_SERVICE_ACC_KEY:
  #   type: service_account
  #   project_id: project-id
  #   private_key_id: key-id
  #   private_key: "-----BEGIN PRIVATE KEY-----\nyour_private_key\n-----END PRIVATE KEY-----"
  #   client_email: your_service_account_email
  #   auth_uri: "https://accounts.google.com/o/oauth2/auth"
  #   token_uri: "https://accounts.google.com/o/oauth2/token"
  #   auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
  #   client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/your_service_account_email"

  GOOGLE_LOCATION: US  # Optional, BigQuery only
```

If both are set, `GOOGLE_SERVICE_ACC_KEY` is used. The `io_config.yaml` that
`mage init` creates has a placeholder `GOOGLE_SERVICE_ACC_KEY`; delete it when you use the
file path. When the server runs in the container
image, put the key file inside the project folder (such as
`/home/src/default_repo/key.json`) so the path is valid in the container.

## Usage

There are two Python templates for Google Sheets, one under "Data Loader" and one under "Data Exporter".

You can see the source code for the templates [here](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/data_preparation/templates/data_exporters/google_sheets.py) and [here](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/data_preparation/templates/data_loaders/google_sheets.py).
