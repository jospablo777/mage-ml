# SFTP

## Configuration

To load files from an SFTP server into your MageML pipelines, provide the following configuration parameters:

| Key                | Description                                                            | Sample Value             |
| ------------------ | ---------------------------------------------------------------------- | ------------------------ |
| `host`             | Hostname or IP address of the SFTP server.                             | `sftp.example.com`        |
| `port`             | Port number for the SFTP server.                                        | `22`                      |
| `username`         | Username to authenticate with the SFTP server.                         | `myuser`                  |
| `password`         | Password for authentication (optional if using private key).           | `mypassword`              |
| `private_key_file` | Path to the private key file for key-based authentication (optional).   | `/home/user/.ssh/id_rsa`  |
| `start_date`       | Earliest file date to synchronize (`YYYY-MM-DD` format).                | `2021-01-28`              |
| `tables`           | An array defining table configurations for loading files (required).   | See table config below    |
| `decryption_config`| Configuration for decrypting GPG-encrypted files (optional).            | See decryption config below |

<br/>

## Tables Configuration

Define how files are processed and structured into tables:

### Required Parameters

| Key               | Description                                                                                          | Sample Value             |
| ----------------- | ---------------------------------------------------------------------------------------------------- | ------------------------ |
| `table_name`       | Name assigned to the table (stream).                                                                | `customer_orders`         |
| `search_prefix`    | Directory path where the target files are located.                                                  | `/Export/SubFolder`       |
| `search_pattern`   | Regex pattern to match filenames for ingestion.                                                     | `MyExportData.*\\.zip.gpg$`|
| `delimiter`        | Delimiter used to separate fields (default is `,`).                                                 | `,`                       |

### Optional Parameters

| Key                      | Description                                                                                     | Sample Value               |
| ------------------------ | ----------------------------------------------------------------------------------------------- | --------------------------- |
| `key_properties`          | Array of unique keys for the table. Defaults to `['_sdc_source_file', '_sdc_source_lineno']`.    | `["_sdc_source_file"]`       |
| `encoding`                | Character encoding of the file (default: `utf-8`).                                              | `utf-8`                      |
| `sanitize_header`         | Clean up header names for SQL compatibility (default: `False`).                                 | `True` or `False`            |
| `skip_rows`               | Number of header or metadata rows to skip at the top of the file (default: `0`).                | `0`                          |

<br/>

## Decryption Configuration (Optional)

If your files are encrypted with GPG, specify the following:

| Key              | Description                                                             | Sample Value               |
| ---------------- | ----------------------------------------------------------------------- | --------------------------- |
| `SSM_key_name`    | AWS SSM parameter key name storing the GPG private key passphrase.      | `SSM_PARAMETER_KEY_NAME`     |
| `gnupghome`       | Path to your `.gnupg` directory containing GPG keys.                    | `/home/.gnupg`               |
| `passphrase`      | Passphrase for decrypting GPG files.                                   | `mysecretpassphrase`         |

<br/>

## What is SFTP?

**SFTP (SSH File Transfer Protocol)** transfers files over an encrypted SSH connection.

<br/>
