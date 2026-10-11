# Snowflake

![Snowflake](https://user-images.githubusercontent.com/78053898/198754338-a8aeb12e-6e23-45e5-b130-7a1979a2b31d.png)

<br/>

## Configuration

Connect to your Snowflake data warehouse and extract data by providing the following configuration parameters:

| Key           | Description                                                                                                  | Sample Value             |
| ------------- | ------------------------------------------------------------------------------------------------------------ | ------------------------ |
| `account`     | Your [Snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).  | `abc1234.us-east-1`       |
| `database`    | The name of the database you want to query data from.                                                        | `DEMO_DB`                 |
| `schema`      | Schema containing the tables you want to extract data from.                                                  | `PUBLIC`                  |
| `username`    | Username for accessing the database (must have read/write permissions).                                       | `guest`                   |
| `warehouse`   | Name of the warehouse for executing queries.                                                                 | `COMPUTE_WH`               |

<br/>

## Optional Connection Configs

| Key                 | Description                                                                                | Sample Value               |
| ------------------- | ------------------------------------------------------------------------------------------ | --------------------------- |
| `password`           | Password for the database user.                                                           | `abc123...`                  |
| `private_key_file`   | Path to the Snowflake private key file.                                                    | `/path/to/snowflake_key`     |
| `private_key_file_pwd`| Passphrase for the private key file.                                                      | `abc123...`                  |
| `role`               | Role of the user to access the database.                                                  | `ROLE`                       |

> For setting up key-pair authentication, follow [this guide](https://docs.snowflake.com/en/user-guide/key-pair-auth).

<br/>

## Other Optional Configs

| Key                | Description                                                                            | Sample Value |
| ------------------ | -------------------------------------------------------------------------------------- | ------------ |
| `batch_fetch_limit` | Number of rows to fetch per batch (default: 50,000). Increase for high-memory systems. | `50000`      |

<br/>

## What is Snowflake?

[Snowflake](https://www.snowflake.com/) is a cloud data warehouse. It separates compute and storage and supports structured and semi-structured data with SQL.

<br/>
