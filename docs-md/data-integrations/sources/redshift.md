# Amazon Redshift

![Amazon Redshift](https://user-images.githubusercontent.com/78053898/198753538-2d606c3a-f6b0-472a-b0b3-c16086f256fc.png)

<br/>

## Configuration

To connect to your Amazon Redshift cluster and extract data, provide the following configuration parameters:

| Key              | Description                                                                                   | Sample Value                                      |
| ---------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `database`       | Name of the database you want to load data from.                                               | `demo`                                           |
| `host`           | Hostname of your Redshift cluster.                                                             | `mage-prod.3.us-west-2.redshift.amazonaws.com`    |
| `password`       | Password for the database user.                                                                | `abc123...`                                       |
| `port`           | Port where Redshift is running (typically `5439`).                                             | `5439`                                            |
| `region`         | AWS region where your Redshift cluster is located.                                             | `us-west-2`                                       |
| `schema`         | Schema containing the tables you want to extract.                                              | `public`                                          |
| `user`           | Username with permissions to read and write to the specified schema.                           | `awsuser`                                         |

<br/>

### IAM Authentication (Optional)

Alternatively, use IAM database authentication by providing the following parameters:

| Key               | Description                                                                                   | Sample Value |
| ----------------- | --------------------------------------------------------------------------------------------- | ------------ |
| `access_key_id`    | AWS access key for the IAM role or IAM user.                                                   | `abc123...`  |
| `cluster_identifier` | Cluster identifier of the Amazon Redshift cluster.                                          | `mage-prod`  |
| `db_user`         | Database user for Redshift IAM authentication.                                                 | `admin`      |
| `secret_access_key` | AWS secret access key for the IAM role or IAM user.                                           | `xyz123`     |

<br/>

### Optional Configuration

| Key               | Description                                                                                   | Sample Value |
| ----------------- | --------------------------------------------------------------------------------------------- | ------------ |
| `batch_fetch_limit` | Number of rows to fetch per batch (defaults to 50,000). Tune higher for larger clusters.     | `50000`      |

<br/>

## What is Amazon Redshift?

[Amazon Redshift](https://aws.amazon.com/redshift/) is a managed cloud data warehouse on AWS. It uses columnar storage and massively parallel processing (MPP).

<br/>
