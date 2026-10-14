# mlflow-ibm-cos

MLflow artifacts in IBM Cloud Object Storage (COS), for MLflow servers and clients.

- `cos://<bucket>/<path>` artifact locations.
- IAM API keys and trusted profiles (Code Engine, IKS) over plain boto3: no `ibm_boto3`, no
  `sitecustomize.py`, no swapping modules. HMAC keys work too.
- Existing `s3://` locations on a COS endpoint (`MLFLOW_S3_ENDPOINT_URL`) authenticate with
  IAM as well.
- boto3's default upload checksums, which COS fails, are turned off for COS.

```bash
uv pip install "mlflow-ibm-cos @ git+https://github.com/jospablo777/mage-ml#subdirectory=packages/mlflow-ibm-cos"

export IBM_COS_ENDPOINT=https://s3.us-south.cloud-object-storage.appdomain.cloud
export IBM_CLOUD_API_KEY=...          # or IBM_COS_HMAC_ACCESS_KEY_ID and IBM_COS_HMAC_SECRET_ACCESS_KEY
mlflow server --artifacts-destination cos://models/mlflow --backend-store-uri postgresql://...
```

| Variable | Meaning |
| --- | --- |
| `IBM_COS_ENDPOINT` | The bucket's regional endpoint (public, private or direct) |
| `IBM_COS_HMAC_ACCESS_KEY_ID`, `IBM_COS_HMAC_SECRET_ACCESS_KEY` | HMAC keys; they win over an API key |
| `IBM_COS_API_KEY` or `IBM_CLOUD_API_KEY` | An IBM Cloud API key |
| `IBM_CLOUD_TRUSTED_PROFILE` | A trusted profile id or name, with the compute resource token Code Engine or IKS mounts |
| `IBM_COS_INSTANCE_ID` | The COS instance CRN, for listing and creating buckets |
| `IBM_CLOUD_IAM_URL` | A private IAM endpoint (`https://private.iam.cloud.ibm.com`) |

With an API key, presigned URLs are not possible (COS presigns only with HMAC keys), so keep
MLflow's artifact proxy on (the default with `--artifacts-destination`).

The code is MageML's (`mage_ai/services/ibm`); MageML has the same support built in. See the
[IBM Cloud guide](https://github.com/jospablo777/mage-ml/blob/master/docs-md/integrations/ibm-cloud.md).
