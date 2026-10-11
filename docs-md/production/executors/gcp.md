# GCP Cloud Run executor

*Execute block runs in separate jobs.*

If your MageML app is deployed on GCP Cloud Run, you can choose to launch separate GCP Cloud Run jobs to execute blocks.

A block with this executor runs alone in production runs: it is not fused with other
blocks. See [Block fusion](../../design/data-pipeline-management.md#block-fusion).

How to configure pipeline to use GCP cloud run executor:
1. Update Project's metadata.yaml
```yaml
gcp_cloud_run_config:
  path_to_credentials_json_file: "/path/to/credentials_json_file"
  project_id: project_id
  region: us-west2       # default: us-west2
  timeout_seconds: 600   # default: 600
```

2. Update the `executor_type` of block to `gcp_cloud_run` in pipeline's metadata.yaml:

```yaml
blocks:
- uuid: example_data_loader
  type: data_loader
  upstream_blocks: []
  downstream_blocks: []
  executor_type: gcp_cloud_run
  ...
```
The GCP Cloud Run executor does not support customizing the job's compute resources.
