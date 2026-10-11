# Azure Container Instance executor

*Execute block runs in separate container instances.*

If your MageML app is deployed on Microsoft Azure with Mage’s [Terraform](https://github.com/mage-ai/mage-ai-terraform-templates/tree/master/azure)
scripts, you can choose to launch separate Azure container instances to execute blocks.

> [!NOTE]
> The Terraform templates deploy the upstream Mage image. A MageML deployment uses an image
> built from the repository's Dockerfile (`docker build -t mageml .`), pushed to your own registry.

A block with this executor runs alone in production runs: it is not fused with other
blocks. See [Block fusion](../../design/data-pipeline-management.md#block-fusion).

How to configure pipeline to use Azure Container Instance executor:
1. Update Project's metadata.yaml
```yaml
azure_container_instance_config:
  cpu: 1     # CPU cores (default: 1)
  memory: 2  # memory in GB (default: 1)
```

2. Update the `executor_type` of the block to `azure_container_instance` in pipeline's metadata.yaml and specify `executor_config` optionally.
The block level executor_config will override the global executor_config.

```yaml
blocks:
- uuid: example_data_loader
  type: data_loader
  upstream_blocks: []
  downstream_blocks: []
  executor_type: azure_container_instance
  executor_config:
    cpu: 1
    memory: 2
  ...
```
