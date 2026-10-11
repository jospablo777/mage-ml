# Compute resources

Follow the instructions in this [document](../deploying-to-cloud/using-terraform.md) to deploy
MageML in a production environment. When running MageML in production,
you can customize the compute resource for the following services:

1. Front-end application
1. Pipeline executor
1. Block executor

## Front-end application

Customize the compute resource of the MageML web service.

The MageML web service runs the web backend, the scheduler service
and local block executions.

You can customize the CPU and memory of the MageML web
service by updating the Terraform variables and then running `terraform apply`.

> [!NOTE]
> The Terraform templates deploy the upstream Mage image by default. For MageML, build an
> image from the repository's Dockerfile, push it to your registry, and set the template's
> image variable to it.

### Amazon Web Services (AWS)

Update the `ecs_task_cpu` and `ecs_task_memory` variables in the
[`mage-ai-terraform-templates/aws/variables.tf`](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/aws/variables.tf)
file.

### Google Cloud Platform (GCP)

Update the `container_cpu` and `container_memory` variables in the
[`mage-ai-terraform-templates/gcp/variables.tf`](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/gcp/variables.tf)
file.

---

## Pipeline executor

Set the pipeline’s executor type to customize the compute resources for pipeline runs.
Here are the available executor types:

* `local_python`
* `k8s`
* `ecs`

### Default for all blocks in a pipeline

You can set the executor type for all the blocks in a pipeline by specifying the `executor_type` at pipeline level.

---

## Block executor

MageML provides multiple executors to execute blocks.
Here are the available executor types:

* `local_python`
* `k8s`
* `ecs`
* `azure_container_instance`
* `gcp_cloud_run`

### Default for all blocks

MageML uses the `local_python` executor type by default. If you want to specify another executor_type as the default executor type for blocks,
you can set the environment variable `DEFAULT_EXECUTOR_TYPE` to one executor type mentioned above.
