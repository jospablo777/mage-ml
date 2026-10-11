# AWS ECS executor

*Execute block runs in separate tasks.*

You can choose to launch separate AWS ECS tasks to execute blocks by specifying
block executor_type to be `ecs` in pipeline's metadata.yaml file.

A block with this executor runs alone in production runs: it is not fused with other
blocks. See [Block fusion](../../design/data-pipeline-management.md#block-fusion).

There're 2 ways to customize the ECS executor config,
1. Specify the `ecs_config` in project's metadata.yaml file. Example config:
```yaml
    ecs_config:
      cpu: 1024
      memory: 2048
    ```

    2. Add the `executor_config` at block level in pipeline's metadata.yaml file. Example config:
```yaml
    blocks:
    - uuid: example_data_loader
      type: data_loader
      upstream_blocks: []
      downstream_blocks: []
      executor_type: ecs
      executor_config:
        cpu: 1024
        memory: 2048
    ```

To run the whole pipeline in one ECS executor, you can set the `executor_type` at pipeline level and set `run_pipeline_in_one_process` to true.
`executor_config` can also be set at pipeline level. Here is the example pipeline metadata.yaml:

```yaml
blocks:
- ...
- ...
executor_type: ecs
run_pipeline_in_one_process: true
name: example_pipeline
...
```

## Configurations

| Field name              | Description                                                                                                                                            | Example values              |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| assign_public_ip        | Whether to assign public IP to the ECS task.                                                                                                           | true/false (default: true)  |
| cpu                     | The CPU allocated to the ECS task.                                                                                                                     | 1024 (default: 512)         |
| enable_execute_command  | Whether to [enable execute command for debugging](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-exec.html)                           | true/false (default: false) |
| launch_type             | The launch type of the ECS task.                                                                                                                       | FARGATE (default)           |
| memory                  | The memory allocated to the ECS task.                                                                                                                  | 2048 (default: 1024)        |
| tags                    | The tags of the ECS task.                                                                                                                              | ['tag1', 'tag2']            |
| wait_timeout            | The maximum wait time for the ECS task (in seconds). The default wait timeout for the ECS task is 10 minutes. Setting to -1 will disable waiting.      | 1200 (default: 600)         |

### Example

```yaml
ecs_config:
  cpu: 1024
  memory: 2048
  assign_public_ip: false
  enable_execute_command: true
  wait_timeout: 1200
```

## IAM permissions

Required IAM permissions for using ECS executor:

```json
[
  "ec2:DescribeNetworkInterfaces",
  "ecs:DescribeTasks",
  "ecs:ListTasks",
  "ecs:RunTask",
  "iam:PassRole"
]
```
