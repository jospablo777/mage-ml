# Pipeline dependencies

*See which pipelines start or feed other pipelines, with each pipeline's last run.*

**Dependencies** in the side navigation draws the project's pipelines that start or feed
other pipelines, and lists each link below the graph. A pipeline appears when it is in at
least one link; the page counts the others.

| Link | Found from | Arrow |
| --- | --- | --- |
| After-run trigger | A trigger with `after_pipeline` (see [triggering pipelines](../../orchestration/triggers/triggering-pipelines.md)) | From the pipeline whose run ends to the trigger's pipeline |
| Global data product | A global data product block in the pipeline | From the product's pipeline to the pipeline that reads it |
| `trigger_pipeline` | A block calling `trigger_pipeline('<pipeline uuid>')` with the name written out | From the block's pipeline to the pipeline it starts |

Each pipeline shows its last run's status and when it ended. Click a pipeline, in the graph
or the list, to open its triggers. A pipeline named by a link but not in the project is
drawn with a dashed border and listed as missing: a trigger that follows a deleted pipeline,
for example. Triggers of deleted pipelines are not shown; deleting a pipeline makes its
triggers inactive.

A block that builds the pipeline name at run time (a variable, a loop) is not found: only a
name written in the call is.

The same graph is at `GET /api/pipeline_dependencies/project`, for viewers and up:

```json
{
  "pipeline_dependency": {
    "nodes": [{"uuid": "orders", "name": "orders", "last_run": {"status": "failed", "...": "..."}}],
    "edges": [{"source": "orders", "target": "cleanup", "kind": "after_run",
               "label": "after orders fails", "trigger": "cleanup after orders"}],
    "pipelines_without_dependencies": 12
  }
}
```
