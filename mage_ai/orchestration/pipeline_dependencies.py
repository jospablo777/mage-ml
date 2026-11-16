"""
The pipelines of a project that start or feed other pipelines, as a graph: an after-run
trigger (after_runs.py) starts a pipeline when a run of another ends, a global data product
block reads the output of the product's pipeline, and a block that calls
trigger_pipeline('<uuid>') starts that pipeline. Each pipeline comes with its last run.
"""
import re
from typing import Dict, List, Optional

from mage_ai.orchestration import after_runs

# trigger_pipeline('uuid') or trigger_pipeline(pipeline_uuid="uuid") with a literal name.
TRIGGER_CALL = re.compile(
    r'\btrigger_pipeline\(\s*(?:pipeline_uuid\s*=\s*)?[\'"]([^\'"]+)[\'"]',
)


def _edge(source: str, target: str, kind: str, label: str, **extra) -> Dict:
    return dict(source=source, target=target, kind=kind, label=label, **extra)


def after_run_edges(schedules, pipeline_uuids=None) -> List[Dict]:
    """
    A trigger with settings.after_pipeline: the followed pipeline starts the trigger's.
    Triggers of pipelines that no longer exist are left out.
    """
    edges = []
    for schedule in schedules:
        settings = schedule.settings or {}
        after = settings.get('after_pipeline')
        if not after or (pipeline_uuids is not None and schedule.pipeline_uuid not in
                         pipeline_uuids):
            continue
        edges.append(_edge(
            after, schedule.pipeline_uuid, 'after_run', after_runs.describe(settings),
            trigger=schedule.name, trigger_id=schedule.id,
            active=str(getattr(schedule.status, 'value', schedule.status)) == 'active',
        ))
    return edges


def block_edges(pipeline, products: Dict[str, Optional[str]]) -> List[Dict]:
    """
    The pipeline's blocks that read a data product (an edge from the product's pipeline)
    or start a pipeline with trigger_pipeline (an edge to it).
    """
    edges = []
    for block in pipeline.blocks_by_uuid.values():
        block_type = str(getattr(block.type, 'value', block.type))
        if block_type == 'global_data_product':
            product = ((block.configuration or {}).get('global_data_product') or {}).get('uuid')
            source = products.get(product)
            if source:
                edges.append(_edge(source, pipeline.uuid, 'data_product',
                                   f'reads product {product}', block=block.uuid))
            continue
        try:
            content = block.content or ''
        except Exception:
            continue
        for target in sorted(set(TRIGGER_CALL.findall(content))):
            edges.append(_edge(pipeline.uuid, target, 'api', f'{block.uuid} starts it',
                               block=block.uuid))
    return edges


def last_runs(pipeline_uuids: List[str]) -> Dict[str, Dict]:
    """The latest run of each pipeline: status and time."""
    from sqlalchemy import func

    from mage_ai.orchestration.db.models.schedules import PipelineRun

    if not pipeline_uuids:
        return {}
    latest = PipelineRun.query.with_entities(
        PipelineRun.pipeline_uuid, func.max(PipelineRun.id).label('id'),
    ).filter(PipelineRun.pipeline_uuid.in_(pipeline_uuids)).group_by(
        PipelineRun.pipeline_uuid,
    ).subquery()
    runs = PipelineRun.query.join(latest, PipelineRun.id == latest.c.id).all()
    return {
        run.pipeline_uuid: dict(
            id=run.id,
            status=str(getattr(run.status, 'value', run.status)),
            started_at=run.started_at.isoformat() if run.started_at else None,
            completed_at=run.completed_at.isoformat() if run.completed_at else None,
        )
        for run in runs
    }


def build(repo_path: str) -> Dict:
    """Nodes (pipelines in an edge) and edges of the project's pipeline dependencies."""
    from mage_ai.data_preparation.models.global_data_product import GlobalDataProduct
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db.models.schedules import PipelineSchedule

    uuids = Pipeline.get_all_pipelines(repo_path)
    pipelines = {}
    for uuid in uuids:
        try:
            pipelines[uuid] = Pipeline.get(uuid, repo_path=repo_path)
        except Exception:
            continue

    products = {}
    for product in GlobalDataProduct.load_all(repo_path=repo_path):
        object_type = str(getattr(product.object_type, 'value', product.object_type))
        if object_type == 'pipeline':
            products[product.uuid] = product.object_uuid

    edges = after_run_edges(PipelineSchedule.repo_query.all(), set(pipelines))
    for pipeline in pipelines.values():
        edges += block_edges(pipeline, products)

    connected = sorted({e['source'] for e in edges} | {e['target'] for e in edges})
    runs = last_runs(connected)
    nodes = []
    for uuid in connected:
        pipeline = pipelines.get(uuid)
        nodes.append(dict(
            uuid=uuid,
            name=pipeline.name if pipeline else uuid,
            type=str(getattr(pipeline.type, 'value', pipeline.type)) if pipeline else None,
            exists=pipeline is not None,
            last_run=runs.get(uuid),
        ))
    return dict(
        nodes=nodes,
        edges=edges,
        pipelines_without_dependencies=len(set(pipelines) - set(connected)),
    )
