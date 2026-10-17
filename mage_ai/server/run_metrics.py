"""
Pipeline and block run metrics for Prometheus, read from the database at each scrape.

Runs execute in the scheduler and in worker processes, on any replica, so counters kept in
the web server's process would miss them; /metrics had server metrics only (upstream
#5196). The database holds every run of the project's triggers.
"""
from typing import Iterator, List

from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import func

from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.schedules import (
    BlockRun,
    PipelineRun,
    PipelineSchedule,
)


def _status(value) -> str:
    return getattr(value, 'value', value) or 'unknown'


class RunMetricsCollector(Collector):
    @safe_db_query
    def collect(self) -> Iterator[GaugeMetricFamily]:
        schedule_ids: List[int] = [
            schedule_id for (schedule_id,) in
            PipelineSchedule.repo_query.with_entities(PipelineSchedule.id).all()
        ]
        in_project = PipelineRun.pipeline_schedule_id.in_(schedule_ids)

        runs = GaugeMetricFamily(
            'mage_pipeline_runs',
            'Pipeline runs of the project by pipeline and status.',
            labels=['pipeline', 'status'],
        )
        for pipeline, status, count in (
            PipelineRun.select(PipelineRun.pipeline_uuid, PipelineRun.status,
                               func.count(PipelineRun.id))
            .filter(in_project)
            .group_by(PipelineRun.pipeline_uuid, PipelineRun.status)
            .all()
        ):
            runs.add_metric([pipeline, _status(status)], count)
        yield runs

        blocks = GaugeMetricFamily(
            'mage_block_runs',
            'Block runs of the project by pipeline, block and status.',
            labels=['pipeline', 'block', 'status'],
        )
        for pipeline, block, status, count in (
            BlockRun.select(PipelineRun.pipeline_uuid, BlockRun.block_uuid, BlockRun.status,
                            func.count(BlockRun.id))
            .join(PipelineRun, PipelineRun.id == BlockRun.pipeline_run_id)
            .filter(in_project)
            .group_by(PipelineRun.pipeline_uuid, BlockRun.block_uuid, BlockRun.status)
            .all()
        ):
            blocks.add_metric([pipeline, block, _status(status)], count)
        yield blocks

        duration = GaugeMetricFamily(
            'mage_pipeline_run_last_duration_seconds',
            'Seconds the last completed run of each pipeline took.',
            labels=['pipeline'],
        )
        success = GaugeMetricFamily(
            'mage_pipeline_run_last_success_timestamp_seconds',
            'When the last completed run of each pipeline ended, as a Unix time.',
            labels=['pipeline'],
        )
        latest = (
            PipelineRun.select(PipelineRun.pipeline_uuid,
                               func.max(PipelineRun.completed_at).label('completed_at'))
            .filter(in_project, PipelineRun.status == PipelineRun.PipelineRunStatus.COMPLETED)
            .group_by(PipelineRun.pipeline_uuid)
            .subquery()
        )
        for pipeline, started_at, completed_at in (
            PipelineRun.select(PipelineRun.pipeline_uuid, PipelineRun.started_at,
                               PipelineRun.completed_at)
            .join(latest, (latest.c.pipeline_uuid == PipelineRun.pipeline_uuid)
                  & (latest.c.completed_at == PipelineRun.completed_at))
            .filter(in_project)
            .all()
        ):
            if completed_at is None:
                continue
            success.add_metric([pipeline], _timestamp(completed_at))
            if started_at is not None:
                duration.add_metric(
                    [pipeline], max(_timestamp(completed_at) - _timestamp(started_at), 0),
                )
        yield duration
        yield success


def _timestamp(value) -> float:
    from mage_ai.shared.dates import as_utc

    return as_utc(value).timestamp()


def register(registry=None) -> RunMetricsCollector:
    from prometheus_client import REGISTRY

    collector = RunMetricsCollector()
    (registry or REGISTRY).register(collector)
    return collector
