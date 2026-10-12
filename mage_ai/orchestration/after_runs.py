"""
After-run triggers: a pipeline's trigger that starts a run when a run of another pipeline
ends. Its settings name the pipeline to follow (after_pipeline) and which endings start it
(after_status: completed, the default; failed; or any). The scheduler calls start_followers
when a run completes or fails; the run it creates starts on the scheduler's next tick, within
the trigger's and the pipeline's concurrency limits, like a run from the API.

Exported pipeline services run the same triggers (pipeline_services/export.py).
"""
import logging
from typing import Callable, Dict, Iterable, List, Optional, Tuple

ENDINGS = ('completed', 'failed', 'any')
ENDING_WORDS = {'completed': 'completes', 'failed': 'fails', 'any': 'ends'}
# How many recent runs of a trigger are looked at to find the one an upstream run started.
RECENT_RUNS = 50



def _logger():
    return logging.getLogger(__name__)


def ending(settings: Optional[Dict]) -> str:
    return ((settings or {}).get('after_status') or 'completed').lower()


def follows(settings: Optional[Dict], pipeline_uuid: str, status: str) -> bool:
    """Whether a run of pipeline_uuid that ended with status starts this trigger."""
    settings = settings or {}
    if settings.get('after_pipeline') != pipeline_uuid:
        return False
    wanted = ending(settings)
    if wanted == 'any':
        return status in ('completed', 'failed')
    return wanted == status


def describe(settings: Optional[Dict]) -> str:
    settings = settings or {}
    word = ENDING_WORDS.get(ending(settings), 'completes')
    return f"after {settings.get('after_pipeline')} {word}"


def trigger_problems(
    pipeline_uuid: str,
    settings: Optional[Dict],
    others: Iterable[Tuple[str, Optional[Dict]]] = (),
    pipeline_exists: Optional[Callable[[str], bool]] = None,
) -> List[str]:
    """
    What is wrong with an after-run trigger of pipeline_uuid: the pipeline it follows, the
    ending, and loops with the other after-run triggers, given as (pipeline, settings).
    """
    settings = settings or {}
    after = settings.get('after_pipeline')
    problems = []
    if not after or not isinstance(after, str):
        problems.append('Choose the pipeline whose runs start this one (after_pipeline).')
        return problems
    if after == pipeline_uuid:
        problems.append(f'Pipeline {pipeline_uuid} cannot start after its own runs.')
    elif pipeline_exists is not None and not pipeline_exists(after):
        problems.append(f'Pipeline {after}, which this trigger follows, does not exist.')
    if ending(settings) not in ENDINGS:
        problems.append(
            f"after_status {settings.get('after_status')!r} is not completed, failed or any."
        )
    if problems:
        return problems

    edges: Dict[str, List[str]] = {}
    for follower, other in [*others, (pipeline_uuid, settings)]:
        upstream = (other or {}).get('after_pipeline')
        if upstream and upstream != follower:
            edges.setdefault(upstream, []).append(follower)

    def path_back(node: str, target: str, seen: set) -> Optional[List[str]]:
        if node == target:
            return [node]
        for nxt in edges.get(node, []):
            if nxt not in seen:
                found = path_back(nxt, target, seen | {nxt})
                if found:
                    return [node, *found]
        return None

    # A loop through this trigger: from the pipeline it starts back to the one it follows.
    loop = path_back(pipeline_uuid, after, {pipeline_uuid})
    if loop:
        problems.append(
            'Pipelines would start each other after their runs in a loop: '
            f'{" -> ".join([after, *loop])}. A run would start the next without end.'
        )
    return problems


def _active_after_run_schedules():
    from mage_ai.data_preparation.models.triggers import ScheduleStatus, ScheduleType
    from mage_ai.orchestration.db.models.schedules import PipelineSchedule

    return PipelineSchedule.repo_query.filter(
        PipelineSchedule.schedule_type == ScheduleType.AFTER_RUN,
        PipelineSchedule.status == ScheduleStatus.ACTIVE,
    ).all()


def schedule_problems(
    pipeline_uuid: str,
    settings: Optional[Dict],
    schedule_id: Optional[int] = None,
    repo_path: Optional[str] = None,
) -> List[str]:
    """trigger_problems against the project's other active after-run triggers."""
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.settings.repo import get_repo_path

    repo_path = repo_path or get_repo_path()
    others = [
        (s.pipeline_uuid, s.settings)
        for s in _active_after_run_schedules()
        if s.id != schedule_id
    ]
    return trigger_problems(
        pipeline_uuid,
        settings,
        others,
        pipeline_exists=lambda uuid: bool(
            Pipeline.get(uuid, repo_path=repo_path, check_if_exists=True)
        ),
    )


def _runs_in_this_env(schedule) -> bool:
    """
    Whether the trigger's envs in triggers.yaml include this environment. The sync leaves out
    a trigger of another environment, but one synced before its envs changed stays active in
    the database. Triggers saved only in the database have no envs and run everywhere.
    """
    from mage_ai.data_preparation.models.triggers import get_triggers_by_pipeline
    from mage_ai.shared.environments import get_env

    try:
        triggers = get_triggers_by_pipeline(schedule.pipeline_uuid, repo_path=schedule.repo_path)
    except Exception:
        return True
    for trigger in triggers:
        if trigger.name == schedule.name:
            return not trigger.envs or get_env() in trigger.envs
    return True


def start_followers(pipeline_run, status: str) -> List:
    """
    Creates a run for each active after-run trigger that follows this run's pipeline and
    ending, once per upstream run. Errors are logged, never raised: a finished run stays
    finished whatever its followers do.
    """
    try:
        return _start_followers(pipeline_run, status)
    except Exception:
        _logger().exception(
            f'After-run triggers of pipeline run {getattr(pipeline_run, "id", None)} failed.'
        )
        return []


def _start_followers(pipeline_run, status: str) -> List:
    from sqlalchemy import desc

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.orchestration.triggers.utils import create_and_start_pipeline_run
    from mage_ai.settings.repo import get_repo_path

    started = []
    schedules = _active_after_run_schedules()
    for schedule in schedules:
        if not follows(schedule.settings, pipeline_run.pipeline_uuid, status):
            continue
        if not _runs_in_this_env(schedule):
            continue
        # Triggers saved straight in triggers.yaml skip the API's checks; a loop must not
        # start runs without end.
        loop = trigger_problems(
            schedule.pipeline_uuid,
            schedule.settings,
            [(s.pipeline_uuid, s.settings) for s in schedules if s.id != schedule.id],
        )
        if loop:
            _logger().error(f'Trigger {schedule.name} of {schedule.pipeline_uuid}: {loop[0]}')
            continue
        recent = (
            PipelineRun.query.filter(PipelineRun.pipeline_schedule_id == schedule.id)
            .order_by(desc(PipelineRun.id))
            .limit(RECENT_RUNS)
            .all()
        )
        if any(
            (r.variables or {}).get('upstream_pipeline_run_id') == pipeline_run.id
            for r in recent
        ):
            continue
        if (schedule.settings or {}).get('skip_if_previous_running') and any(
            r.status in (
                PipelineRun.PipelineRunStatus.INITIAL,
                PipelineRun.PipelineRunStatus.RUNNING,
            )
            for r in recent
        ):
            _logger().info(
                f'Trigger {schedule.name} of {schedule.pipeline_uuid} started no run after '
                f'pipeline run {pipeline_run.id}: a run of it is still going.'
            )
            continue
        pipeline = Pipeline.get(
            schedule.pipeline_uuid, repo_path=schedule.repo_path or get_repo_path(),
        )
        run = create_and_start_pipeline_run(
            pipeline,
            schedule,
            dict(variables=dict(
                upstream_pipeline=pipeline_run.pipeline_uuid,
                upstream_pipeline_run_id=pipeline_run.id,
                upstream_status=status,
            )),
            should_schedule=False,
        )
        _logger().info(
            f'Pipeline run {pipeline_run.id} of {pipeline_run.pipeline_uuid} {status}; '
            f'trigger {schedule.name} created run {run.id} of {schedule.pipeline_uuid}.'
        )
        started.append(run)
    return started
