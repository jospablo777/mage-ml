import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import dateutil.parser
from dateutil.relativedelta import relativedelta

from mage_ai.data_preparation.models.triggers import ScheduleInterval, ScheduleStatus
from mage_ai.orchestration import partitions
from mage_ai.orchestration.db.models.schedules import (
    Backfill,
    PipelineRun,
    PipelineSchedule,
)
from mage_ai.shared.dates import utc_now
from mage_ai.shared.hash import merge_dict

MAX_RUNS_ENV = 'MAGE_BACKFILL_MAX_RUNS'
DEFAULT_MAX_RUNS = 100000


class BackfillError(ValueError):
    pass


def max_runs() -> int:
    try:
        return max(int(os.getenv(MAX_RUNS_ENV) or DEFAULT_MAX_RUNS), 1)
    except ValueError:
        return DEFAULT_MAX_RUNS


def start_backfill(backfill: Backfill) -> List[PipelineRun]:
    pipeline_runs = []
    variables_list = __build_variables_list(backfill)

    if len(variables_list) == 0:
        return []

    backfill_variables = backfill.variables

    pipeline_schedule = backfill.pipeline_schedule
    if not pipeline_schedule:
        pipeline_schedule = PipelineSchedule.create(
            name=f'Backfill {backfill.name}',
            pipeline_uuid=backfill.pipeline_uuid,
            schedule_interval=ScheduleInterval.ONCE,
            start_time=utc_now(),
            variables=backfill_variables,
            settings=backfill.settings,
        )

    for backfill_run_variables in _by_partition(backfill.pipeline_uuid, variables_list):
        backfill_run_variables = dict(backfill_run_variables)
        partition = backfill_run_variables.pop('partition', None)
        execution_date = None
        if 'execution_date' in backfill_run_variables:
            execution_date = dateutil.parser.parse(backfill_run_variables['execution_date'])

        run = dict(
            backfill_id=backfill.id,
            execution_date=execution_date,
            pipeline_schedule_id=pipeline_schedule.id,
            pipeline_uuid=backfill.pipeline_uuid,
            variables=merge_dict(backfill_variables or {}, backfill_run_variables),
        )
        if partition is not None:
            run['partition'] = partition
        pipeline_run = PipelineRun.create(**run)
        pipeline_runs.append(pipeline_run)

    backfill.update(pipeline_schedule_id=pipeline_schedule.id)
    pipeline_schedule.update(status=ScheduleStatus.ACTIVE)

    return pipeline_runs


def _time_partitions(pipeline_uuid: str):
    """The pipeline's time partitions declaration, or None (no partitions, values, or a
    declaration that does not parse: the backfill then runs dates as before)."""
    from mage_ai.data_preparation.models.pipeline import Pipeline

    try:
        pipeline = Pipeline.get(pipeline_uuid, check_if_exists=True)
        declaration = pipeline.partition_declaration if pipeline else None
    except Exception:
        return None
    if declaration is None or declaration.kind == 'values':
        return None
    return declaration


def _by_partition(pipeline_uuid: str, variables_list: List[Dict]) -> List[Dict]:
    """
    For a pipeline with time partitions, the backfill's dates as the partitions holding them,
    once each and without those with a run waiting or running (ADR-0039): an hourly backfill
    over three days of a daily pipeline is three runs. Each gets its partition, its partition
    variables, and execution_date, ds, hr and the interval variables of the partition. Other
    pipelines' dates are returned as they are.
    """
    declaration = _time_partitions(pipeline_uuid)
    if declaration is None:
        return variables_list
    arr = []
    seen = set()
    for item in variables_list:
        if 'execution_date' not in item:
            continue
        moment = dateutil.parser.parse(item['execution_date'])
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        value = partitions.value_of(declaration, moment)
        if value in seen:
            continue
        seen.add(value)
        if _active_partition_run(pipeline_uuid, value):
            continue
        start = partitions.run_start(declaration, value)
        end = partitions.end_of(declaration, value).astimezone(timezone.utc)
        seconds = (end - start).total_seconds()
        row = dict(
            item,
            ds=start.strftime('%Y-%m-%d'),
            execution_date=start.isoformat(),
            hr=start.strftime('%H'),
            interval_end_datetime=end.isoformat(),
            interval_seconds=seconds,
            interval_start_datetime=start.isoformat(),
            interval_start_datetime_previous=(start - timedelta(seconds=seconds)).isoformat(),
            partition=value,
        )
        # The key last: a partition keyed `ds` gets its value.
        row.update(partitions.run_variables(declaration, value))
        arr.append(row)
    return arr


def _active_partition_run(pipeline_uuid: str, value: str) -> bool:
    return PipelineRun.query.filter(
        PipelineRun.pipeline_uuid == pipeline_uuid,
        PipelineRun.partition == value,
        PipelineRun.status.in_([
            PipelineRun.PipelineRunStatus.INITIAL,
            PipelineRun.PipelineRunStatus.RUNNING,
        ]),
    ).first() is not None


def preview_run_dates(backfill: Backfill) -> List[Dict]:
    return _by_partition(backfill.pipeline_uuid, __build_variables_list(backfill))


def cancel_backfill(backfill: Backfill) -> None:
    for pipeline_run in PipelineRun.query.filter(
        PipelineRun.backfill_id == backfill.id,
        PipelineRun.status.not_in([
            PipelineRun.PipelineRunStatus.CANCELLED,
            PipelineRun.PipelineRunStatus.COMPLETED,
            PipelineRun.PipelineRunStatus.FAILED,
        ]),
    ):
        pipeline_run.update(status=PipelineRun.PipelineRunStatus.CANCELLED)

    backfill.update(status=Backfill.Status.CANCELLED)
    backfill.pipeline_schedule.update(status=ScheduleStatus.INACTIVE)


def __build_variables_list(backfill: Backfill) -> List[Dict]:
    if backfill.block_uuid:
        return []

    dates = __build_dates(backfill)
    number_of_dates = len(dates)

    arr = []

    for idx, execution_date in enumerate(dates):
        interval_end_datetime = None
        interval_seconds = None
        interval_start_datetime = execution_date
        interval_start_datetime_previous = None

        if idx < number_of_dates - 1:
            interval_end_datetime = dates[idx + 1]
            interval_seconds = (
                interval_end_datetime.timestamp() - interval_start_datetime.timestamp()
            )
        elif idx >= 1 and idx == number_of_dates - 1:
            # Last date
            interval_start_datetime_previous = dates[idx - 1]
            interval_seconds = (
                interval_start_datetime.timestamp() - interval_start_datetime_previous.timestamp()
            )
            interval_end_datetime = interval_start_datetime + timedelta(seconds=interval_seconds)

        if not interval_start_datetime_previous and (interval_seconds and interval_start_datetime):
            interval_start_datetime_previous = interval_start_datetime - timedelta(
                seconds=interval_seconds,
            )

        if interval_end_datetime:
            interval_end_datetime = interval_end_datetime.isoformat()

        if interval_start_datetime:
            interval_start_datetime = interval_start_datetime.isoformat()

        if interval_start_datetime_previous:
            interval_start_datetime_previous = interval_start_datetime_previous.isoformat()

        arr.append(dict(
            ds=execution_date.strftime('%Y-%m-%d'),
            execution_date=execution_date.isoformat(),
            hr=execution_date.strftime('%H'),
            interval_end_datetime=interval_end_datetime,
            interval_seconds=interval_seconds,
            interval_start_datetime=interval_start_datetime,
            interval_start_datetime_previous=interval_start_datetime_previous,
        ))

    return arr


def __build_dates(backfill: Backfill) -> List[datetime]:
    """
    The execution dates from start to end, every interval_units of interval_type. Months
    and years count from the start date: adding a month to the date before drifted, so a
    backfill from January 31 ran on February 28, then March 28.
    """
    units = backfill.interval_units
    if not isinstance(units, int) or isinstance(units, bool) or units <= 0:
        # Zero or a negative number never reached the end date: building the dates never
        # finished, and the backfill's page hung the server.
        raise BackfillError(
            f'The interval units must be a positive whole number; they are {units!r}.',
        )
    interval_type = backfill.interval_type
    limit = max_runs()
    dates = []
    step = 0
    current_datetime = backfill.start_datetime
    while current_datetime <= backfill.end_datetime:
        if len(dates) >= limit:
            raise BackfillError(
                f'The backfill would start more than {limit} runs; choose a shorter period '
                f'or a longer interval, or raise {MAX_RUNS_ENV}.',
            )
        dates.append(current_datetime)
        step += 1
        if interval_type in [Backfill.IntervalType.MONTH, Backfill.IntervalType.YEAR]:
            key = 'months' if Backfill.IntervalType.MONTH == interval_type else 'years'
            current_datetime = backfill.start_datetime + relativedelta(**{key: step * units})
        else:
            unit = 'seconds' if Backfill.IntervalType.CUSTOM == interval_type \
                else f'{interval_type.value}s'
            current_datetime = backfill.start_datetime + timedelta(**{unit: step * units})

    return dates
