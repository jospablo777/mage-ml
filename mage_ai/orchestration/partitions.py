"""
Partitions of a pipeline (ADR-0039, do_not_commit/mage_modernization_plan/PARTITIONS.md): the
slices of its work that run and are tracked on their own, one day, one hour, one region.

The pipeline declares them in its metadata.yaml:

    partitions:
      key: day                  # the variable each partition's run gets
      daily:                    # or hourly, weekly, monthly
        start: 2026-01-01
        end: null               # optional
        timezone: UTC           # where days, weeks and months begin
        close_after: 2h         # a partition closes this long after its end (late data)
      upstream: [orders_raw]    # optional: pipelines with the same partitions it reads
      schedule: on_close        # optional: run each partition when it closes
      catch_up: 30              # at most this many closed partitions queued at once

or fixed values:

    partitions:
      key: region
      values: [eu, us, apac]

This module parses the declaration and names the partitions: `2026-11-01T13` (hourly, in UTC:
a local hour can happen twice, or not at all, on a daylight saving day), `2026-11-01` (daily),
`2026-11-02` (weekly, the Monday it starts), `2026-11` (monthly), or the listed value.
"""
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

KINDS = ('hourly', 'daily', 'weekly', 'monthly', 'values')
_KEY = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,63}$')
_DURATION = re.compile(r'^\s*(\d+)\s*([smhd]?)\s*$')
_SECONDS = dict(s=1, m=60, h=3600, d=86400)
_VALUE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')
# Expected partitions listed at most: an hourly pipeline over ten years.
MAX_PARTITIONS = 100_000


class PartitionError(ValueError):
    pass


@dataclass(frozen=True)
class Declaration:
    key: str
    kind: str
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    timezone: str = 'UTC'
    close_after: timedelta = timedelta(0)
    values: List[str] = field(default_factory=list)
    upstream: List[str] = field(default_factory=list)
    # 'on_close': the scheduler queues each partition when it closes; 'on_upstream': when its
    # upstream pipelines' same partition is done (partition_schedule.py).
    schedule: Optional[str] = None
    catch_up: int = 30

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo('UTC') if self.kind == 'hourly' else ZoneInfo(self.timezone)


def _duration(value: Any, where: str) -> timedelta:
    if value in (None, '', 0):
        return timedelta(0)
    if isinstance(value, bool):
        raise PartitionError(f'{where} is a duration such as 2h, 30m or 1d.')
    if isinstance(value, (int, float)):
        return timedelta(seconds=value)
    match = _DURATION.match(str(value))
    if not match:
        raise PartitionError(f'{where} is a duration such as 2h, 30m or 1d, not {value!r}.')
    return timedelta(seconds=int(match.group(1)) * _SECONDS[match.group(2) or 's'])


def _moment(value: Any, zone: ZoneInfo, where: str) -> Optional[datetime]:
    """A start or end: a date (its midnight there) or a date and time, as an aware time."""
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, date):
        moment = datetime.combine(value, time())
    else:
        try:
            moment = datetime.fromisoformat(str(value))
        except ValueError:
            raise PartitionError(
                f'{where} is a date such as 2026-01-01, or a date and time, not {value!r}.'
            ) from None
    return moment if moment.tzinfo else moment.replace(tzinfo=zone)


def parse(document: Any) -> Optional[Declaration]:
    """The declaration in a pipeline's metadata (its `partitions`), or None without one."""
    if document in (None, {}):
        return None
    if not isinstance(document, dict):
        raise PartitionError('partitions is a mapping with key and one of '
                             f'{", ".join(KINDS)}.')
    key = document.get('key')
    if not isinstance(key, str) or not _KEY.match(key):
        raise PartitionError(
            'partitions: key is the name of the variable each partition run gets: letters, '
            'digits and _, such as day or region.'
        )
    kinds = [kind for kind in KINDS if kind in document]
    if len(kinds) != 1:
        raise PartitionError(
            f'partitions: give one of {", ".join(KINDS)}'
            + (f'; {" and ".join(kinds)} were given.' if kinds else '.')
        )
    unknown = set(document) - set(KINDS) - {'key', 'upstream', 'schedule', 'catch_up'}
    if unknown:
        raise PartitionError(f'partitions: unknown settings {", ".join(sorted(unknown))}.')
    upstream = document.get('upstream') or []
    if isinstance(upstream, str):
        upstream = [upstream]
    if not isinstance(upstream, list) or not all(isinstance(u, str) and u for u in upstream):
        raise PartitionError('partitions: upstream is a list of pipeline UUIDs.')
    schedule = document.get('schedule')
    if schedule not in (None, 'on_close', 'on_upstream'):
        raise PartitionError('partitions: schedule is on_close (each partition when it '
                             'closes), on_upstream (when its upstream pipelines\' same '
                             'partition is done), or left out to run them when asked.')
    if schedule == 'on_upstream' and not upstream:
        raise PartitionError('partitions: schedule: on_upstream needs upstream, the pipelines '
                             'whose partitions it follows.')
    catch_up = document.get('catch_up', 30)
    if isinstance(catch_up, bool) or not isinstance(catch_up, int) or catch_up < 1:
        raise PartitionError('partitions: catch_up is how many closed partitions are queued '
                             'at once, 1 or more.')
    kind = kinds[0]
    if kind == 'values' and schedule == 'on_close':
        raise PartitionError('partitions: schedule: on_close is for time partitions; values '
                             'do not close.')
    if kind == 'values':
        values = document['values']
        if not isinstance(values, list) or not values:
            raise PartitionError('partitions: values is a list, such as [eu, us, apac].')
        values = [str(v) for v in values]
        bad = [v for v in values if not _VALUE.match(v)]
        if bad:
            raise PartitionError(
                f'partitions: a value is letters, digits, _, . and -, not {bad[0]!r}.')
        if len(set(values)) != len(values):
            raise PartitionError('partitions: each value is listed once.')
        return Declaration(key=key, kind=kind, values=values, upstream=list(upstream),
                           schedule=schedule, catch_up=catch_up)
    settings = document[kind] or {}
    if not isinstance(settings, dict):
        raise PartitionError(f'partitions: {kind} is a mapping with start.')
    unknown = set(settings) - {'start', 'end', 'timezone', 'close_after'}
    if unknown:
        raise PartitionError(
            f'partitions: {kind} has unknown settings {", ".join(sorted(unknown))}.')
    zone_name = settings.get('timezone') or 'UTC'
    try:
        zone = ZoneInfo(str(zone_name))
    except (ZoneInfoNotFoundError, ValueError):
        raise PartitionError(
            f'partitions: {kind}.timezone is a time zone such as UTC or America/Costa_Rica, '
            f'not {zone_name!r}.'
        ) from None
    if kind == 'hourly':
        zone = ZoneInfo('UTC')
    start = _moment(settings.get('start'), zone, f'partitions: {kind}.start')
    if start is None:
        raise PartitionError(f'partitions: {kind}.start is the first partition\'s date.')
    end = _moment(settings.get('end'), zone, f'partitions: {kind}.end')
    if end is not None and end <= start:
        raise PartitionError(f'partitions: {kind}.end is after its start.')
    declaration = Declaration(
        key=key, kind=kind, start=start, end=end, timezone=str(zone_name),
        close_after=_duration(settings.get('close_after'), f'partitions: {kind}.close_after'),
        upstream=list(upstream), schedule=schedule, catch_up=catch_up,
    )
    # Starts on a partition's boundary: a daily start at 13:00 would name days by noon.
    first = begin(declaration, value_of(declaration, start))
    if first != start.astimezone(declaration.zone):
        raise PartitionError(
            f'partitions: {kind}.start is where a partition begins, such as '
            f'{value_of(declaration, start)}; {start.isoformat()} is inside one.'
        )
    return declaration


def value_of(declaration: Declaration, moment: datetime) -> str:
    """The partition that holds `moment` (an aware time)."""
    if declaration.kind == 'values':
        raise PartitionError('Value partitions are not times.')
    local = moment.astimezone(declaration.zone)
    if declaration.kind == 'hourly':
        return local.strftime('%Y-%m-%dT%H')
    if declaration.kind == 'daily':
        return local.strftime('%Y-%m-%d')
    if declaration.kind == 'weekly':
        return (local.date() - timedelta(days=local.weekday())).isoformat()
    return local.strftime('%Y-%m')


def begin(declaration: Declaration, value: str) -> datetime:
    """Where the partition named `value` begins, in its zone."""
    zone = declaration.zone
    try:
        if declaration.kind == 'hourly':
            return datetime.strptime(value, '%Y-%m-%dT%H').replace(tzinfo=zone)
        if declaration.kind in ('daily', 'weekly'):
            day = date.fromisoformat(value)
            if declaration.kind == 'weekly' and day.weekday() != 0:
                raise ValueError
            return datetime.combine(day, time(), tzinfo=zone)
        if declaration.kind == 'monthly':
            return datetime.strptime(value, '%Y-%m').replace(tzinfo=zone)
    except ValueError:
        pass
    raise PartitionError(f'{value!r} is not a {declaration.kind} partition of '
                         f'{declaration.key}.')


def end_of(declaration: Declaration, value: str) -> datetime:
    """Where the partition ends: where the next begins (a local day can be 23 or 25 hours)."""
    start = begin(declaration, value)
    if declaration.kind == 'hourly':
        return start + timedelta(hours=1)
    if declaration.kind == 'daily':
        following = start.date() + timedelta(days=1)
    elif declaration.kind == 'weekly':
        following = start.date() + timedelta(days=7)
    else:
        month = start.month % 12 + 1
        following = date(start.year + (start.month == 12), month, 1)
    return datetime.combine(following, time(), tzinfo=start.tzinfo)


def expected(declaration: Declaration, now: Optional[datetime] = None) -> List[str]:
    """The partitions that exist by `now`: every listed value, or every closed time partition
    from the start (a partition closes `close_after` after its end)."""
    if declaration.kind == 'values':
        return list(declaration.values)
    now = now or datetime.now(timezone.utc)
    values = []
    value = value_of(declaration, declaration.start)
    while len(values) < MAX_PARTITIONS:
        finish = end_of(declaration, value)
        if declaration.end is not None and begin(declaration, value) >= declaration.end:
            break
        # In UTC: adding to a local time adds wall-clock time, an hour off across a clock
        # change.
        if finish.astimezone(timezone.utc) + declaration.close_after > now:
            break
        values.append(value)
        value = value_of(declaration, finish)
    return values


def folder(value: str) -> str:
    """A partition's execution partition: one output folder whichever trigger runs it."""
    return f'partitions/{value}'


def run_variables(declaration: Declaration, value: str) -> Dict[str, Any]:
    """The variables of a partition's run: its key and its output folder."""
    if declaration.kind == 'values':
        if value not in declaration.values:
            raise PartitionError(f'{value!r} is not one of the values of {declaration.key}: '
                                 f'{", ".join(declaration.values)}.')
    else:
        begin(declaration, value)
    return {declaration.key: value, 'execution_partition': folder(value)}


def run_start(declaration: Declaration, value: str) -> Optional[datetime]:
    """A partition run's execution_date: its partition's start in UTC; None for values."""
    if declaration.kind == 'values':
        return None
    return begin(declaration, value).astimezone(timezone.utc)
