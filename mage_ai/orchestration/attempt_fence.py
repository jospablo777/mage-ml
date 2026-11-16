"""
Stale outputs (I-277). A block run's worker holds the attempt it claimed. When the block
run is reset, times out, is cancelled or is claimed again, its attempt changes, and the
worker that still runs the old attempt must not write the block's outputs over those of
the new one. Before each output write the worker checks, in one query, that its block run
is RUNNING with its attempt; a superseded worker raises SupersededAttempt, which is not
retried, and its failure is discarded by the fenced status updates.

The check and the write are not atomic: a worker superseded during the write itself still
writes. That window is the length of one write, where it was the rest of the block's run.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Optional, Tuple

_held: ContextVar[Optional[Tuple[int, int]]] = ContextVar('mage_block_run_attempt', default=None)


class SupersededAttempt(Exception):
    # mage_ai.shared.retry does not retry it: every attempt would be refused.
    retryable = False


@contextmanager
def holding(block_run_id: Optional[int], attempt: Optional[int]):
    """Output writes in this context check the block run's attempt first."""
    if block_run_id is None or attempt is None:
        yield
        return
    token = _held.set((block_run_id, attempt))
    try:
        yield
    finally:
        _held.reset(token)


def current_attempt(block_run_id: int, attempt: int) -> bool:
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.models.schedules import BlockRun

    row = db_connection.session.query(BlockRun.attempt, BlockRun.status).filter(
        BlockRun.id == block_run_id,
    ).first()
    db_connection.session.commit()
    return row is not None and row[0] == attempt and row[1] == BlockRun.BlockRunStatus.RUNNING


def check() -> None:
    """Raises SupersededAttempt when the attempt this context holds is no longer current."""
    held = _held.get()
    if held is None:
        return
    block_run_id, attempt = held
    if not current_attempt(block_run_id, attempt):
        raise SupersededAttempt(
            f'BlockRun {block_run_id} was claimed again or reset after attempt {attempt} '
            'started; this attempt does not write its outputs.'
        )
