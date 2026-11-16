"""
Trigger rules (upstream #4704): when a block runs, given how its upstream blocks ended.
A block's rule is configuration.trigger_rule.

all_success (default): every upstream block completed. A failed upstream block makes the
    block upstream failed; a skipped one (false condition, disabled) skips it.
none_failed_min_one_success: no upstream block failed and at least one completed; the
    others were skipped. A join after branches of which some were skipped.
all_done: every upstream block finished, however it ended. Cleanup and alerts.

An upstream block that did not complete gives the block None as its input. A failed
block still fails the run: the run fails once no other block can run. A pipeline with an
all_done block keeps running after a failure, as with allow_blocks_to_fail, so that block
runs.
"""
from enum import StrEnum
from typing import Dict, Iterable, List, Optional

CONFIG_KEY_TRIGGER_RULE = 'trigger_rule'


class TriggerRule(StrEnum):
    ALL_SUCCESS = 'all_success'
    NONE_FAILED_MIN_ONE_SUCCESS = 'none_failed_min_one_success'
    ALL_DONE = 'all_done'


class Decision(StrEnum):
    RUN = 'run'
    WAIT = 'wait'
    # As BlockRun.BlockRunStatus names them.
    UPSTREAM_FAILED = 'upstream_failed'
    CONDITION_FAILED = 'condition_failed'


COMPLETED = 'completed'
FAILED = ('failed', 'upstream_failed')
SKIPPED = 'condition_failed'


def block_trigger_rule(block) -> TriggerRule:
    value = ((getattr(block, 'configuration', None) or {}).get(CONFIG_KEY_TRIGGER_RULE))
    try:
        return TriggerRule(value) if value else TriggerRule.ALL_SUCCESS
    except ValueError:
        return TriggerRule.ALL_SUCCESS


def has_trigger_rule(block) -> bool:
    return block_trigger_rule(block) != TriggerRule.ALL_SUCCESS


def applies(block) -> bool:
    """Whether a run decides the block by its rule: a block with a rule other than the
    default, not dynamic, replicated or data integration, nor after such a block, whose
    runs read upstream block runs of their own."""
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
        should_reduce_output,
    )

    if block is None or not has_trigger_rule(block):
        return False
    for b in [block] + list(block.upstream_blocks or []):
        if (
            is_dynamic_block(b)
            or is_dynamic_block_child(b)
            or should_reduce_output(b)
            or is_replicated_block(b)
            or b.is_data_integration()
        ):
            return False
    return True


def decide(rule: TriggerRule, upstream_statuses: Iterable[Optional[str]]) -> Decision:
    """
    upstream_statuses: each upstream block's status in the run, None when it has none yet.
    """
    statuses = [str(s) if s is not None else None for s in upstream_statuses]
    if not statuses:
        return Decision.RUN
    failed = any(s in FAILED for s in statuses)
    skipped = [s == SKIPPED for s in statuses]
    finished = all(s == COMPLETED or s == SKIPPED or s in FAILED for s in statuses)

    if rule == TriggerRule.ALL_DONE:
        return Decision.RUN if finished else Decision.WAIT
    if failed:
        return Decision.UPSTREAM_FAILED
    if rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS:
        if all(skipped):
            return Decision.CONDITION_FAILED
        return Decision.RUN if finished else Decision.WAIT
    if any(skipped):
        return Decision.CONDITION_FAILED
    return Decision.RUN if finished else Decision.WAIT


def missing_upstream_outputs(block, pipeline_run) -> Dict[str, List]:
    """The upstream blocks of a block decided by its rule that did not complete in the
    run, each with None as its output."""
    if pipeline_run is None or not applies(block):
        return {}
    status_by_block_uuid = {b.block_uuid: b.status for b in pipeline_run.block_runs}
    return {
        uuid: [None] for uuid in block.upstream_block_uuids
        if status_by_block_uuid.get(uuid) != COMPLETED
    }


def continues_after_failure(pipeline) -> bool:
    """Whether a failed block leaves the run going: an all_done block runs after it."""
    blocks = (getattr(pipeline, 'blocks_by_uuid', None) or {}).values()
    return any(block_trigger_rule(b) == TriggerRule.ALL_DONE for b in blocks)


def validation_error(pipeline, block, value) -> Optional[str]:
    """Why the block cannot have this trigger rule, or None."""
    from mage_ai.data_preparation.models.block.dynamic.utils import (
        is_dynamic_block,
        is_dynamic_block_child,
        is_replicated_block,
    )
    from mage_ai.data_preparation.models.constants import (
        BlockLanguage,
        PipelineType,
    )

    if value in (None, '', TriggerRule.ALL_SUCCESS):
        return None
    if value not in [r.value for r in TriggerRule]:
        return (
            f'{value!r} is not a trigger rule. The rules: '
            + ', '.join(r.value for r in TriggerRule) + '.'
        )
    if pipeline is not None and pipeline.type != PipelineType.PYTHON:
        return 'Trigger rules apply to the blocks of batch pipelines.'
    if block.language != BlockLanguage.PYTHON:
        return 'Trigger rules apply to Python blocks: they get None for an upstream block ' \
            'that did not complete.'
    if (
        is_dynamic_block(block)
        or is_dynamic_block_child(block)
        or is_replicated_block(block)
        or block.is_data_integration()
    ):
        return 'Trigger rules do not apply to dynamic, replicated or data integration blocks.'
    return None
