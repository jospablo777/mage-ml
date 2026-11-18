"""
Which blocks of a pipeline run together, from the one planner (I-18): the planner of
mage-service-core (crates/core/src/plan.rs), reached through column_atlas_native. MageML's
scheduler, `mage run`, the Compile panel and exported services all plan with it, so a
pipeline is chained one way everywhere.

This module only describes the blocks: for each, what the planner cannot know from the
graph, such as why a block must run alone or must write its own output, as a reason the
Compile panel can show. Every decision is the planner's.
"""
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

LANGUAGES = ('python', 'r', 'rust', 'sql')


@dataclass(frozen=True)
class Plan:
    groups: List[List[str]]
    segments: List[Dict] = field(default_factory=list)
    findings: List[Dict] = field(default_factory=list)
    order: List[str] = field(default_factory=list)

    def group_of(self, uuid: str) -> Optional[List[str]]:
        return next((g for g in self.groups if uuid in g), None)

    def next_in_group(self, uuid: str) -> Optional[str]:
        group = self.group_of(uuid) or []
        position = group.index(uuid) if uuid in group else -1
        return group[position + 1] if 0 <= position < len(group) - 1 else None

    def segment_of(self, uuid: str) -> Optional[Dict]:
        return next((s for s in self.segments if uuid in s['blocks']), None)

    def in_segment_with(self, uuid: str, following: str) -> bool:
        segment = self.segment_of(uuid)
        return segment is not None and following in segment['blocks']


def _value(enum_or_str) -> str:
    return str(getattr(enum_or_str, 'value', enum_or_str) or '')


def _has_tests(block) -> bool:
    from mage_ai.orchestration import compilation

    language = _value(block.language)
    if language == 'rust':
        return compilation._rust_tests(block)
    code = compilation._code(block)
    return '@test' in code


def block_facts(pipeline, block, executable_uuids, segments: bool = True) -> Dict:
    """What the planner needs to know about one block of a MageML run. Without segments,
    only what grouping needs: the scheduler plans on every pass, and segment facts read
    each block's contract and monitors."""
    from mage_ai.data_preparation import environments
    from mage_ai.data_preparation.models.block import plans
    from mage_ai.orchestration import fusion

    language = _value(block.language)
    configuration = block.configuration or {}
    facts = dict(
        uuid=block.uuid,
        language=language if language in LANGUAGES else 'other',
        upstream=[u for u in (block.upstream_block_uuids or []) if u in executable_uuids],
        alone=fusion.alone_reason(pipeline, block),
    )
    if language == 'python':
        worker = environments.worker_selection(block)
        if worker not in (None, 'mage'):
            facts['worker'] = worker
    if not segments:
        return facts
    if language in ('rust', 'sql'):
        # A run's Rust process and a SQL block's connection use the default profile when
        # the block names none.
        facts['profile'] = configuration.get('data_provider_profile') or 'default'
    if language == 'sql':
        facts['provider'] = configuration.get('data_provider')
        facts['view_problem'] = plans.sql_view_problem(block)
    if language in ('rust', 'r', 'sql'):
        facts['output_needed'] = plans.output_needed_reason(block, _has_tests(block))
    return facts


def plan_pipeline(pipeline, groups: Optional[bool] = None, segments: bool = True) -> Plan:
    """The plan of a MageML run of the pipeline as saved. groups defaults to whether block
    fusion is on for the pipeline; segments=False plans groups only, cheaply."""
    import os

    import column_atlas_native

    from mage_ai.orchestration import fusion

    blocks = [b for b in pipeline.blocks_by_uuid.values() if fusion.executable(b)]
    uuids = {b.uuid for b in blocks}
    options = dict(
        target='mageml',
        groups=fusion.fusion_enabled(pipeline) if groups is None else groups,
        segments=segments and os.getenv('MAGE_COMPILED_CHAINS') != 'off',
    )
    payload = json.dumps(dict(
        blocks=[block_facts(pipeline, b, uuids, segments) for b in blocks],
        options=options,
    ))
    result = json.loads(column_atlas_native.plan_pipeline(payload))
    return Plan(
        groups=result['groups'],
        segments=result['segments'],
        findings=result['findings'],
        order=result['order'],
    )
