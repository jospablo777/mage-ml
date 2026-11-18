"""
The one planner (I-18, planning.py and crates/core/src/plan.rs). MageML's stages come from
it; on generated pipelines they must equal the stages of the rules up to 0.3.0, kept below
as the reference, and its findings give each place a line does not chain a reason.
"""
import random

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import compilation, fusion, planning
from mage_ai.tests.base_test import DBTestCase

LANGUAGES = ('python', 'python', 'python', 'r', 'rust', 'sql')

# Rust blocks without tests: a block with tests writes its own output and ends its segment.
RUST_LOAD = '''
use mage::prelude::*;

fn load_data() -> Result<DataFrame> {
    Ok(df!("a" => [1i64, 2, 3])?)
}
'''
RUST_TRANSFORM = '''
use mage::prelude::*;

fn transform(rows: LazyFrame) -> Result<LazyFrame> {
    Ok(rows.with_column((col("a") * lit(2)).alias("b")))
}
'''


def reference_stages(pipeline):
    """fusion_plan of 0.3.0: each block's next is its only downstream block when that block
    has no other upstream block and both can fuse."""
    def following(block):
        downstream = block.downstream_blocks or []
        if len(downstream) != 1 or len(downstream[0].upstream_blocks or []) != 1:
            return None
        if not fusion.block_can_fuse(pipeline, block) or \
                not fusion.block_can_fuse(pipeline, downstream[0]):
            return None
        return downstream[0]

    blocks = [b for b in pipeline.blocks_by_uuid.values() if fusion.executable(b)]
    continues = {f.uuid for f in (following(b) for b in blocks) if f is not None}
    stages = []
    for block in blocks:
        if block.uuid in continues:
            continue
        stage = [block.uuid]
        nxt = following(block)
        while nxt is not None:
            stage.append(nxt.uuid)
            nxt = following(nxt)
        stages.append(stage)
    return stages


class PlanningTest(DBTestCase):
    def pipeline(self, name, nodes, settings=None):
        """nodes: (language, upstream indexes) per block, in creation order."""
        settings = settings or {}
        pipeline = Pipeline.create(name, repo_path=self.repo_path)
        created = []
        for index, (language, upstream) in enumerate(nodes):
            block_type = 'data_loader' if not upstream else 'transformer'
            block = Block.create(f'{name}_{index}', block_type, self.repo_path,
                                 language=language)
            configuration = dict(settings.get(index) or {})
            if language == 'sql':
                configuration.setdefault('data_provider', 'duckdb')
                configuration.setdefault('export_write_policy', 'replace')
            if configuration:
                block.update(dict(configuration=configuration))
            if language == 'rust':
                with open(block.file_path, 'w') as file:
                    file.write(RUST_TRANSFORM if upstream else RUST_LOAD)
            pipeline.add_block(block, upstream_block_uuids=[created[u].uuid for u in upstream])
            created.append(block)
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path), [b.uuid for b in created]

    def test_stages_equal_the_rules_up_to_0_3_0_on_generated_pipelines(self):
        rng = random.Random(18)
        compared = 0
        for case in range(40):
            size = rng.randint(2, 9)
            nodes = []
            for index in range(size):
                upstream = [u for u in range(index)
                            if rng.random() < (0.9 if u == index - 1 else 0.15)]
                nodes.append((rng.choice(LANGUAGES), upstream))
            settings = {}
            for index in range(size):
                roll = rng.random()
                if roll < 0.08:
                    settings[index] = dict(disabled=True)
                elif roll < 0.14:
                    settings[index] = dict(trigger_rule='all_done')
                elif roll < 0.18:
                    settings[index] = dict(fusion=False)
            pipeline, _ = self.pipeline(f'generated_{case}', nodes, settings)
            expected = sorted(reference_stages(pipeline))
            self.assertEqual(sorted(fusion.fusion_plan(pipeline)), expected,
                             f'case {case}: {nodes} {settings}')
            compared += 1
        self.assertEqual(compared, 40)

    def test_stages_come_in_the_order_they_can_start(self):
        # Created out of order: the last block created feeds the first.
        pipeline, uuids = self.pipeline('ordered', [
            ('python', []), ('python', [0]), ('python', []),
        ])
        stages = fusion.fusion_plan(pipeline)
        position = {uuid: i for i, stage in enumerate(stages) for uuid in stage}
        for block in pipeline.blocks_by_uuid.values():
            for up in block.upstream_block_uuids:
                if position[up] != position[block.uuid]:
                    self.assertLess(position[up], position[block.uuid])

    def test_a_block_left_alone_has_its_reason(self):
        pipeline, uuids = self.pipeline('alone', [
            ('python', []), ('python', [0]), ('python', [1]),
        ], {1: dict(trigger_rule='all_done')})
        plan = planning.plan_pipeline(pipeline, groups=True)
        self.assertEqual(plan.groups, [[uuids[0]], [uuids[1]], [uuids[2]]])
        reasons = [f['reason'] for f in plan.findings if f['code'] == 'runs_alone']
        self.assertTrue(any('trigger rule (all_done)' in r for r in reasons), reasons)

    def test_compiled_rust_chains_need_one_connection_profile(self):
        # Up to 0.3.0 the run chained Rust blocks of different profiles while compiling did
        # not; the planner splits them everywhere (one process has one MAGE_IO_CONFIG).
        pipeline, uuids = self.pipeline('profiles', [
            ('rust', []), ('rust', [0]), ('rust', [1]),
        ], {2: dict(data_provider_profile='lake')})
        plan = planning.plan_pipeline(pipeline, groups=True)
        self.assertEqual([s['blocks'] for s in plan.segments], [uuids[:2]])
        self.assertTrue(plan.in_segment_with(uuids[0], uuids[1]))
        self.assertFalse(plan.in_segment_with(uuids[1], uuids[2]))
        units = [(u.kind, u.block_uuids) for u in compilation.units_for(pipeline)
                 if u.kind.startswith('rust')]
        self.assertEqual(units, [('rust_chain', uuids[:2]), ('rust_block', [uuids[2]])])

    def test_the_compile_summary_carries_segments_and_findings(self):
        pipeline, uuids = self.pipeline('summary', [
            ('sql', []), ('sql', [0]), ('python', [1]),
        ])
        summary = compilation.summary_for(pipeline)
        self.assertEqual(summary['chains'][0]['blocks'], uuids)
        self.assertEqual(summary['segments'], [dict(kind='sql', blocks=uuids[:2])])
        self.assertEqual(summary['sql_chains'], [dict(blocks=uuids[:2], views=[uuids[0]])])
        self.assertTrue(any(f['code'] == 'language_switch' for f in summary['findings']))
