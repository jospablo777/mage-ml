"""
Failures in generated pipelines (library.py): a block that raises, returns no rows, or
returns a table without a column its consumers read, in each language, on a chain and on a
diamond, block by block and fused.

The contract checked:
- a block that raises fails, and so does a consumer that reads a missing column;
- the blocks after a failed block do not complete;
- blocks that do not depend on it complete;
- empty tables flow through: every block completes with the oracle's (empty) output;
- block by block and fused report the same status for every block before the failure.

MAGE_STUDY_FULL=1 runs every language and shape; by default pandas and Rust on the chain.
MAGE_STUDY_RESULTS_DIR keeps the results (provenance.py).
"""
import os
import shutil

import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

CASES = [
    # (topology, upstream lists, faulty node)
    ('chain5', lib.chain(5), 2),
    ('diamond', lib.diamond(), 1),
]
STATUS_OF_MODE = {}


@pytest.fixture
def ready(monkeypatch, tmp_path):
    def check(top):
        used = {n.language for n in top.nodes}
        if 'rust' in used and shutil.which('cargo') is None:
            pytest.skip('Rust is not installed')
        if 'r' in used:
            path = os.getenv('MAGE_TEST_R_PROJECT_DIR')
            if not path or shutil.which('Rscript') is None:
                pytest.skip('R is not configured: MAGE_TEST_R_PROJECT_DIR unset')
            monkeypatch.setenv('MAGE_R_PROJECT_DIR', path)
            monkeypatch.setenv('MAGE_R_CACHE_DIR', str(tmp_path / 'mageml'))
    return check


FULL = bool(os.getenv('MAGE_STUDY_FULL'))
LANGUAGES = lib.LANGUAGES if FULL else ('pandas', 'rust')
SHAPES = CASES if FULL else CASES[:1]


@pytest.mark.parametrize('fault', lib.FAULTS)
@pytest.mark.parametrize('language', LANGUAGES)
@pytest.mark.parametrize('name,upstream,faulty', SHAPES, ids=[c[0] for c in SHAPES])
def test_fault(mage_project, ready, name, upstream, faulty, language, fault):
    top = lib.topology(name, upstream, language, faults={faulty: fault})
    ready(top)
    pipeline, uuids = lib.build(mage_project, top, 500, prefix='fault')
    by_index = {uuid: index for index, uuid in uuids.items()}
    after = set(lib.descendants(top, faulty))
    # A missing column fails the blocks that read it: the faulty block's consumers.
    failing = {faulty} if fault == 'raise' else (
        {n.index for n in top.nodes if faulty in n.upstream} if fault == 'missing_column'
        else set())
    unaffected = {n.index for n in top.nodes} - after - {faulty}
    seen = {}
    for mode in ('off', 'chains'):
        before = provenance.machine_snapshot()
        pipeline_run, seconds = lib.run(pipeline, mode)
        states = {by_index[b]: s for b, s in lib.statuses(pipeline_run).items()}
        seen[mode] = states
        provenance.record('faults', dict(topology=name, language=language, fault=fault,
                    mode=mode, faulty=faulty, statuses=states, seconds=round(seconds, 3),
                    run_status=getattr(pipeline_run.status, 'value', pipeline_run.status),
                    pipeline=pipeline.uuid), before)
        for index in unaffected:
            assert states.get(index) == 'completed', (mode, index, states)
        if fault == 'empty':
            assert all(s == 'completed' for s in states.values()), (mode, states)
            expected = lib.oracle(top, 500)
            for leaf in top.leaves:
                actual = lib.stored(pipeline, uuids[leaf.index], pipeline_run)
                assert lib.differences(expected[leaf.index], actual) is None, (mode, leaf)
            continue
        for index in failing:
            assert states.get(index) == 'failed', (mode, index, states)
        for index in after - failing:
            assert states.get(index) != 'completed', (mode, index, states)
    # Blocks after a failure never complete; every other block has the same status in both
    # modes. After a failure the statuses may differ by design: the block that ran a
    # compiled chain fails with an error that names the block that failed in it.
    for index in set(seen['off']) - after:
        assert seen['off'][index] == seen['chains'].get(index), (index, seen)
