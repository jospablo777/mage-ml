"""
Correctness of generated pipelines (library.py): every topology in every language, mixed
languages on a set of topologies, and empty and one-row inputs. Each pipeline runs block
by block and fused; every block must complete and every leaf must equal the pandas oracle.

MAGE_STUDY_FULL=1 runs every case; MAGE_STUDY_RESULTS_DIR keeps the results (provenance.py).
"""
import os
import shutil
import zlib

import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

TOPOLOGIES = {
    'chain1': lib.chain(1),
    'chain2': lib.chain(2),
    'chain3': lib.chain(3),
    'chain5': lib.chain(5),
    'chain8': lib.chain(8),
    'chains2x3': lib.chains(2, 3),
    'chains4x3': lib.chains(4, 3),
    'diamond': lib.diamond(),
    'fan_out3': lib.fan_out(3),
    'fan_in3': lib.fan_in(3),
    'lattice3x2': lib.lattice(3, 2),
    'random8a': lib.random_dag(8, 0.35, 1),
    'random8b': lib.random_dag(8, 0.35, 2),
}
MIXED_ON = ['chain5', 'diamond', 'fan_in3', 'lattice3x2', 'random8a']
MIXES = [
    'alternate:pandas,rust', 'alternate:pandas,r', 'alternate:rust,r',
    'alternate:polars,rust', 'alternate:pandas,polars', 'islands:2:pandas,rust',
    'islands:2:rust,r', 'islands:2:pandas,rust,r',
]

FULL_CASES = (
    [(t, lang, 1000) for t in TOPOLOGIES for lang in lib.LANGUAGES]
    + [(t, mix, 1000) for t in MIXED_ON for mix in MIXES]
    + [('chain3', lang, rows) for lang in lib.LANGUAGES for rows in (0, 1)]
)
# The default is a few minutes for make ci: each language once, two mixes, the shapes
# that are not chains, and an empty input. MAGE_STUDY_FULL=1 runs every case.
SMALL_CASES = (
    [('chain3', lang, 1000) for lang in lib.LANGUAGES]
    + [('diamond', 'alternate:pandas,rust', 1000), ('lattice3x2', 'islands:2:pandas,rust,r', 1000),
       ('fan_in3', 'polars', 1000), ('random8a', 'pandas', 1000), ('chain3', 'rust', 0)]
)
CASES = FULL_CASES if os.getenv('MAGE_STUDY_FULL') else SMALL_CASES


@pytest.fixture
def languages_available(monkeypatch, tmp_path):
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


@pytest.mark.parametrize('mode', ['off', 'chains'])
@pytest.mark.parametrize('name,pattern,rows', CASES, ids=lambda v: str(v))
def test_generated_pipeline(mage_project, languages_available, name, pattern, rows, mode):
    top = lib.topology(name, TOPOLOGIES[name], pattern, seed=zlib.crc32(name.encode()) % 1000)
    languages_available(top)
    pipeline, uuids = lib.build(mage_project, top, rows)
    before = provenance.machine_snapshot()
    pipeline_run, seconds = lib.run(pipeline, mode)
    states = lib.statuses(pipeline_run)
    expected = lib.oracle(top, rows)
    problems = {}
    for leaf in top.leaves:
        block_uuid = uuids[leaf.index]
        if states.get(block_uuid) != 'completed':
            problems[block_uuid] = f'status {states.get(block_uuid)}'
            continue
        try:
            actual = lib.stored(pipeline, block_uuid, pipeline_run)
            diff = lib.differences(expected[leaf.index], actual)
        except Exception as error:
            diff = f'{type(error).__name__}: {error}'[:300]
        if diff:
            problems[block_uuid] = diff
    failed = {b: s for b, s in states.items() if s != 'completed'}
    provenance.record('correctness', dict(
        topology=name, pattern=pattern, rows=rows, mode=mode, nodes=len(top.nodes),
        edges=sum(len(n.upstream) for n in top.nodes), languages=top.languages(),
        seconds=round(seconds, 3), block_seconds=lib.block_seconds(pipeline_run),
        statuses=states, problems=problems, pipeline=pipeline.uuid,
        status=getattr(pipeline_run.status, 'value', pipeline_run.status),
    ), before)
    assert not failed and not problems, {'failed': failed, 'problems': problems}
