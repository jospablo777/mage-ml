"""
Measurements for the compilation study (docs/development/compilation-study.md): how run time
depends on chain length, language boundaries, data size and branching, block by block and
fused. Every measured pipeline must also give the oracle's output.

By default each experiment runs a small grid once, as a smoke test. MAGE_STUDY_FULL=1 runs
the whole grid with MAGE_STUDY_REPEATS repetitions (5 by default), and MAGE_STUDY_RESULTS_DIR
keeps the results (provenance.py).
"""
import os
import random
import shutil
import zlib

import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

FULL = bool(os.getenv('MAGE_STUDY_FULL'))
REPEATS = int(os.getenv('MAGE_STUDY_REPEATS', '5')) if FULL else 1


def grid(full, small):
    return full if FULL else small


LENGTHS = grid([1, 2, 4, 8, 16], [1, 4])
SIZES = grid([100, 10_000, 100_000, 1_000_000], [100, 10_000])
WIDTHS = grid([1, 2, 4, 8], [1, 3])
# A 6-block chain with 0 to 5 switches between two languages.
SWITCHES = {
    0: 'islands:6:{a},{b}',
    1: 'islands:3:{a},{b}',
    2: 'islands:2:{a},{b}',
    5: 'alternate:{a},{b}',
}
PAIRS = grid([('pandas', 'rust'), ('pandas', 'r'), ('pandas', 'polars'), ('rust', 'r')],
             [('pandas', 'rust')])
LANGUAGES = grid(list(lib.LANGUAGES), ['pandas', 'rust'])
MODES = ['off', 'chains']


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


def measure(project, experiment, top, rows, **labels):
    """
    Builds the pipeline once, then runs it REPEATS + 1 times in each mode. Within each
    repeat the modes run in a seeded random order, so drift over time (heat, caches) does
    not favour one mode. Repeat 0 builds binaries and fills caches: it is marked cold.
    """
    pipeline, uuids = lib.build(project, top, rows, prefix=experiment)
    expected = lib.oracle(top, rows)
    order = random.Random(zlib.crc32(
        f'{experiment}|{top.name}|{top.languages()}|{rows}|{sorted(labels.items())}'.encode()))
    for repeat in range(REPEATS + 1):
        modes = list(MODES)
        order.shuffle(modes)
        for position, mode in enumerate(modes):
            before = provenance.wait_quiet()
            pipeline_run, seconds = lib.run(pipeline, mode)
            states = lib.statuses(pipeline_run)
            problems = {}
            for leaf in top.leaves:
                block_uuid = uuids[leaf.index]
                if states.get(block_uuid) != 'completed':
                    problems[block_uuid] = f'status {states.get(block_uuid)}'
                    continue
                diff = lib.differences(expected[leaf.index],
                                       lib.stored(pipeline, block_uuid, pipeline_run))
                if diff:
                    problems[block_uuid] = diff
            provenance.record(experiment, dict(
                topology=top.name, languages=top.languages(), rows=rows, mode=mode,
                repeat=repeat, position=position, cold=repeat == 0, nodes=len(top.nodes),
                pipeline=pipeline.uuid, seconds=round(seconds, 4),
                block_seconds=lib.block_seconds(pipeline_run), problems=problems, **labels,
            ), before)
            assert not problems, problems


@pytest.mark.parametrize('length', LENGTHS)
@pytest.mark.parametrize('language', LANGUAGES)
def test_chain_length(mage_project, ready, language, length):
    top = lib.topology(f'chain{length}', lib.chain(length), language)
    ready(top)
    measure(mage_project, 'chain_length', top, 10_000, language=language, length=length)


@pytest.mark.parametrize('switches', list(SWITCHES))
@pytest.mark.parametrize('pair', PAIRS, ids=lambda p: '-'.join(p))
def test_language_switches(mage_project, ready, pair, switches):
    pattern = SWITCHES[switches].format(a=pair[0], b=pair[1])
    top = lib.topology('chain6', lib.chain(6), pattern)
    ready(top)
    measure(mage_project, 'switches', top, 10_000, pair='-'.join(pair), switches=switches)


@pytest.mark.parametrize('rows', SIZES)
@pytest.mark.parametrize('language', LANGUAGES)
def test_data_size(mage_project, ready, language, rows):
    top = lib.topology('chain4', lib.chain(4), language)
    ready(top)
    measure(mage_project, 'data_size', top, rows, language=language)


@pytest.mark.parametrize('width', WIDTHS)
@pytest.mark.parametrize('shape', ['fan_out', 'fan_in'])
@pytest.mark.parametrize('language', LANGUAGES)
def test_width(mage_project, ready, language, shape, width):
    upstream = lib.fan_out(width) if shape == 'fan_out' else lib.fan_in(width)
    top = lib.topology(f'{shape}{width}', upstream, language,
                       seed=zlib.crc32(shape.encode()))
    ready(top)
    measure(mage_project, 'width', top, 10_000, language=language, shape=shape, width=width)


@pytest.mark.parametrize('strings', ['object', 'arrow'])
def test_pandas_string_dtype(mage_project, ready, strings):
    """
    A fused stage passes a pandas frame in memory only when it has no object column. The
    same chain with its strings as object or as string[pyarrow].
    """
    top = lib.topology('chain6', lib.chain(6), 'pandas')
    top.strings = strings
    ready(top)
    measure(mage_project, 'string_dtype', top, grid(100_000, 10_000), strings=strings)
