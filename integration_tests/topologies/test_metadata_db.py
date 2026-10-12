"""
The metadata database's journal and the long tails of run times (compilation study).

Block by block, the scheduler and each block's process write block statuses to the same
SQLite database. In SQLite's default rollback journal a writer locks out readers and other
writers, and Python's sqlite3 waits up to 5 seconds for a lock; in WAL mode readers do not
block the writer. The chain_length experiment showed blocks that took 6 to 8 seconds
instead of 0.5, and runs several times their median.

The journal mode is stored in the database file, so the experiment switches the study's
database between the two modes and runs the same 8-block pandas Line in both, block by
block and fused, in a seeded order of arms, with MAGE_STUDY_JOURNAL_REPEATS repetitions
(20 by default). The database is left in its original mode.

Runs only with MAGE_STUDY_FULL=1.
"""
import os
import random
import sqlite3
import zlib

import pytest

from integration_tests.topologies import library as lib
from integration_tests.topologies import provenance

REPEATS = int(os.getenv('MAGE_STUDY_JOURNAL_REPEATS', '20'))
MODES = ['off', 'chains']


def database_path():
    from mage_ai.orchestration.db import engine

    if engine.url.get_backend_name() != 'sqlite':
        pytest.skip('The metadata database is not SQLite')
    return engine.url.database


def journal_mode(path, mode=None) -> str:
    from mage_ai.orchestration.db import engine

    # Pooled connections hold no transaction between runs; close them so the change
    # needs no lock, and new connections open in the new mode.
    engine.dispose()
    with sqlite3.connect(path, timeout=30) as connection:
        if mode:
            return connection.execute(f'PRAGMA journal_mode={mode}').fetchone()[0]
        return connection.execute('PRAGMA journal_mode').fetchone()[0]


def test_journal_mode_and_run_time_tails(mage_project):
    path = database_path()
    original = journal_mode(path)
    top = lib.topology('chain8', lib.chain(8), 'pandas')
    pipeline, uuids = lib.build(mage_project, top, 10_000, prefix='journal')
    expected = lib.oracle(top, 10_000)
    order = random.Random(zlib.crc32(b'journal'))
    try:
        for repeat in range(REPEATS + 1):
            arms = ['delete', 'wal']
            order.shuffle(arms)
            for arm in arms:
                assert journal_mode(path, arm) == arm
                modes = list(MODES)
                order.shuffle(modes)
                for position, mode in enumerate(modes):
                    before = provenance.wait_quiet()
                    pipeline_run, seconds = lib.run(pipeline, mode)
                    states = lib.statuses(pipeline_run)
                    problems = {}
                    leaf = uuids[top.leaves[0].index]
                    if states.get(leaf) != 'completed':
                        problems[leaf] = f'status {states.get(leaf)}'
                    else:
                        diff = lib.differences(expected[top.leaves[0].index],
                                               lib.stored(pipeline, leaf, pipeline_run))
                        if diff:
                            problems[leaf] = diff
                    provenance.record('journal', dict(
                        topology=top.name, languages=top.languages(), rows=10_000,
                        journal=arm, mode=mode, repeat=repeat, position=position,
                        cold=repeat == 0, pipeline=pipeline.uuid, seconds=round(seconds, 4),
                        block_seconds=lib.block_seconds(pipeline_run), problems=problems,
                    ), before)
                    assert not problems, problems
    finally:
        journal_mode(path, original)
