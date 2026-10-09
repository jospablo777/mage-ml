"""
Mage's MongoDB change stream source against MongoDB 8, a single-node replica set.
"""
import threading
import time

import pytest


class Stop(Exception):
    pass


def source(mongodb_url, database, checkpoint_path=None, **config):
    from mage_ai.streaming.sources.mongodb import MongoSource

    return MongoSource(
        dict(connection_str=mongodb_url, database=database, **config),
        checkpoint_path=checkpoint_path,
    )


def run_source(reader, handler, actions, timeout=30):
    """batch_read in a thread, with actions run once the change stream is open."""
    errors = []

    def target():
        try:
            reader.batch_read(handler)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    time.sleep(1.5)
    actions()
    thread.join(timeout)
    reader.destroy()
    assert not thread.is_alive(), 'batch_read did not return'
    return errors[0] if errors else None


def test_changes_reach_the_handler_and_errors_raise(mongodb_url, mongo):
    """Every error, from the handler too, was printed and batch_read returned."""
    received = []

    def handle(changes):
        received.extend(c['fullDocument']['n'] for c in changes)
        if len(received) == 3:
            raise Stop

    error = run_source(
        source(mongodb_url, mongo.name, collection='events'), handle,
        lambda: mongo.events.insert_many([{'n': i} for i in range(3)]),
    )

    assert isinstance(error, Stop)
    assert received == [0, 1, 2]


def test_a_restarted_source_resumes_where_it_stopped(mongodb_url, mongo, tmp_path):
    """Each start watched from that moment, so changes made while stopped were lost."""
    checkpoint = str(tmp_path / 'checkpoint.json')
    first = []

    def handle_first(changes):
        n = changes[0]['fullDocument']['n']
        if n == 2:
            raise Stop
        first.append(n)

    run_source(
        source(mongodb_url, mongo.name, checkpoint, collection='events'), handle_first,
        lambda: mongo.events.insert_many([{'n': i} for i in range(3)]),
    )
    # Inserted while no source runs.
    mongo.events.insert_many([{'n': 3}, {'n': 4}])
    second = []

    def handle_second(changes):
        second.append(changes[0]['fullDocument']['n'])
        if len(second) == 3:
            raise Stop

    run_source(
        source(mongodb_url, mongo.name, checkpoint, collection='events'), handle_second,
        lambda: None,
    )

    assert first == [0, 1]
    # The change whose handler failed comes again.
    assert second == [2, 3, 4]


def test_a_database_without_a_collection_is_watched(mongodb_url, mongo):
    """Without a collection the source watched nothing and returned."""
    received = []

    def handle(changes):
        received.append((changes[0]['ns']['coll'], changes[0]['fullDocument']['n']))
        if len(received) == 2:
            raise Stop

    def insert():
        mongo.a.insert_one({'n': 1})
        mongo.b.insert_one({'n': 2})

    error = run_source(source(mongodb_url, mongo.name), handle, insert)

    assert isinstance(error, Stop)
    assert received == [('a', 1), ('b', 2)]


def test_start_at_an_operation_time(mongodb_url, mongo):
    """operation_time was passed to watch under a name it does not take."""
    mongo.events.insert_one({'n': 'before'})
    start = int(time.time()) - 1
    received = []

    def handle(changes):
        received.append(changes[0]['fullDocument']['n'])
        raise Stop

    error = run_source(
        source(mongodb_url, mongo.name, collection='events', operation_time=start),
        handle, lambda: None,
    )

    assert isinstance(error, Stop)
    assert received == ['before']


def test_change_documents_encode_as_json(mongodb_url, mongo):
    """ObjectIds and BSON timestamps failed simplejson with encode_complex."""
    import simplejson

    from mage_ai.shared.parsers import encode_complex

    changes = []

    def handle(batch):
        changes.extend(batch)
        raise Stop

    run_source(
        source(mongodb_url, mongo.name, collection='events'), handle,
        lambda: mongo.events.insert_one({'n': 1}),
    )

    decoded = simplejson.loads(simplejson.dumps(changes[0], default=encode_complex))
    assert decoded['fullDocument']['_id'] == str(changes[0]['fullDocument']['_id'])
    assert decoded['clusterTime'] == {
        't': changes[0]['clusterTime'].time, 'i': changes[0]['clusterTime'].inc,
    }


@pytest.fixture(autouse=True)
def _mongodb_url(mongodb_url):
    return mongodb_url


def test_a_checkpoint_of_another_collection_is_not_used(mongodb_url, mongo, tmp_path):
    """After the config changed, the saved token failed every start."""
    checkpoint = str(tmp_path / 'checkpoint.json')

    def handle_one(changes):
        stop_after.append(changes)
        if len(stop_after) == 1:
            return
        raise Stop

    stop_after = []
    run_source(
        source(mongodb_url, mongo.name, checkpoint, collection='a'), handle_one,
        lambda: mongo.a.insert_many([{'n': 1}, {'n': 2}]),
    )
    received = []

    def handle(changes):
        received.append(changes[0]['fullDocument']['n'])
        raise Stop

    error = run_source(
        source(mongodb_url, mongo.name, checkpoint, collection='b'), handle,
        lambda: mongo.b.insert_one({'n': 3}),
    )

    assert isinstance(error, Stop)
    assert received == [3]
