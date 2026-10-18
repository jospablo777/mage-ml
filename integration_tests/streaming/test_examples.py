"""
The stream examples on the engine, with generated traffic: orders to clean facts with dead
letters, payments scored by the example's model, IoT telemetry in two branches, clicks upserted
into PostgreSQL once, and a Python block followed by a Rust block.
"""
import time
import uuid
from pathlib import Path

import pytest


def wait_for(condition, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        found = condition()
        if found:
            return found
        time.sleep(0.5)
    raise AssertionError('timed out')


def rename(project, pipeline, renames):
    """Gives the example's topics or subjects names of this test's own."""
    from mage_ai.data_preparation.models.pipeline import Pipeline

    for block in Pipeline.get(pipeline, repo_path=project).blocks_by_uuid.values():
        path = Path(block.file_path)
        text = path.read_text()
        for old, new in renames.items():
            text = text.replace(old, new)
        path.write_text(text)


@pytest.fixture
def engine(monkeypatch):
    from integration_tests.exported_services import stream_service_binary

    monkeypatch.setenv('MAGE_ENGINE_PATH', str(stream_service_binary()))


def run_example(project, name, renames, target, **generate):
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dev_run

    pipeline = f'{name}_{uuid.uuid4().hex[:8]}'
    streams.write(project, name, pipeline)
    rename(project, pipeline, renames)
    sinks = [b.uuid for b in Pipeline.get(pipeline, repo_path=project).blocks_by_uuid.values()
             if str(b.type) == 'data_exporter']
    streams.generate(name, count=200, rate=0, seed=1, target=target, **generate)
    dev_run.start(project, pipeline, sinks, user='ex')
    return pipeline


def test_orders_become_facts_and_broken_ones_dead_letters(
    mage_project, kafka_bootstrap, engine, monkeypatch,
):
    from kafka.admin import KafkaAdminClient, NewTopic

    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dead_letters, dev_run

    monkeypatch.setenv('KAFKA_BOOTSTRAP', kafka_bootstrap)
    suffix = uuid.uuid4().hex[:8]
    topics = {t: f'{t}_{suffix}' for t in ('orders', 'orders_clean', 'orders_dead_letters')}
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1)
                         for t in topics.values()])
    broken = sum(1 for m in streams.messages('orders', 200, seed=1)
                 if any(i['quantity'] <= 0 for i in m['items']))
    pipeline = None
    try:
        pipeline = run_example(
            mage_project, 'orders',
            {f'topic: {k}\n': f'topic: {v}\n' for k, v in topics.items()},
            topics['orders'], bootstrap=kafka_bootstrap)
        facts = wait_for(lambda: read_topic(kafka_bootstrap, topics['orders_clean'],
                                            200 - broken))
        assert all(f['total_cents'] > 0 for f in facts)
        found = wait_for(lambda: (lambda f: f if len(f['letters']) == broken else None)(
            dead_letters.read_dead_letters(mage_project, pipeline)))
        assert all('quantity -1' in letter['error'] for letter in found['letters'])
    finally:
        if pipeline:
            dev_run.stop(mage_project, pipeline)
            Pipeline.get(pipeline, repo_path=mage_project).delete()
        admin.delete_topics(list(topics.values()))
        admin.close()


def test_payments_are_scored_by_the_example_model(
    mage_project, kafka_bootstrap, engine, monkeypatch,
):
    from kafka.admin import KafkaAdminClient, NewTopic

    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming import dev_run

    monkeypatch.setenv('KAFKA_BOOTSTRAP', kafka_bootstrap)
    suffix = uuid.uuid4().hex[:8]
    topics = {t: f'{t}_{suffix}' for t in ('payments', 'payment_scores')}
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1)
                         for t in topics.values()])
    pipeline = None
    try:
        pipeline = run_example(
            mage_project, 'payments',
            {f'topic: {k}\n': f'topic: {v}\n' for k, v in topics.items()},
            topics['payments'], bootstrap=kafka_bootstrap)
        scores = wait_for(lambda: read_topic(kafka_bootstrap, topics['payment_scores'], 200))
        assert all(0 <= s['risk'] <= 1 for s in scores)
        assert 0 < sum(s['review'] for s in scores) < 200
    finally:
        if pipeline:
            dev_run.stop(mage_project, pipeline)
            Pipeline.get(pipeline, repo_path=mage_project).delete()
        admin.delete_topics(list(topics.values()))
        admin.close()


def test_telemetry_flows_through_two_branches(mage_project, nats_url, engine, monkeypatch):
    import asyncio

    import nats

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dev_run

    monkeypatch.setenv('NATS_URL', nats_url)
    suffix = uuid.uuid4().hex[:8]
    names = dict(stream=f'TEL_{suffix}', out=f'TELOUT_{suffix}',
                 readings=f'tel_{suffix}.readings', out_subject=f'telout_{suffix}')

    async def js_call(action):
        connection = await nats.connect(nats_url)
        try:
            return await action(connection.jetstream())
        finally:
            await connection.close()

    async def setup(js):
        await js.add_stream(name=names['stream'], subjects=[names['readings']])
        await js.add_stream(name=names['out'], subjects=[f"{names['out_subject']}.>"])

    asyncio.run(js_call(setup))
    expected = list(streams.messages('iot', 200, seed=1))
    hot = sum(1 for m in expected if m['temperature'] > 40 and -40 <= m['temperature'] <= 125)
    pipeline = None
    try:
        pipeline = run_example(mage_project, 'iot', {
            'stream_name: TELEMETRY\n': f"stream_name: {names['stream']}\n",
            'subject: telemetry.readings\n': f"subject: {names['readings']}\n",
            'telemetry_out.': f"{names['out_subject']}.",
        }, names['readings'], nats_url=nats_url)

        async def counts(js):
            # Every clean reading on one subject, and the hot ones again as alerts.
            return (await js.stream_info(names['out'])).state.messages

        seen = []

        def complete():
            seen.append(asyncio.run(js_call(counts)))
            return seen[-1] == 200 + hot

        try:
            wait_for(complete)
        except AssertionError:
            status = dev_run.status(mage_project, pipeline)
            raise AssertionError(
                f'{seen[-1:]} messages in the out stream, expected {200 + hot}; '
                f'blocks {status.get("blocks")}; last error {status.get("last_error")}\n'
                + '\n'.join(status.get('logs') or [])[-3000:]
            )
    finally:
        if pipeline:
            dev_run.stop(mage_project, pipeline)
            Pipeline.get(pipeline, repo_path=mage_project).delete()

        async def cleanup(js):
            for stream in (names['stream'], names['out']):
                await js.delete_stream(stream)

        asyncio.run(js_call(cleanup))


def test_clicks_are_upserted_once_however_often_they_are_sent(
    mage_project, kafka_bootstrap, postgres_settings, pg, engine, monkeypatch,
):
    from kafka.admin import KafkaAdminClient, NewTopic

    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dev_run

    monkeypatch.setenv('KAFKA_BOOTSTRAP', kafka_bootstrap)
    for key, variable in (('host', 'PGHOST'), ('port', 'PGPORT'), ('dbname', 'PGDATABASE'),
                          ('user', 'PGUSER'), ('password', 'PGPASSWORD')):
        monkeypatch.setenv(variable, str(postgres_settings[key]))
    suffix = uuid.uuid4().hex[:8]
    topic, table = f'clicks_{suffix}', f'click_events_{suffix}'
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    pipeline = None

    def rows():
        with pg.cursor() as cursor:
            cursor.execute("SELECT to_regclass(%s)", (f'public.{table}',))
            if cursor.fetchone()[0] is None:
                pg.rollback()
                return 0
            cursor.execute(f'SELECT count(*), count(DISTINCT session_key) FROM public.{table}')
            count, _ = cursor.fetchone()
        pg.rollback()
        return count

    try:
        pipeline = run_example(mage_project, 'clickstream', {
            'topic: clicks\n': f'topic: {topic}\n', 'table: click_events\n': f'table: {table}\n',
        }, topic, bootstrap=kafka_bootstrap)
        wait_for(lambda: rows() == 200)
        # The same 200 clicks again: upserts by event_id write no new row.
        streams.generate('clickstream', count=200, rate=0, seed=1, target=topic,
                         bootstrap=kafka_bootstrap)
        wait_for(lambda: dev_run.status(mage_project, pipeline).get('messages_in') == 400)
        time.sleep(2)
        assert rows() == 200
    finally:
        if pipeline:
            dev_run.stop(mage_project, pipeline)
            Pipeline.get(pipeline, repo_path=mage_project).delete()
        with pg.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
        pg.commit()
        admin.delete_topics([topic])
        admin.close()


def test_a_python_and_a_rust_block_run_as_two_runs(
    mage_project, kafka_bootstrap, engine, monkeypatch,
):
    from kafka.admin import KafkaAdminClient, NewTopic

    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dev_run

    monkeypatch.setenv('KAFKA_BOOTSTRAP', kafka_bootstrap)
    suffix = uuid.uuid4().hex[:8]
    topics = {t: f'{t}_{suffix}' for t in ('orders_multi', 'orders_priced')}
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1)
                         for t in topics.values()])
    kept = sum(1 for m in streams.messages('multilang', 200, seed=1)
               if all(i['quantity'] > 0 for i in m['items']))
    pipeline = None
    try:
        pipeline = run_example(
            mage_project, 'multilang',
            {f'topic: {k}\n': f'topic: {v}\n' for k, v in topics.items()},
            topics['orders_multi'], bootstrap=kafka_bootstrap)
        priced = wait_for(lambda: read_topic(kafka_bootstrap, topics['orders_priced'], kept),
                          timeout=300)
        assert all(p['band'] in ('high', 'standard') and p['total'] > 0 for p in priced)
        blocks = {b['block'].rsplit('_', 1)[-1] for b in
                  dev_run.status(mage_project, pipeline)['blocks']}
        assert {'parse', 'price'} <= blocks
    finally:
        if pipeline:
            dev_run.stop(mage_project, pipeline)
            Pipeline.get(pipeline, repo_path=mage_project).delete()
        admin.delete_topics(list(topics.values()))
        admin.close()


def test_table_changes_become_kafka_events(
    mage_project, kafka_bootstrap, postgres_settings, pg, engine, monkeypatch,
):
    from kafka.admin import KafkaAdminClient, NewTopic

    from integration_tests.streaming.test_pipelines import read_topic
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.examples import streams
    from mage_ai.streaming import dev_run
    from mage_ai.streaming.development import development_slot

    monkeypatch.setenv('KAFKA_BOOTSTRAP', kafka_bootstrap)
    for key, variable in (('host', 'PGHOST'), ('port', 'PGPORT'), ('dbname', 'PGDATABASE'),
                          ('user', 'PGUSER'), ('password', 'PGPASSWORD')):
        monkeypatch.setenv(variable, str(postgres_settings[key]))
    suffix = uuid.uuid4().hex[:8]
    table, slot, topic = f'shop_orders_{suffix}', f'cdc_{suffix}', f'order_changes_{suffix}'
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    with pg.cursor() as cursor:
        cursor.execute(f'CREATE TABLE public.{table} (id bigint PRIMARY KEY, customer text, '
                       'status text, amount numeric(12, 2))')
    pg.commit()
    pipeline = f'cdc_{suffix}'
    dev_slot = development_slot(slot, 'ex')
    try:
        streams.write(mage_project, 'cdc', pipeline)
        rename(mage_project, pipeline, {
            'public.shop_orders': f'public.{table}', 'shop_orders_cdc': slot,
            'topic: order_changes\n': f'topic: {topic}\n',
        })
        sinks = [b.uuid for b in
                 Pipeline.get(pipeline, repo_path=mage_project).blocks_by_uuid.values()
                 if str(b.type) == 'data_exporter']
        dev_run.start(mage_project, pipeline, sinks, user='ex')

        def slot_ready():
            with pg.cursor() as cursor:
                cursor.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name = %s',
                               (dev_slot,))
                found = cursor.fetchone()
            pg.rollback()
            return found

        # Changes before the slot exists are not in it: rows go in once it does.
        wait_for(slot_ready)
        streams.generate('cdc', count=50, rate=0, seed=1, target=f'public.{table}')

        events = wait_for(lambda: read_topic(kafka_bootstrap, topic, 50))
        assert sorted(e['id'] for e in events) == list(range(1, 51))
        assert {e['op'] for e in events} == {'insert'}
        assert all(e['table'] == f'public.{table}' for e in events)
    finally:
        dev_run.stop(mage_project, pipeline)
        if Pipeline.get(pipeline, repo_path=mage_project, check_if_exists=True):
            Pipeline.get(pipeline, repo_path=mage_project).delete()
        with pg.cursor() as cursor:
            cursor.execute('SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots '
                           'WHERE slot_name = %s AND active_pid IS NOT NULL', (dev_slot,))
            time.sleep(1)
            cursor.execute('SELECT pg_drop_replication_slot(slot_name) FROM '
                           'pg_replication_slots WHERE slot_name = %s', (dev_slot,))
            cursor.execute(f'DROP PUBLICATION IF EXISTS {slot}')
            cursor.execute(f'DROP TABLE IF EXISTS public.{table}')
        pg.commit()
        admin.delete_topics([topic])
        admin.close()
