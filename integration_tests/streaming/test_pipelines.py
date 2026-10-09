"""
Streaming pipelines run the two ways Mage runs them, from a trigger and from the
notebook: a source, a Python transformer and several sinks, with messages of several
value types, checked in every sink.
"""
import json
import uuid

import pytest

from integration_tests.streaming_runner import MODES, streaming_pipeline, wait_until

COUNT = 40


def message(i):
    return dict(
        n=i,
        big=2**53 + i if i % 3 else None,
        price=i + 0.25,
        text=f'ñ "{i}" it\'s',
        flag=i % 2 == 0,
    )


def expected(i):
    return dict(message(i), doubled=i * 2)


@pytest.fixture
def kafka_topics(kafka_bootstrap):
    from kafka.admin import KafkaAdminClient, NewTopic

    names = [f'it_{uuid.uuid4().hex[:12]}' for _ in range(2)]
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(n, num_partitions=2, replication_factor=1) for n in names])
    yield names
    admin.delete_topics(names)
    admin.close()


def read_topic(bootstrap, topic, count, timeout_ms=1000):
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic, bootstrap_servers=bootstrap, auto_offset_reset='earliest',
        consumer_timeout_ms=timeout_ms, value_deserializer=json.loads,
    )
    values = [m.value for m in consumer]
    consumer.close()
    return values if len(values) >= count else None


@pytest.mark.parametrize('mode', MODES)
def test_kafka_to_kafka_postgres_and_mongodb(
    mode, mage_project, kafka_bootstrap, kafka_topics, pg, schema, mongo,
):
    from kafka import KafkaProducer

    in_topic, out_topic = kafka_topics
    with streaming_pipeline(
        'stream_kafka', mode, in_topic=in_topic, out_topic=out_topic,
        consumer_group=f'it_{uuid.uuid4().hex[:8]}', pg_schema=schema,
        mongo_database=mongo.name,
    ) as run:
        producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap, value_serializer=lambda v: json.dumps(v).encode(),
        )
        for i in range(COUNT):
            producer.send(in_topic, message(i))
        producer.flush()
        producer.close()

        values = wait_until(
            lambda: read_topic(kafka_bootstrap, out_topic, COUNT), run, message='Kafka sink',
        )
        wait_until(lambda: mongo.events.count_documents({}) >= COUNT, run, message='MongoDB')

        def postgres_rows():
            with pg.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = %s "
                    "AND table_name = 'events'", (schema,),
                )
                if not cursor.fetchone()[0]:
                    pg.rollback()
                    return None
                cursor.execute(
                    f'SELECT n, big, price, text, flag, doubled FROM {schema}.events ORDER BY n',
                )
                rows = cursor.fetchall()
            pg.rollback()
            return rows if len(rows) >= COUNT else None

        rows = wait_until(postgres_rows, run, message='Postgres')

    assert sorted(values, key=lambda v: v['n']) == [expected(i) for i in range(COUNT)]
    documents = sorted(mongo.events.find({}, {'_id': 0}), key=lambda d: d['n'])
    assert documents == [expected(i) for i in range(COUNT)]
    assert [tuple(row) for row in rows] == [
        tuple(expected(i)[k] for k in ('n', 'big', 'price', 'text', 'flag', 'doubled'))
        for i in range(COUNT)
    ]


@pytest.mark.parametrize('mode', MODES)
def test_rabbitmq_to_rabbitmq_mysql_and_clickhouse(
    mode, mage_project, rabbitmq_channel, rabbitmq_queue, my, mysql_database, ch,
    monkeypatch,
):
    # The mysql profile of io_config.yaml reads the database from this variable.
    monkeypatch.setenv('MAGE_TEST_MYSQL_DATABASE', mysql_database)
    out_queue = f'it_{uuid.uuid4().hex[:12]}'
    rabbitmq_channel.queue_declare(out_queue, durable=True)
    try:
        with streaming_pipeline(
            'stream_rabbitmq', mode, in_queue=rabbitmq_queue, out_queue=out_queue,
        ) as run:
            for i in range(COUNT):
                rabbitmq_channel.basic_publish('', rabbitmq_queue, json.dumps(message(i)))

            def out_messages():
                count = rabbitmq_channel.queue_declare(out_queue, passive=True).method
                return count.message_count >= COUNT

            wait_until(out_messages, run, message='RabbitMQ sink')

            def mysql_rows():
                with my.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = 'events'",
                    )
                    if not cursor.fetchone()[0]:
                        return None
                    cursor.execute(
                        'SELECT n, big, price, text, flag, doubled FROM events ORDER BY n',
                    )
                    rows = cursor.fetchall()
                return rows if len(rows) >= COUNT else None

            mysql = wait_until(mysql_rows, run, message='MySQL')

            def clickhouse_rows():
                if not ch.command("EXISTS TABLE events"):
                    return None
                rows = ch.query(
                    'SELECT n, big, price, text, flag, doubled FROM events ORDER BY n',
                ).result_rows
                return rows if len(rows) >= COUNT else None

            clickhouse = wait_until(clickhouse_rows, run, message='ClickHouse')

        out = []
        while True:
            method, _, body = rabbitmq_channel.basic_get(out_queue, auto_ack=True)
            if method is None:
                break
            out.append(json.loads(body))
        # Every input message was acked.
        assert rabbitmq_channel.queue_declare(rabbitmq_queue, passive=True).method \
            .message_count == 0
    finally:
        rabbitmq_channel.queue_delete(out_queue)

    assert sorted(out, key=lambda v: v['n']) == [expected(i) for i in range(COUNT)]
    keys = ('n', 'big', 'price', 'text', 'flag', 'doubled')
    rows = [tuple(expected(i)[k] for k in keys) for i in range(COUNT)]
    # MySQL stores BOOLEAN as TINYINT.
    assert [tuple(r) for r in mysql] == [r[:4] + (int(r[4]),) + r[5:] for r in rows]
    assert [tuple(r) for r in clickhouse] == rows


@pytest.mark.parametrize('mode', MODES)
def test_nats_to_nats(mode, mage_project, nats_url):
    from integration_tests.nats.test_streaming import jetstream, publish

    names = [f'it_{uuid.uuid4().hex[:12]}' for _ in range(2)]
    subjects = [f'{n}.events' for n in names]
    for name, subject in zip(names, subjects):
        jetstream(nats_url, lambda js, n=name, s=subject: js.add_stream(name=n, subjects=[s]))
    try:
        with streaming_pipeline(
            'stream_nats', mode, in_stream=names[0], in_subject=subjects[0],
            out_stream=names[1], out_subject=subjects[1],
        ) as run:
            publish(nats_url, subjects[0], *[message(i) for i in range(COUNT)])

            def out_count():
                async def action(js):
                    return (await js.stream_info(names[1])).state.messages

                return jetstream(nats_url, action) >= COUNT

            wait_until(out_count, run, message='NATS sink')

        async def read(js):
            subscription = await js.pull_subscribe(subjects[1], durable='reader', stream=names[1])
            return [json.loads(m.data) for m in await subscription.fetch(COUNT, timeout=5)]

        values = jetstream(nats_url, read)
    finally:
        for name in names:
            jetstream(nats_url, lambda js, n=name: js.delete_stream(n))

    assert sorted(values, key=lambda v: v['n']) == [expected(i) for i in range(COUNT)]


@pytest.mark.parametrize('mode', MODES)
def test_activemq_to_activemq(mode, mage_project, activemq_port):
    from integration_tests.activemq.test_streaming import drain, publish

    in_queue, out_queue = (f'it_{uuid.uuid4().hex[:12]}' for _ in range(2))
    with streaming_pipeline(
        'stream_activemq', mode, in_queue=in_queue, out_queue=out_queue,
    ) as run:
        publish(activemq_port, in_queue, *[message(i) for i in range(COUNT)])
        frames = []

        def out_messages():
            frames.extend(drain(activemq_port, out_queue, wait=1))
            return len(frames) >= COUNT

        wait_until(out_messages, run, message='ActiveMQ sink')

    values = [json.loads(f.body) for f in frames]
    assert sorted(values, key=lambda v: v['n']) == [expected(i) for i in range(COUNT)]
    # Every input message was acked.
    assert drain(activemq_port, in_queue, wait=1) == []
