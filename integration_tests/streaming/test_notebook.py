"""
Blocks of a streaming pipeline run from the notebook. A YAML source or sink block checks
its connection; its YAML ran as Python and failed with a NameError.
"""
import json

import pytest


def run_block(pipeline_uuid, block_uuid, **variables):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    block = Pipeline.get(pipeline_uuid).get_block(block_uuid)
    return block.execute_with_callback(from_notebook=True, global_vars=variables)


def test_source_and_sink_blocks_check_their_connection(
    mage_project, rabbitmq_channel, rabbitmq_queue, capsys,
):
    rabbitmq_channel.basic_publish('', rabbitmq_queue, json.dumps({'n': 1}))

    source = run_block('stream_rabbitmq', 'stream_rabbitmq_source', in_queue=rabbitmq_queue)
    sink = run_block('stream_rabbitmq', 'stream_rabbitmq_to_rabbitmq', out_queue=rabbitmq_queue)

    assert source['output'] == [] and sink['output'] == []
    output = capsys.readouterr().out
    assert 'Connected to the rabbitmq source' in output
    assert 'Connected to the rabbitmq sink' in output
    # The check reads no message.
    method = rabbitmq_channel.queue_declare(rabbitmq_queue, passive=True).method
    assert (method.message_count, method.consumer_count) == (1, 0)


def test_kafka_blocks_check_their_connection(mage_project, kafka_bootstrap, kafka_topic, capsys):
    run_block(
        'stream_kafka', 'stream_kafka_source', in_topic=kafka_topic, consumer_group='notebook',
    )
    run_block('stream_kafka', 'stream_kafka_to_kafka', out_topic=kafka_topic)

    output = capsys.readouterr().out
    assert 'Connected to the kafka source' in output
    assert 'Connected to the kafka sink' in output


def test_a_wrong_password_fails_the_check(mage_project, rabbitmq_queue, monkeypatch):
    from pika.exceptions import AMQPConnectionError

    monkeypatch.setenv('MAGE_TEST_RABBITMQ_PASSWORD', 'wrong')

    with pytest.raises(AMQPConnectionError):
        run_block('stream_rabbitmq', 'stream_rabbitmq_source', in_queue=rabbitmq_queue)
