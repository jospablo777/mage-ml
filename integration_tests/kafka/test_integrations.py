"""
The Kafka destination of mage_integrations against Kafka 4.1, run as a program the way
Mage runs it, on Singer messages.
"""
import json
import subprocess
import sys
from pathlib import Path

from kafka import KafkaConsumer

DESTINATION = (
    Path(__file__).resolve().parents[2]
    / 'mage_integrations' / 'mage_integrations' / 'destinations' / 'kafka' / '__init__.py'
)


def messages(count):
    schema = {
        'type': 'SCHEMA',
        'stream': 'orders',
        'key_properties': ['id'],
        'schema': {'properties': {
            'id': {'type': ['integer']},
            'text': {'type': ['null', 'string']},
        }},
    }
    records = [
        {'type': 'RECORD', 'stream': 'orders', 'record': {'id': i, 'text': f'ñ {i}'}}
        for i in range(count)
    ]
    return [schema, *records]


def run_destination(kafka_bootstrap, topic, tmp_path, singer_messages, **config):
    input_path = tmp_path / 'input.jsonl'
    input_path.write_text('\n'.join(json.dumps(m) for m in singer_messages) + '\n')
    config_json = json.dumps(dict(bootstrap_server=kafka_bootstrap, topic=topic, **config))
    result = subprocess.run(
        [sys.executable, str(DESTINATION), '--config_json', config_json,
         '--input_file_path', str(input_path), '--state', str(tmp_path / 'state.json')],
        capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]


def consumed(kafka_bootstrap, topic):
    consumer = KafkaConsumer(
        topic, bootstrap_servers=kafka_bootstrap, auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
    )
    result = [(m.key, json.loads(m.value)) for m in consumer]
    consumer.close()
    return result


def test_records_reach_the_topic_with_their_keys(kafka_bootstrap, kafka_topic, tmp_path):
    """
    With the default api_version, (0, 10, 2), every request timed out against Kafka 4.
    The records stayed in the producer's buffer when the program ended, and every record
    had the key property's name, 'id', as its key.
    """
    run_destination(kafka_bootstrap, kafka_topic, tmp_path, messages(50))

    received = consumed(kafka_bootstrap, kafka_topic)

    assert len(received) == 50
    by_id = {value['id']: (key, value) for key, value in received}
    assert sorted(by_id) == list(range(50))
    assert all(key == str(i).encode() for i, (key, _) in by_id.items())
    assert by_id[7][1]['text'] == 'ñ 7'


def test_an_explicit_api_version_is_kept(kafka_bootstrap, kafka_topic, tmp_path):
    run_destination(kafka_bootstrap, kafka_topic, tmp_path, messages(3), api_version='(2, 6)')

    assert len(consumed(kafka_bootstrap, kafka_topic)) == 3
