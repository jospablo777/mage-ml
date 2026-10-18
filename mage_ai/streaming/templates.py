"""
New streaming pipelines from a template per source (C4): a source, a transformer, a sink that
prints, and a sample, so Run on sample works on the first ⌘↵ and the Run tab starts the
stream once the connection settings are filled in. Connection settings read the environment,
as an exported service needs: nothing secret is written in the files.
"""
import json
import textwrap
from typing import Dict, List

ORDERS = [
    {'order_id': 1001, 'customer': 'c-17', 'amount': 42.5, 'currency': 'EUR'},
    {'order_id': 1002, 'customer': 'c-03', 'amount': 7.0, 'currency': 'EUR'},
    {'order_id': 1003, 'customer': 'c-17', 'amount': 129.99, 'currency': 'USD'},
    {'order_id': 1004, 'customer': 'c-42', 'amount': -5.0, 'currency': 'EUR'},
    {'order_id': 1005, 'customer': 'c-08', 'amount': 18.25, 'currency': 'EUR'},
]

CHANGES = [
    dict(order, _mage_operation='insert', _mage_schema='public', _mage_table='orders',
         _mage_lsn=f'0/16B{3740 + index:04X}', _mage_commit_time='2026-10-17T10:00:00Z')
    for index, order in enumerate(ORDERS)
]

TRANSFORMER = '''
from typing import Dict, List


@transformer
def transform(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    Gets each batch the source reads and returns what the sinks write. Run it on the sample
    with Cmd+Enter (Ctrl+Enter); the Run tab runs the whole stream on the local engine.
    """
    valid = []
    for message in messages:
        # Text that is not JSON arrives as a string; refunds have a negative amount.
        if not isinstance(message, dict) or message.get('amount', 0) <= 0:
            continue
        valid.append(dict(message, amount_cents=round(message['amount'] * 100)))
    return valid
'''

SINK = '''
# Prints each batch, here and in the service's log. Replace it with a sink of the
# Data exporter menu (Kafka, PostgreSQL, ClickHouse, object storage and more).
connector_type: dummy
print_msg: true
'''

SOURCES: Dict[str, Dict] = {
    'kafka': dict(label='Kafka', sample=ORDERS, source='''
        connector_type: kafka
        bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"   # such as localhost:9092
        topic: orders
        # Runs from the editor read with <group>-dev-<user>; triggers and services with this.
        consumer_group: orders-pipeline
        auto_offset_reset: latest
        batch_size: 500
        # For SASL_SSL (IBM Event Streams, Confluent Cloud):
        # security_protocol: SASL_SSL
        # sasl_config:
        #   mechanism: PLAIN
        #   username: "{{ env_var('KAFKA_USERNAME') }}"
        #   password: "{{ env_var('KAFKA_PASSWORD') }}"
        # Fields replaced by <redacted> in captured samples:
        # secret_fields: [customer]
    '''),
    'rabbitmq': dict(label='RabbitMQ', sample=ORDERS, source='''
        connector_type: rabbitmq
        connection_host: "{{ env_var('RABBITMQ_HOST') }}"   # such as localhost
        connection_port: 5672
        queue_name: orders
        username: "{{ env_var('RABBITMQ_USER') }}"
        password: "{{ env_var('RABBITMQ_PASSWORD') }}"
        # How many messages the broker hands over before they are acknowledged.
        prefetch_count: 100
    '''),
    'nats': dict(label='NATS JetStream', sample=ORDERS, source='''
        connector_type: nats
        server_url: "{{ env_var('NATS_URL') }}"   # such as nats://localhost:4222
        stream_name: ORDERS
        subject: orders.created
        consumer_name: orders-pipeline
        batch_size: 100
    '''),
    'webhook': dict(label='Webhook (HTTP)', sample=ORDERS, source='''
        # Senders POST JSON messages to the service:
        #   POST /v1/streams/<pipeline>/messages, with the service's ingest token.
        # The stream runs on the engine (the Run tab, or an exported service).
        connector_type: webhook
        answer_within_seconds: 30
        dedupe_window_seconds: 86400
        # signature:                     # an HMAC-SHA256 of the body, as GitHub sends it
        #   header: X-Hub-Signature-256
        #   prefix: "sha256="
        #   secret: "{{ env_var('WEBHOOK_SECRET') }}"
    '''),
    'postgres': dict(label='PostgreSQL changes', sample=CHANGES, source='''
        # Changes of tables by logical replication: the server needs wal_level = logical,
        # and the user the REPLICATION attribute.
        connector_type: postgres
        host: "{{ env_var('PGHOST') }}"
        port: 5432
        database: "{{ env_var('PGDATABASE') }}"
        user: "{{ env_var('PGUSER') }}"
        password: "{{ env_var('PGPASSWORD') }}"
        # Runs from the editor read with <slot>_dev_<user>.
        replication_slot: orders_pipeline
        publication_name: orders_pipeline
        tables:
          - public.orders
        create_slot: true
    '''),
}


class TemplateError(ValueError):
    pass


def templates() -> List[Dict]:
    return [dict(uuid=uuid, label=value['label']) for uuid, value in SOURCES.items()]


def apply(pipeline, template: str) -> None:
    """Adds the template's blocks and sample to a new, empty streaming pipeline."""
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import BlockLanguage
    from mage_ai.streaming.samples import write_sample

    if template not in SOURCES:
        raise TemplateError(f'No stream template {template}; there are '
                            f'{", ".join(SOURCES)}.')
    found = SOURCES[template]
    repo_path = pipeline.repo_path

    def add(name, kind, content, language, upstream=()):
        block = Block.create(f'{pipeline.uuid}_{name}', kind, repo_path, language=language)
        with open(block.file_path, 'w') as file:
            file.write(textwrap.dedent(content).lstrip())
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add(template, 'data_loader', found['source'], BlockLanguage.YAML)
    transformer = add('transform', 'transformer', TRANSFORMER, BlockLanguage.PYTHON,
                      upstream=[source])
    add('print', 'data_exporter', SINK, BlockLanguage.YAML, upstream=[transformer])
    write_sample(repo_path, pipeline.uuid, 'example', json.loads(json.dumps(found['sample'])))
