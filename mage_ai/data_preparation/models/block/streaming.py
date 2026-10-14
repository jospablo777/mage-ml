"""
Streaming source and sink blocks run from the notebook.

A streaming pipeline's YAML source and sink blocks run with the pipeline. Run alone, their
YAML ran as Python and failed with a NameError. They now check the block: the config is
rendered as the pipeline renders it, the source or sink connects, and the connection is
closed, so a wrong host, password or queue shows while the pipeline is built.
"""
from typing import Dict

import yaml
from jinja2 import Template

from mage_ai.data_preparation.models.constants import BlockType


def streaming_config(content: str, global_vars: Dict = None) -> Dict:
    """The config of a streaming block, with variables and template functions rendered."""
    from mage_ai.data_preparation.shared.utils import get_template_vars

    global_vars = global_vars or {}
    rendered = Template(content).render(
        variables=lambda name: global_vars.get(name),
        **get_template_vars(),
    )
    return yaml.safe_load(rendered)


def check_connection(block_type: str, content: str, global_vars: Dict = None) -> None:
    config = streaming_config(content, global_vars)
    if not isinstance(config, dict) or not config.get('connector_type'):
        raise ValueError('The block needs a connector_type, such as kafka or rabbitmq.')
    connector_type = config['connector_type']
    kind = 'source' if block_type == BlockType.DATA_LOADER else 'sink'
    if connector_type == 'webhook' and block_type == BlockType.DATA_LOADER:
        # Nothing to connect to: webhooks call the exported service.
        print(
            'A webhook source has no connection to check: senders POST JSON messages to the '
            'exported service at /v1/streams/<pipeline>/messages. Develop the blocks on '
            'samples (the Samples tab), then export the stream as a service.'
        )
        return
    if connector_type in ('http', 'feast') and block_type == BlockType.DATA_EXPORTER:
        print(
            f'The {connector_type} sink posts each batch to {config.get("url")} from the '
            'exported service, which checks its address when the stream starts. Run the '
            'blocks on samples to see the messages it would send.'
        )
        return
    print(f'Connecting to the {connector_type} {kind}...')
    if block_type == BlockType.DATA_LOADER:
        from mage_ai.streaming.sources.source_factory import SourceFactory

        config = dict(config)
        if connector_type == 'postgres':
            # A replication slot keeps the server's log until it is read, so a check does
            # not create one.
            config['create_slot'] = False
        connector = SourceFactory.get_source(config)
        # The source skips it in test environments.
        connector.test_connection()
    else:
        from mage_ai.streaming.sinks.sink_factory import SinkFactory

        connector = SinkFactory.get_sink(dict(config))
    try:
        describe = getattr(connector, 'describe_setup', None)
        if describe:
            for line in describe():
                print(line)
        print(
            f'Connected to the {connector_type} {kind}. The block runs with the pipeline: '
            'use Execute pipeline to process messages.'
        )
    finally:
        connector.destroy()
