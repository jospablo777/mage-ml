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
    print(f'Connecting to the {connector_type} {kind}...')
    if block_type == BlockType.DATA_LOADER:
        from mage_ai.streaming.sources.source_factory import SourceFactory

        connector = SourceFactory.get_source(dict(config))
        # The source skips it in test environments.
        connector.test_connection()
    else:
        from mage_ai.streaming.sinks.sink_factory import SinkFactory

        connector = SinkFactory.get_sink(dict(config))
    try:
        print(
            f'Connected to the {connector_type} {kind}. The block runs with the pipeline: '
            'use Execute pipeline to process messages.'
        )
    finally:
        connector.destroy()
