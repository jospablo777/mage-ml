"""
Exports a Kafka streaming pipeline (a YAML source, a Python transformer, a YAML sink) from a
new Mage project, for CI to build its Docker image: the image must build librdkafka and run
`mage-service validate`.

    python scripts/ci/export_stream.py PROJECT_DIR OUT_DIR
"""
import subprocess
import sys
import textwrap
from pathlib import Path


def main(project: str, out: str) -> None:
    subprocess.run(
        [sys.executable, '-m', 'mage_ai.cli.main', 'init', project],
        check=True, capture_output=True,
    )
    from mage_ai.settings.repo import set_repo_path

    set_repo_path(project)
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.pipeline_services import export

    pipeline = Pipeline.create(
        'orders_stream', pipeline_type=PipelineType.STREAMING, repo_path=project,
    )

    def add(name, kind, code, language, upstream=()):
        block = Block.create(name, kind, project, language=language, pipeline=pipeline)
        Path(block.file_path).write_text(textwrap.dedent(code))
        pipeline.add_block(block, upstream_block_uuids=[b.uuid for b in upstream])
        return block

    source = add('orders_source', 'data_loader', '''
        connector_type: kafka
        bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
        topic: orders
        consumer_group: orders-clean
    ''', 'yaml')
    clean = add('clean_orders', 'transformer', '''
        @transformer
        def clean(messages, *args, **kwargs):
            return [m for m in messages if m.get('amount') is not None]
    ''', 'python', [source])
    add('orders_sink', 'data_exporter', '''
        connector_type: kafka
        bootstrap_server: "{{ env_var('KAFKA_BOOTSTRAP') }}"
        topic: orders_clean
    ''', 'yaml', [clean])
    print(export.write(export.capture(project, ['orders_stream']), out, force=True))


if __name__ == '__main__':
    main(*sys.argv[1:3])
