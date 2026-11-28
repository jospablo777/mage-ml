"""
A streaming pipeline on Spark Structured Streaming, over Spark Connect (ADR-0047).

The plan comes from the export's capture, as a service's manifest does. `differences` says
what Spark cannot run as the engine does; a pipeline with any is refused with the list.
Version 1 runs a Kafka source, Python stream transformers and one Kafka sink:

    spark.readStream.format('kafka')       the source's topic and start
      .mapInArrow(blocks)                  the blocks, by the engine's own worker code
      .writeStream.format('kafka')         the sink's topic, positions in the checkpoint

The blocks run in mapInArrow through runtime/worker.py's run_stream_batch, which the
function carries with the blocks' sources and loads once per Python worker process, so a
block behaves as it does on the engine. Spark's Python workers need the client's Python
minor version and pyarrow.

Positions live in Spark's checkpoint, never in the consumer group: the first run starts
where the source's auto_offset_reset says. max_records is maxOffsetsPerTrigger and
max_wait_ms the trigger interval.
"""
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional

WORKER = Path(__file__).parent / 'runtime' / 'worker.py'
SECURITY_SETTINGS = {'security_protocol', 'sasl_config'}
SOURCE_SETTINGS = {'connector_type', 'bootstrap_server', 'topic', 'topics', 'consumer_group',
                   'auto_offset_reset', 'batch_size', 'timeout_ms'} | SECURITY_SETTINGS
SINK_SETTINGS = {'connector_type', 'bootstrap_server', 'topic'} | SECURITY_SETTINGS
LOGIN_MODULES = {
    'PLAIN': 'org.apache.kafka.common.security.plain.PlainLoginModule',
    'SCRAM-SHA-256': 'org.apache.kafka.common.security.scram.ScramLoginModule',
    'SCRAM-SHA-512': 'org.apache.kafka.common.security.scram.ScramLoginModule',
}
BLOCK_SETTINGS_ENGINE_ONLY = ('state_key', 'checkpoint')


class SparkStreamError(Exception):
    pass


@dataclass
class SparkStream:
    pipeline_uuid: str
    # Spark's Kafka reader and writer options.
    source: Dict[str, str]
    sink: Dict[str, str]
    # The worker's stream chain: each block's uuid, source text and configuration.
    chain: List[Dict[str, Any]]
    output: str
    variables: Dict[str, Any] = field(default_factory=dict)
    trigger_seconds: float = 1.0
    notes: List[str] = field(default_factory=list)


def _value(value: Any, environ: Mapping[str, str], where: str) -> Any:
    """A setting with its `{"$parts": ...}` references filled in from environ."""
    if isinstance(value, dict) and '$parts' in value:
        text = ''
        for part in value['$parts']:
            if not isinstance(part, dict):
                text += str(part)
                continue
            name = part.get('env')
            if name in environ:
                text += environ[name]
            elif 'default' in part:
                text += str(part['default'] or '')
            else:
                raise SparkStreamError(f'{where} reads the environment variable {name}, which '
                                       'is not set.')
        return text
    if isinstance(value, dict):
        return {k: _value(v, environ, where) for k, v in value.items()}
    if isinstance(value, list):
        return [_value(v, environ, where) for v in value]
    return value


def differences(pipeline: Dict[str, Any]) -> List[str]:
    """What Spark cannot run as the engine does, one sentence each; empty when it can."""
    stream = pipeline.get('stream') or {}
    found = []
    source = stream.get('source') or {}
    if source.get('kind') != 'kafka':
        found.append(f"The source is {source.get('kind')}; Spark runs Kafka sources for now.")
    else:
        extra = sorted(set(source.get('config') or {}) - SOURCE_SETTINGS)
        if extra:
            found.append(f"The source's settings {', '.join(extra)} have no Spark equivalent "
                         'yet.')
    sinks = stream.get('sinks') or []
    if len(sinks) != 1:
        found.append(f'The stream has {len(sinks)} sinks; Spark runs one for now.')
    for sink in sinks:
        if sink.get('kind') != 'kafka':
            found.append(f"Sink {sink.get('name')} is {sink.get('kind')}; Spark writes Kafka "
                         'for now.')
        else:
            extra = sorted(set(sink.get('config') or {}) - SINK_SETTINGS)
            if extra:
                found.append(f"Sink {sink.get('name')}'s settings {', '.join(extra)} have no "
                             'Spark equivalent yet.')
    if (stream.get('errors') or {}).get('dead_letter'):
        found.append('Dead letters run on the engine only for now.')
    if stream.get('delivery'):
        found.append(f"Delivery {stream['delivery']} runs on the engine only.")
    if stream.get('time'):
        found.append('Event time and late messages run on the engine only for now.')
    for block in pipeline.get('blocks') or []:
        name = block.get('uuid')
        if block.get('language') != 'python' or block.get('type') != 'transformer':
            # SQL, window and lookup blocks, and Rust and R blocks.
            found.append(f"Block {name} is a {block.get('language')} {block.get('type')}; "
                         'Spark runs Python stream transformers for now.')
            continue
        configured = [k for k in BLOCK_SETTINGS_ENGINE_ONLY
                      if (block.get('configuration') or {}).get(k)]
        if configured:
            found.append(f"Block {name} uses {', '.join(configured)}, which runs on the engine "
                         'only for now.')
    return found


def _kafka_security(config: Dict[str, Any], where: str) -> Dict[str, str]:
    """security_protocol and sasl_config as Spark's kafka.* options."""
    options = {}
    if config.get('security_protocol'):
        options['kafka.security.protocol'] = str(config['security_protocol'])
    sasl = config.get('sasl_config') or {}
    if sasl:
        mechanism = str(sasl.get('mechanism') or 'PLAIN').upper()
        if mechanism not in LOGIN_MODULES:
            raise SparkStreamError(f'{where}: SASL {mechanism} has no Spark equivalent yet; '
                                   f"use {', '.join(LOGIN_MODULES)}.")

        def quoted(value: Any) -> str:
            return '"' + str(value or '').replace('\\', '\\\\').replace('"', '\\"') + '"'

        options['kafka.sasl.mechanism'] = mechanism
        options['kafka.sasl.jaas.config'] = (
            f'{LOGIN_MODULES[mechanism]} required username={quoted(sasl.get("username"))} '
            f'password={quoted(sasl.get("password"))};'
        )
    return options


def _kafka_source(config: Dict[str, Any], batch: Dict[str, Any]) -> Dict[str, str]:
    topics = config.get('topics') or config.get('topic')
    if isinstance(topics, list):
        topics = ','.join(str(t) for t in topics)
    if not topics or not config.get('bootstrap_server'):
        raise SparkStreamError('The Kafka source needs bootstrap_server and topic.')
    options = {
        'kafka.bootstrap.servers': str(config['bootstrap_server']),
        'subscribe': str(topics),
        'startingOffsets': 'earliest' if config.get('auto_offset_reset') == 'earliest'
        else 'latest',
        # A topic deleted or truncated under the query stops it, as on the engine.
        'failOnDataLoss': 'true',
    }
    if batch.get('max_records'):
        options['maxOffsetsPerTrigger'] = str(int(batch['max_records']))
    options.update(_kafka_security(config, 'The source'))
    return options


def plan(repo_path: str, pipeline_uuid: str,
         environ: Optional[Mapping[str, str]] = None) -> SparkStream:
    """The pipeline as a Spark stream, from the export's capture; refused with what differs."""
    from mage_ai.pipeline_services import export

    environ = os.environ if environ is None else environ
    captured = export.capture(repo_path, [pipeline_uuid])
    pipeline = captured.manifest['pipelines'][0]
    stream = pipeline.get('stream')
    if not stream:
        raise SparkStreamError(f'{pipeline_uuid} is not a streaming pipeline.')
    found = differences(pipeline)
    if found:
        raise SparkStreamError(
            f'{pipeline_uuid} cannot run on Spark as it runs on the engine:\n- '
            + '\n- '.join(found)
        )
    source = _value(stream['source'].get('config') or {}, environ, 'The source')
    sink = stream['sinks'][0]
    sink_config = _value(sink.get('config') or {}, environ, f"Sink {sink['name']}")
    if not sink_config.get('bootstrap_server') or not sink_config.get('topic'):
        raise SparkStreamError(f"Sink {sink['name']} needs bootstrap_server and topic.")
    batch = stream.get('batch') or {}
    chain = []
    for block in pipeline.get('blocks') or []:
        path = Path(repo_path) / block['file']
        chain.append(dict(
            uuid=block['uuid'],
            code=path.read_text(),
            upstream=list(block.get('upstream') or []),
            configuration=block.get('configuration') or {},
        ))
    output = sink.get('after') or (chain[-1]['uuid'] if chain else '')
    notes = [
        'Positions are kept in the checkpoint; the consumer group\'s committed offsets are not '
        f"read. A first run starts at the {'earliest' if source.get('auto_offset_reset') == 'earliest' else 'latest'} "  # noqa: E501
        'offsets.',
    ]
    return SparkStream(
        pipeline_uuid=pipeline_uuid,
        source=_kafka_source(source, batch),
        sink={'kafka.bootstrap.servers': str(sink_config['bootstrap_server']),
              'topic': str(sink_config['topic']),
              **_kafka_security(sink_config, f"Sink {sink['name']}")},
        chain=chain,
        output=output,
        variables=dict(pipeline.get('variables') or {}),
        trigger_seconds=max(float(batch.get('max_wait_ms') or 1000) / 1000, 0.1),
        notes=notes,
    )


def blocks_function(stream: SparkStream, worker_source: Optional[str] = None) -> Callable:
    """
    The function mapInArrow runs on each partition's batches: the Kafka values in, the sink's
    messages out as JSON, made by the engine's worker code. Everything it needs travels in
    it, so Spark's Python workers need only pyarrow.
    """
    worker_source = WORKER.read_text() if worker_source is None else worker_source
    chain = [dict(b) for b in stream.chain]
    output = stream.output
    variables = dict(stream.variables)

    def run(batches: Iterator) -> Iterator:
        import hashlib
        import json as json_module
        import sys as sys_module
        import tempfile
        import types

        import pyarrow as pa

        digest = hashlib.sha256(worker_source.encode()).hexdigest()[:16]
        name = f'mageml_stream_worker_{digest}'
        worker = sys_module.modules.get(name)
        if worker is None:
            worker = types.ModuleType(name)
            # Its tracebacks leave out its own frames by this name.
            worker.__file__ = 'mageml_stream_worker.py'
            exec(compile(worker_source, worker.__file__, 'exec'), worker.__dict__)
            worker.mageml_block_files = {}
            sys_module.modules[name] = worker
        files = worker.mageml_block_files
        request_chain = []
        for block in chain:
            key = hashlib.sha256(block['code'].encode()).hexdigest()
            if key not in files:
                folder = tempfile.mkdtemp(prefix='mageml-block-')
                path = f"{folder}/{block['uuid']}.py"
                with open(path, 'w') as file:
                    file.write(block['code'])
                files[key] = path
            request_chain.append(dict(
                uuid=block['uuid'], file=files[key], sha256=key, upstream=block['upstream'],
                configuration=block['configuration'],
            ))
        for batch in batches:
            messages = []
            for value in batch.column('value').to_pylist():
                text = value.decode('utf-8', errors='replace') if value is not None else ''
                try:
                    messages.append(json_module.loads(text))
                except ValueError:
                    # Text that is not JSON reaches the blocks as text, as on the engine.
                    messages.append(text)
            if not messages:
                continue
            try:
                result = worker.run_stream_batch(dict(
                    chain=request_chain, messages=messages, outputs=[output],
                    kwargs=variables,
                ))
            except worker.BlockError as error:
                raise RuntimeError(
                    f'Block {error.block_uuid} failed ({error.phase}): {error}\n{error.details}'
                ) from None
            values = [
                json_module.dumps(m, allow_nan=False, default=worker._json_default).encode()
                for m in result['outputs'].get(output) or []
            ]
            yield pa.RecordBatch.from_pydict(
                {'value': values}, schema=pa.schema([('value', pa.binary())]),
            )

    return run


def _spark_session_class():
    import warnings

    try:
        with warnings.catch_warnings():
            # pyspark warns about pandas 3 on import; mapInArrow does not use pandas.
            warnings.filterwarnings('ignore', message='PySpark does not yet fully support pandas')
            from pyspark.sql import SparkSession
            from pyspark.sql.connect import (  # noqa: F401 - imported here, warned once
                functions,
            )
    except ImportError:
        raise SparkStreamError(
            'Spark Connect needs pyspark-client: uv pip install "mage-ml[spark]".'
        ) from None
    return SparkSession


def start(stream: SparkStream, remote: str, checkpoint: str, query_name: Optional[str] = None):
    """Starts the query on the Spark Connect server; returns (session, query)."""
    SparkSession = _spark_session_class()  # noqa: N806
    if not checkpoint:
        raise SparkStreamError('A checkpoint location is required: Spark keeps the stream\'s '
                               'positions there.')
    spark = SparkSession.builder.remote(remote).getOrCreate()
    reader = spark.readStream.format('kafka')
    for key, value in stream.source.items():
        reader = reader.option(key, value)
    frame = reader.load().select('value').mapInArrow(blocks_function(stream), 'value binary')
    writer = frame.writeStream.format('kafka').option('checkpointLocation', checkpoint)
    for key, value in stream.sink.items():
        writer = writer.option(key, value)
    query = (
        writer.queryName(query_name or stream.pipeline_uuid)
        .trigger(processingTime=f'{stream.trigger_seconds} seconds')
        .start()
    )
    return spark, query


def progress_line(progress: Optional[Dict[str, Any]]) -> Optional[str]:
    """One line of a query's last progress: its batch, rows and rates."""
    if not progress:
        return None
    if isinstance(progress, str):
        progress = json.loads(progress)
    rows = progress.get('numInputRows', 0)
    rate = progress.get('inputRowsPerSecond') or 0
    processed = progress.get('processedRowsPerSecond') or 0
    sink = (progress.get('sink') or {}).get('numOutputRows')
    line = (f"batch {progress.get('batchId')}: {rows} rows in, {rate:.0f}/s arriving, "
            f'{processed:.0f}/s processed')
    if sink is not None and sink >= 0:
        line += f', {sink} rows out'
    return line


def python_version_check(spark) -> Optional[str]:
    """Why the server's Python workers cannot run the blocks, or None."""
    import pyspark.sql.functions as F  # noqa: N812

    def versions(batches):
        import sys as sys_module

        import pyarrow as pa
        for _ in batches:
            yield pa.RecordBatch.from_pydict({'version': [
                f'{sys_module.version_info[0]}.{sys_module.version_info[1]}',
            ]})

    try:
        found = spark.range(1).select(F.col('id')).mapInArrow(versions, 'version string') \
            .collect()[0]['version']
    except Exception as error:
        return f'The server could not run Python with pyarrow: {str(error).splitlines()[0]}'
    mine = f'{sys.version_info[0]}.{sys.version_info[1]}'
    if found != mine:
        return f'The server\'s Python workers run {found}; the blocks need {mine}.'
    return None
