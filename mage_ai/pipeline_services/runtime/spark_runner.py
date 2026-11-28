"""
Runs a streaming pipeline's Spark plan (spark_stream.py, ADR-0047) on a Spark Connect server
and watches it: the process `mage spark-stream` runs, and the container a Spark stream
export runs. It needs pyspark-client and pyarrow, nothing of MageML:

    python spark_runner.py            # plan.json and worker.py beside it

with MAGE_SPARK_REMOTE (sc://host:15002) and MAGE_SPARK_CHECKPOINT set, and the variables
the plan's settings read. It starts the query, writes its progress every
MAGE_SPARK_PROGRESS_SECONDS (10), and on SIGTERM or SIGINT stops it; the next start
continues from the checkpoint. A query that fails ends the process with 1 and the reason.
Spark stops an older run of the same checkpoint when a new one starts, so a restarted
runner never leaves two.

The plan's settings keep their variable references (`{"$parts": [...]}`); they are filled
in from the environment at start, so an export holds no secret.
"""
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Mapping, Optional

HERE = Path(__file__).parent
# The target of a dead letter in the blocks' output; a sink's is the block it follows.
DEAD_LETTER = '__dead_letter__'
OUTPUT_SCHEMA = ('target string, key binary, value binary, '
                 'headers array<struct<key: string, value: binary>>')
LOGIN_MODULES = {
    'PLAIN': 'org.apache.kafka.common.security.plain.PlainLoginModule',
    'SCRAM-SHA-256': 'org.apache.kafka.common.security.scram.ScramLoginModule',
    'SCRAM-SHA-512': 'org.apache.kafka.common.security.scram.ScramLoginModule',
}


class SparkRunError(Exception):
    pass


def resolve(value: Any, environ: Mapping[str, str], where: str) -> Any:
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
                raise SparkRunError(f'{where} reads the environment variable {name}, which is '
                                    'not set.')
        return text
    if isinstance(value, dict):
        return {k: resolve(v, environ, where) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve(v, environ, where) for v in value]
    return value


def kafka_security(config: Dict[str, Any], where: str) -> Dict[str, str]:
    """security_protocol and sasl_config as Spark's kafka.* options."""
    options = {}
    if config.get('security_protocol'):
        options['kafka.security.protocol'] = str(config['security_protocol'])
    sasl = config.get('sasl_config') or {}
    if sasl:
        mechanism = str(sasl.get('mechanism') or 'PLAIN').upper()
        if mechanism not in LOGIN_MODULES:
            raise SparkRunError(f'{where}: SASL {mechanism} has no Spark equivalent yet; use '
                                f"{', '.join(LOGIN_MODULES)}.")

        def quoted(value: Any) -> str:
            return '"' + str(value or '').replace('\\', '\\\\').replace('"', '\\"') + '"'

        options['kafka.sasl.mechanism'] = mechanism
        options['kafka.sasl.jaas.config'] = (
            f'{LOGIN_MODULES[mechanism]} required username={quoted(sasl.get("username"))} '
            f'password={quoted(sasl.get("password"))};'
        )
    return options


def source_options(plan: Dict[str, Any], environ: Mapping[str, str]) -> Dict[str, str]:
    """Spark's Kafka reader options for the plan's source."""
    config = resolve(plan['source']['config'], environ, 'The source')
    batch = plan['source'].get('batch') or {}
    topics = config.get('topics') or config.get('topic')
    if isinstance(topics, list):
        topics = ','.join(str(t) for t in topics)
    if not topics or not config.get('bootstrap_server'):
        raise SparkRunError('The Kafka source needs bootstrap_server and topic.')
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
    options.update(kafka_security(config, 'The source'))
    return options


def sink_options(sink: Dict[str, Any], environ: Mapping[str, str]) -> Dict[str, str]:
    """Spark's Kafka writer options for one of the plan's sinks."""
    where = f"Sink {sink['name']}"
    config = resolve(sink['config'], environ, where)
    if not config.get('bootstrap_server') or not config.get('topic'):
        raise SparkRunError(f'{where} needs bootstrap_server and topic.')
    return {'kafka.bootstrap.servers': str(config['bootstrap_server']),
            'topic': str(config['topic']), **kafka_security(config, where)}


def trigger_seconds(plan: Dict[str, Any]) -> float:
    return max(float((plan['source'].get('batch') or {}).get('max_wait_ms') or 1000) / 1000,
               0.1)


def blocks_function(plan: Dict[str, Any], worker_source: str) -> Callable:
    """
    The function mapInArrow runs on each partition's batches: Kafka's key, value and headers
    in; out, each sink's messages as JSON (`target`: the block it follows), made by the
    engine's worker code (runtime/worker.py). With a dead-letter sink, a message a block fails
    on goes out as it came, with `mage-stream` and `mage-error` headers, as on the engine;
    when every message of a batch fails, or a block does not load, the batch fails. Everything
    it needs travels in it, so Spark's Python workers need only pyarrow.
    """
    chain = [dict(b) for b in plan['chain']]
    # With a window, the blocks' output is the window's input.
    outputs = list(plan['window']['upstream']) if plan.get('window') else \
        list(dict.fromkeys(s['after'] for s in plan['sinks']))
    variables = dict(plan.get('variables') or {})
    isolate = bool(plan.get('dead_letter'))
    stream_name = plan['pipeline_uuid'].encode()

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
        header = pa.struct([('key', pa.string()), ('value', pa.binary())])
        schema = pa.schema([('target', pa.string()), ('key', pa.binary()),
                            ('value', pa.binary()), ('headers', pa.list_(header))])
        for batch in batches:
            raw = batch.column('value').to_pylist()
            if not raw:
                continue
            messages = []
            for value in raw:
                text = value.decode('utf-8', errors='replace') if value is not None else ''
                try:
                    messages.append(json_module.loads(text))
                except ValueError:
                    # Text that is not JSON reaches the blocks as text, as on the engine.
                    messages.append(text)
            try:
                result = worker.run_stream_batch(dict(
                    chain=request_chain, messages=messages, outputs=outputs,
                    kwargs=variables, isolate=isolate,
                ))
            except worker.BlockError as error:
                raise RuntimeError(
                    f'Block {error.block_uuid} failed ({error.phase}): {error}\n{error.details}'
                ) from None
            rows = dict(target=[], key=[], value=[], headers=[])
            for target in outputs:
                for message in result['outputs'].get(target) or []:
                    rows['target'].append(target)
                    rows['key'].append(None)
                    rows['value'].append(json_module.dumps(
                        message, allow_nan=False, default=worker._json_default).encode())
                    rows['headers'].append(None)
            if result.get('failures'):
                keys = batch.column('key').to_pylist() if 'key' in batch.schema.names \
                    else [None] * len(raw)
                headers = batch.column('headers').to_pylist() \
                    if 'headers' in batch.schema.names else [None] * len(raw)
                for failure in result['failures']:
                    row = failure['row']
                    error = f"{failure.get('block_uuid') or 'a block'}: {failure['message']}"
                    rows['target'].append(DEAD_LETTER)
                    rows['key'].append(keys[row])
                    rows['value'].append(raw[row])
                    rows['headers'].append(list(headers[row] or []) + [
                        dict(key='mage-stream', value=stream_name),
                        dict(key='mage-error', value=error.encode()),
                    ])
            yield pa.RecordBatch.from_pydict(rows, schema=schema)

    return run


# A window's start and end as the engine writes them: UTC, milliseconds, Z.
ISO_MILLIS = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"


def window_frame(frame, plan: Dict[str, Any]):
    """
    The window block as Spark's aggregation (the session's time zone is UTC): the event time
    the stream reads, a watermark its lateness behind, and per key and window the engine's
    aggregates, written as the engine writes them: the key (`key`, or key_as), window_start,
    window_end, then each aggregate. A key is its field's text (a string as it is, other
    values as JSON, "null" when missing); count counts every event, sum is 0.0 without
    numbers, min, max and avg are null without them; a number may be text.
    """
    from pyspark.sql import functions as F

    spec = plan['window']['spec']
    time = plan.get('time') or {}
    value = F.col('value').cast('string')

    def field(column, path):
        return F.get_json_object(column, f'$.{path}')

    if time.get('field'):
        raw = field(value, time['field'])
        if time.get('format') == 'rfc3339':
            event = raw.try_cast('timestamp')
        else:
            number = raw.try_cast('double')
            if time.get('format') == 'epoch_s':
                number = number * 1000
            event = F.timestamp_millis(number.cast('bigint'))
    else:
        event = F.col('timestamp')
    key = F.coalesce(field(value, spec['key']), F.lit('null')) if spec.get('key') \
        else F.lit('')
    events = (
        frame.select(event.alias('_event'), key.alias('_key'), value.alias('_value'))
        .where(F.col('_event').isNotNull())
        .withWatermark('_event', f"{int(time.get('lateness_ms') or 0)} milliseconds")
    )
    kind = spec['kind']
    if kind['type'] == 'tumbling':
        window = F.window('_event', f"{kind['size_ms']} milliseconds")
    elif kind['type'] == 'hopping':
        window = F.window('_event', f"{kind['size_ms']} milliseconds",
                          f"{kind['slide_ms']} milliseconds")
    else:
        window = F.session_window('_event', f"{kind['gap_ms']} milliseconds")
    aggregates = []
    for index, (_name, function) in enumerate(spec['aggregates']):
        if function == 'count':
            aggregate = F.count(F.lit(1))
        else:
            ((kind_of, path),) = function.items()
            text = field(F.col('_value'), path)
            number = text.try_cast('double')
            if kind_of == 'sum':
                aggregate = F.coalesce(F.sum(number), F.lit(0.0))
            elif kind_of == 'min':
                aggregate = F.min(number)
            elif kind_of == 'max':
                aggregate = F.max(number)
            elif kind_of == 'avg':
                aggregate = F.avg(number)
            else:
                aggregate = F.size(F.collect_set(text))
        aggregates.append(aggregate.alias(f'_a{index}'))
    grouped = events.groupBy(window.alias('_window'), F.col('_key')).agg(*aggregates)
    fields = []
    if spec.get('key'):
        fields.append(F.col('_key').alias(spec.get('key_as') or 'key'))
    fields += [
        F.date_format(F.col('_window.start'), ISO_MILLIS).alias('window_start'),
        F.date_format(F.col('_window.end'), ISO_MILLIS).alias('window_end'),
    ]
    fields += [F.col(f'_a{index}').alias(name)
               for index, (name, _) in enumerate(spec['aggregates'])]
    return grouped.select(
        (F.col('_key') if spec.get('key') else F.lit(None).cast('string')).alias('key'),
        F.to_json(F.struct(*fields), {'ignoreNullFields': 'false'}).alias('value'),
    )


def spark_session_class():
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
        raise SparkRunError('Spark Connect needs PySpark: run with uv run --with '
                            '"pyspark[connect]>=4.3", or install mage-ml[spark-connect].') from None
    return SparkSession


def start(plan: Dict[str, Any], remote: str, checkpoint: str,
          environ: Optional[Mapping[str, str]] = None, worker_source: Optional[str] = None):
    """Starts the query on the Spark Connect server; returns (session, query)."""
    environ = os.environ if environ is None else environ
    if not checkpoint:
        raise SparkRunError('A checkpoint location is required: Spark keeps the stream\'s '
                            'positions there.')
    if worker_source is None:
        worker_source = (HERE / 'worker.py').read_text()
    source = source_options(plan, environ)
    targets = [(s['after'], sink_options(s, environ)) for s in plan['sinks']]
    if plan.get('dead_letter'):
        targets.append((DEAD_LETTER, sink_options(plan['dead_letter'], environ)))
    session = spark_session_class()
    spark = session.builder.remote(remote).getOrCreate()
    reader = spark.readStream.format('kafka').option('includeHeaders', 'true')
    for key, value in source.items():
        reader = reader.option(key, value)
    if plan.get('window'):
        # Window boundaries and their text are in UTC, as on the engine.
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        frame = reader.load()
        if plan['window']['upstream']:
            frame = frame.select('key', 'value', 'headers').mapInArrow(
                blocks_function(plan, worker_source), OUTPUT_SCHEMA,
            )
        frame = window_frame(frame, plan)
        if len(targets) == 1:
            writer = frame.writeStream.format('kafka').outputMode('append')
            for key, value in targets[0][1].items():
                writer = writer.option(key, value)
        else:
            writer = frame.writeStream.outputMode('append').foreachBatch(
                write_each([options for _, options in targets]))
        return spark, _started(writer, plan, checkpoint)
    frame = reader.load().select('key', 'value', 'headers').mapInArrow(
        blocks_function(plan, worker_source), OUTPUT_SCHEMA,
    )
    if len(targets) == 1:
        writer = frame.select('value').writeStream.format('kafka')
        for key, value in targets[0][1].items():
            writer = writer.option(key, value)
    else:
        writer = frame.writeStream.foreachBatch(write_targets(targets))
    return spark, _started(writer, plan, checkpoint)


def _started(writer, plan: Dict[str, Any], checkpoint: str):
    return (
        writer.option('checkpointLocation', checkpoint)
        .queryName(plan['pipeline_uuid'])
        .trigger(processingTime=f'{trigger_seconds(plan)} seconds')
        .start()
    )


def write_each(targets):
    """foreachBatch's function for a window's results and several sinks: every sink gets
    every result. A batch that fails is written again: at least once, as on the engine."""
    def write(frame, _batch_id):
        frame.persist()
        try:
            for options in targets:
                writer = frame.write.format('kafka')
                for key, value in options.items():
                    writer = writer.option(key, value)
                writer.save()
        finally:
            frame.unpersist()

    return write


def write_targets(targets):
    """
    foreachBatch's function for several sinks or dead letters: each target's rows written to
    its topic. A batch that fails is written again, to every sink: at least once, as on the
    engine.
    """
    def write(frame, _batch_id):
        frame.persist()
        try:
            for target, options in targets:
                rows = frame.where(frame.target == target)
                rows = rows.select('key', 'value', 'headers') if target == DEAD_LETTER \
                    else rows.select('value')
                writer = rows.write.format('kafka')
                for key, value in options.items():
                    writer = writer.option(key, value)
                writer.save()
        finally:
            frame.unpersist()

    return write


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
    def versions(batches):
        import sys as sys_module

        import pyarrow as pa
        for _ in batches:
            yield pa.RecordBatch.from_pydict({'version': [
                f'{sys_module.version_info[0]}.{sys_module.version_info[1]}',
            ]})

    try:
        found = spark.range(1).mapInArrow(versions, 'version string').collect()[0]['version']
    except Exception as error:
        return f'The server could not run Python with pyarrow: {str(error).splitlines()[0]}'
    mine = f'{sys.version_info[0]}.{sys.version_info[1]}'
    if found != mine:
        return f'The server\'s Python workers run {found}; the blocks need {mine}.'
    return None


def run(plan: Dict[str, Any], remote: str, checkpoint: str,
        environ: Optional[Mapping[str, str]] = None, every: float = 10,
        log: Callable[[str], None] = print, stop: Optional[threading.Event] = None,
        worker_source: Optional[str] = None) -> int:
    """
    Starts the query and watches it until it fails (1) or `stop` is set (0, the query
    stopped). Progress every `every` seconds.
    """
    stop = stop or threading.Event()
    spark, query = start(plan, remote, checkpoint, environ, worker_source)
    problem = python_version_check(spark)
    if problem:
        query.stop()
        log(problem)
        return 1
    log(f"Started {plan['pipeline_uuid']} on {remote} (query {query.id}).")
    last, next_line = None, time.monotonic() + every
    while query.isActive and not stop.wait(0.5):
        if time.monotonic() < next_line:
            continue
        next_line = time.monotonic() + every
        progress = query.lastProgress
        # A line per new batch: an idle stream stays quiet.
        if progress and progress.get('batchId') != last:
            log(progress_line(progress))
            last = progress.get('batchId')
    if stop.is_set():
        query.stop()
        log('Stopped; the next start continues from the checkpoint.')
        return 0
    error = query.exception()
    log(f"The query stopped: {str(error).splitlines()[0] if error else 'without an error'}")
    return 1


def main() -> int:
    plan_path = Path(os.environ.get('MAGE_SPARK_PLAN') or HERE / 'plan.json')
    remote = os.environ.get('MAGE_SPARK_REMOTE')
    checkpoint = os.environ.get('MAGE_SPARK_CHECKPOINT')
    if not remote or not checkpoint:
        print('Set MAGE_SPARK_REMOTE (sc://host:15002) and MAGE_SPARK_CHECKPOINT.',
              file=sys.stderr)
        return 2
    plan = json.loads(plan_path.read_text())
    stop = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_: stop.set())
    try:
        return run(plan, remote, checkpoint, every=float(
            os.environ.get('MAGE_SPARK_PROGRESS_SECONDS') or 10), stop=stop,
            log=lambda line: print(line, flush=True))
    except SparkRunError as error:
        print(str(error), file=sys.stderr, flush=True)
        return 2


if __name__ == '__main__':
    sys.exit(main())
