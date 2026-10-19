"""
Stream examples (C5): `mage examples stream <name>` writes a runnable streaming pipeline into a
project, with a sample, and `mage examples generate <name>` sends synthetic messages to its
source, so every example runs without real data.

Connection settings read the environment (KAFKA_BOOTSTRAP, NATS_URL), as an exported service
needs them.
"""
import json
import random
import textwrap
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterator, List, Optional, Tuple


@dataclass
class Example:
    title: str
    teaches: str
    # (name, block type, language, content, upstream names)
    blocks: List[Tuple[str, str, str, str, List[str]]]
    # One synthetic message; the generator calls it with a random generator and a counter.
    message: Callable[[random.Random, int], Dict]
    # Where `generate` sends: ('kafka', topic) or ('nats', subject).
    target: Tuple[str, str]
    sample_size: int = 20
    # Files the example writes besides its blocks, by path in the project.
    files: Dict[str, Callable[[str], None]] = field(default_factory=dict)
    variables: List[str] = field(default_factory=list)
    # The sample's messages when they differ from what `generate` sends (a change source
    # turns rows into changes).
    sample_message: Optional[Callable[[random.Random, int], Dict]] = None


def _kafka_source(topic: str, group: str) -> str:
    return f'''
        connector_type: kafka
        bootstrap_server: "{{{{ env_var('KAFKA_BOOTSTRAP') }}}}"
        topic: {topic}
        consumer_group: {group}
        auto_offset_reset: earliest
        batch_size: 500
    '''


def _kafka_sink(topic: str, extra: str = '') -> str:
    return (
        'connector_type: kafka\n'
        'bootstrap_server: "{{ env_var(\'KAFKA_BOOTSTRAP\') }}"\n'
        f'topic: {topic}\n'
    ) + extra


# Order events to clean facts.

def _order(rng: random.Random, n: int) -> Dict:
    order = dict(order_id=n, customer=f'c-{rng.randint(1, 500)}', currency='EUR',
                 items=[dict(sku=f'sku-{rng.randint(1, 80)}', quantity=rng.randint(1, 4),
                             unit_price=round(rng.uniform(2, 120), 2))
                        for _ in range(rng.randint(1, 3))])
    # About one order in fifty is broken, so dead letters have something to show.
    if rng.random() < 0.02:
        order['items'][0]['quantity'] = -1
    return order


PARSE_ORDERS = '''
from typing import Dict, List


@transformer
def parse_orders(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """
    One clean fact per order. An order with a negative quantity raises: with the dead-letter
    sink, that order goes there with the error and the others flow on.
    """
    facts = []
    for order in messages:
        for item in order['items']:
            if item['quantity'] <= 0:
                raise ValueError(f"order {order['order_id']}: quantity {item['quantity']}")
        total = sum(i['quantity'] * i['unit_price'] for i in order['items'])
        facts.append(dict(
            order_id=order['order_id'],
            customer=order['customer'],
            items=len(order['items']),
            total_cents=round(total * 100),
            currency=order['currency'],
        ))
    return facts
'''


# IoT telemetry with branches.

def _reading(rng: random.Random, n: int) -> Dict:
    device = f'd-{rng.randint(1, 40)}'
    temperature = round(rng.gauss(22, 4), 2)
    if rng.random() < 0.03:
        temperature = round(rng.uniform(45, 80), 2)
    return dict(device=device, seq=n, temperature=temperature,
                humidity=round(rng.uniform(20, 80), 1), ts=int(time.time() * 1000))


CLEAN_READINGS = '''
from typing import Dict, List


@transformer
def clean_readings(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """Readings with a device and a plausible temperature, in Celsius and Fahrenheit."""
    return [
        dict(m, temperature_f=round(m['temperature'] * 9 / 5 + 32, 2))
        for m in messages
        if isinstance(m, dict) and m.get('device') and -40 <= m['temperature'] <= 125
    ]
'''

ALERTS = '''
from typing import Dict, List

THRESHOLD = 40.0


@transformer
def alerts(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """A second branch: only the readings above the threshold, as alerts."""
    return [
        dict(device=m['device'], seq=m['seq'], temperature=m['temperature'],
             reason=f"above {THRESHOLD} C")
        for m in messages if m['temperature'] > THRESHOLD
    ]
'''


def _nats_sink(subject: str) -> str:
    return f'''
        connector_type: nats
        server_url: "{{{{ env_var('NATS_URL') }}}}"
        subject: {subject}
    '''


# Payment risk scoring.

FEATURES = ['amount', 'hour', 'new_device', 'foreign', 'attempts']


def _payment(rng: random.Random, n: int) -> Dict:
    risky = rng.random() < 0.05
    return dict(
        payment_id=n,
        customer=f'c-{rng.randint(1, 2000)}',
        amount=round(rng.lognormvariate(4.5 if risky else 3.2, 0.8), 2),
        hour=rng.choice(range(0, 6)) if risky and rng.random() < 0.7 else rng.randint(0, 23),
        new_device=int(rng.random() < (0.6 if risky else 0.05)),
        foreign=int(rng.random() < (0.5 if risky else 0.08)),
        attempts=rng.randint(2, 6) if risky else 1,
    )


def _train_risk_model(path: str) -> None:
    """A logistic regression fit on seeded synthetic payments; labels follow the generator."""
    import joblib
    from sklearn.linear_model import LogisticRegression

    rng = random.Random(7)
    rows, labels = [], []
    for n in range(4000):
        risky = rng.random() < 0.05
        payment = _payment(random.Random(n * 31 + int(risky)), n)
        if risky:
            payment.update(amount=payment['amount'] * 4, new_device=1, attempts=4)
        rows.append([payment[f] for f in FEATURES])
        labels.append(int(risky))
    model = LogisticRegression(max_iter=1000).fit(rows, labels)
    joblib.dump(model, path)


SCORE_PAYMENTS = '''
from typing import Dict, List

from mage_ai.pipeline_services.models import load_artifact

FEATURES = ['amount', 'hour', 'new_device', 'foreign', 'attempts']
# Loaded once per worker and kept: each batch only scores.
MODEL = load_artifact('models/payments_risk.joblib')


@transformer
def score_payments(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    """The risk of each payment, scored in one call per batch."""
    if not messages:
        return []
    rows = [[m[f] for f in FEATURES] for m in messages]
    risks = MODEL.predict_proba(rows)[:, 1]
    return [
        dict(payment_id=m['payment_id'], customer=m['customer'], amount=m['amount'],
             risk=round(float(r), 4), review=bool(r > 0.5))
        for m, r in zip(messages, risks)
    ]
'''


# Clickstream to PostgreSQL.

PAGES = ['/', '/search', '/product', '/cart', '/checkout', '/account']


def _click(rng: random.Random, n: int) -> Dict:
    return dict(event_id=f'e-{n}', user=f'u-{rng.randint(1, 300)}', page=rng.choice(PAGES),
                ts=int(time.time() * 1000) - rng.randint(0, 3_600_000))


SESSIONIZE = """
from typing import Dict, List

SESSION_MS = 30 * 60 * 1000


@transformer
def sessionize(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    \"\"\"
    Each click with the session it belongs to, without state: the user and the 30-minute
    window of its time. The sink upserts by event_id, so a replayed batch writes nothing twice.
    \"\"\"
    return [
        dict(event_id=m['event_id'], user_id=m['user'], page=m['page'], ts=m['ts'],
             session_key=f"{m['user']}-{m['ts'] // SESSION_MS}")
        for m in messages
    ]
"""

PG_SINK = """
connector_type: postgres
host: "{{ env_var('PGHOST') }}"
port: "{{ env_var('PGPORT') }}"
database: "{{ env_var('PGDATABASE') }}"
username: "{{ env_var('PGUSER') }}"
password: "{{ env_var('PGPASSWORD') }}"
schema: public
table: click_events
unique_constraints: [event_id]
unique_conflict_method: UPDATE
"""


# Multi-language stream.

PARSE_EVENTS = """
from typing import Dict, List


@transformer
def parse(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    \"\"\"Python: keeps the orders with items and adds their quantity.\"\"\"
    return [
        dict(order_id=m['order_id'], customer=m['customer'],
             quantity=sum(i['quantity'] for i in m['items']),
             amount=sum(i['quantity'] * i['unit_price'] for i in m['items']))
        for m in messages if m.get('items') and all(i['quantity'] > 0 for i in m['items'])
    ]
"""

PRICE_RUST = """
use mage::prelude::*;

// Rust: the tax and the price band, one compiled pass per batch.
fn transform(messages: Vec<Value>, _vars: Vars) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(messages.len());
    for mut message in messages {
        let amount = message["amount"].as_f64().ok_or_else(|| anyhow!("no amount"))?;
        message["total"] = json!((amount * 1.21 * 100.0).round() / 100.0);
        message["band"] = json!(if amount >= 100.0 { "high" } else { "standard" });
        out.push(message);
    }
    Ok(out)
}
"""


# Database change capture to Kafka.

def _shop_order(rng: random.Random, n: int) -> Dict:
    return dict(id=n, customer=f'c-{rng.randint(1, 500)}', status=rng.choice(
        ['placed', 'paid', 'shipped']), amount=round(rng.uniform(5, 300), 2))


def _shop_order_change(rng: random.Random, n: int) -> Dict:
    """A row inserted into public.shop_orders, as the change source hands it over."""
    return dict(_shop_order(rng, n), _mage_operation='insert', _mage_schema='public',
                _mage_table='shop_orders', _mage_lsn=f'0/16B{3740 + n:04X}',
                _mage_commit_time='2026-10-18T10:00:00Z')


CHANGES_TO_EVENTS = """
from typing import Dict, List


@transformer
def to_events(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    \"\"\"
    One event per changed row: the operation, the row's columns and where in the log it
    happened. A transaction's changes arrive whole, in commit order.
    \"\"\"
    return [
        dict(op=m['_mage_operation'], table=f"{m['_mage_schema']}.{m['_mage_table']}",
             id=m['id'], status=m.get('status'), amount=m.get('amount'),
             lsn=m['_mage_lsn'], committed_at=m['_mage_commit_time'])
        for m in messages
    ]
"""

CDC_SOURCE = """
connector_type: postgres
host: "{{ env_var('PGHOST') }}"
port: "{{ env_var('PGPORT') }}"
database: "{{ env_var('PGDATABASE') }}"
user: "{{ env_var('PGUSER') }}"
password: "{{ env_var('PGPASSWORD') }}"
# Runs from the editor read with <slot>_dev_<user>; drop it when done, it holds WAL.
replication_slot: shop_orders_cdc
publication_name: shop_orders_cdc
tables:
  - public.shop_orders
create_slot: true
"""


# Webhooks to a queue.

EVENT_TYPES = ['signup', 'login', 'purchase', 'refund', 'logout']


def _webhook_event(rng: random.Random, n: int) -> Dict:
    event = dict(event_id=f'evt-{n}', type=rng.choice(EVENT_TYPES),
                 user=f'u-{rng.randint(1, 1000)}', at=int(time.time() * 1000))
    if rng.random() < 0.03:
        del event['type']
    return event


ROUTE_EVENTS = """
import time
from typing import Dict, List


@transformer
def route_events(messages: List[Dict], *args, **kwargs) -> List[Dict]:
    \"\"\"
    Events with a type, with the time they were routed. A sender is answered only once the
    queue took its batch, and its Idempotency-Key makes a retry write nothing twice.
    \"\"\"
    now = int(time.time() * 1000)
    return [dict(m, routed_at=now) for m in messages if m.get('type')]
"""

WEBHOOK_SOURCE = """
# Senders POST JSON events to /v1/streams/<pipeline>/messages with the ingest token,
# which can only send messages.
connector_type: webhook
answer_within_seconds: 30
dedupe_window_seconds: 86400
"""

RABBIT_SINK = """
connector_type: rabbitmq
connection_host: "{{ env_var('RABBITMQ_HOST') }}"
connection_port: "{{ env_var('RABBITMQ_PORT') }}"
username: "{{ env_var('RABBITMQ_USER') }}"
password: "{{ env_var('RABBITMQ_PASSWORD') }}"
queue_name: events
"""


EXAMPLES: Dict[str, Example] = {
    'orders': Example(
        title='Order events to clean facts',
        teaches='Parse and validate orders; a broken order goes to dead letters with its '
                'error and the others flow on; replay after the fix.',
        blocks=[
            ('orders', 'data_loader', 'yaml', _kafka_source('orders', 'orders-clean'), []),
            ('parse_orders', 'transformer', 'python', PARSE_ORDERS, ['orders']),
            ('orders_clean', 'data_exporter', 'yaml', _kafka_sink('orders_clean'),
             ['parse_orders']),
            ('orders_dead_letters', 'data_exporter', 'yaml',
             _kafka_sink('orders_dead_letters', 'dead_letter: true\n'),
             ['parse_orders']),
        ],
        message=_order,
        target=('kafka', 'orders'),
        variables=['KAFKA_BOOTSTRAP'],
    ),
    'iot': Example(
        title='IoT telemetry with branches',
        teaches='A NATS JetStream source, two branches after one block, and a sink on each.',
        blocks=[
            ('telemetry', 'data_loader', 'yaml', '''
                connector_type: nats
                server_url: "{{ env_var('NATS_URL') }}"
                stream_name: TELEMETRY
                subject: telemetry.readings
                consumer_name: telemetry-pipeline
                batch_size: 200
            ''', []),
            ('clean_readings', 'transformer', 'python', CLEAN_READINGS, ['telemetry']),
            ('alerts', 'transformer', 'python', ALERTS, ['clean_readings']),
            ('readings_clean', 'data_exporter', 'yaml', _nats_sink('telemetry_out.clean'),
             ['clean_readings']),
            ('readings_alerts', 'data_exporter', 'yaml', _nats_sink('telemetry_out.alerts'),
             ['alerts']),
        ],
        message=_reading,
        target=('nats', 'telemetry.readings'),
        variables=['NATS_URL'],
    ),
    'payments': Example(
        title='Real-time payment risk scoring',
        teaches='A model loaded once per worker and kept, scoring each batch in one call.',
        blocks=[
            ('payments', 'data_loader', 'yaml', _kafka_source('payments', 'payments-risk'), []),
            ('score_payments', 'transformer', 'python', SCORE_PAYMENTS, ['payments']),
            ('scores', 'data_exporter', 'yaml', _kafka_sink('payment_scores'),
             ['score_payments']),
        ],
        message=_payment,
        target=('kafka', 'payments'),
        files={'models/payments_risk.joblib': _train_risk_model},
        variables=['KAFKA_BOOTSTRAP'],
    ),
    'clickstream': Example(
        title='Clickstream to PostgreSQL',
        teaches='Upserts by an event id: a replayed batch writes nothing twice, so the sink is '
                'idempotent and replicas can scale out.',
        blocks=[
            ('clicks', 'data_loader', 'yaml', _kafka_source('clicks', 'clickstream'), []),
            ('sessionize', 'transformer', 'python', SESSIONIZE, ['clicks']),
            ('click_events', 'data_exporter', 'yaml', PG_SINK, ['sessionize']),
        ],
        message=_click,
        target=('kafka', 'clicks'),
        variables=['KAFKA_BOOTSTRAP', 'PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER',
                   'PGPASSWORD'],
    ),
    'cdc': Example(
        title='Database change capture to Kafka',
        teaches='Changes of a PostgreSQL table by logical replication, transactions kept '
                'whole, the slot holding the position.',
        blocks=[
            ('shop_orders', 'data_loader', 'yaml', CDC_SOURCE, []),
            ('to_events', 'transformer', 'python', CHANGES_TO_EVENTS, ['shop_orders']),
            ('order_changes', 'data_exporter', 'yaml', _kafka_sink('order_changes'),
             ['to_events']),
        ],
        message=_shop_order,
        sample_message=_shop_order_change,
        target=('postgres', 'public.shop_orders'),
        variables=['PGHOST', 'PGPORT', 'PGDATABASE', 'PGUSER', 'PGPASSWORD', 'KAFKA_BOOTSTRAP'],
    ),
    'webhooks': Example(
        title='Webhooks to a queue',
        teaches='Senders answered once the queue took their events; an Idempotency-Key makes '
                'a retry safe; a send-only token.',
        blocks=[
            ('events', 'data_loader', 'yaml', WEBHOOK_SOURCE, []),
            ('route_events', 'transformer', 'python', ROUTE_EVENTS, ['events']),
            ('queue', 'data_exporter', 'yaml', RABBIT_SINK, ['route_events']),
        ],
        message=_webhook_event,
        target=('http', 'events'),
        variables=['RABBITMQ_HOST', 'RABBITMQ_PORT', 'RABBITMQ_USER', 'RABBITMQ_PASSWORD'],
    ),
    'multilang': Example(
        title='Multi-language stream',
        teaches='A Python block and a Rust block: two runs of one language each, one boundary '
                'where messages cross as JSON (see the Compile panel).',
        blocks=[
            ('orders', 'data_loader', 'yaml', _kafka_source('orders_multi', 'multilang'), []),
            ('parse', 'transformer', 'python', PARSE_EVENTS, ['orders']),
            ('price', 'transformer', 'rust', PRICE_RUST, ['parse']),
            ('priced', 'data_exporter', 'yaml', _kafka_sink('orders_priced'), ['price']),
        ],
        message=_order,
        target=('kafka', 'orders_multi'),
        variables=['KAFKA_BOOTSTRAP'],
    ),
}


class ExampleError(ValueError):
    pass


DECORATOR_IMPORT = (
    "if 'transformer' not in globals():\n"
    "    from mage_ai.data_preparation.decorators import transformer\n\n\n"
)


def _with_decorator_import(code: str) -> str:
    """A Python block as MageML writes its own: the decorator imported when the file is not
    run by MageML, so linters and a plain `python` find it."""
    if '@transformer' not in code or "'transformer' not in globals()" in code:
        return code
    at = code.index('@transformer')
    # One blank line between the imports (or constants) and the guard, as isort keeps it.
    head = code[:at].rstrip('\n')
    return (head + '\n\n' if head else '') + DECORATOR_IMPORT + code[at:]


def write(repo_path: str, name: str, pipeline_name: Optional[str] = None) -> Dict:
    """Writes the example's pipeline, its files and a sample into the project."""
    import os

    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.constants import PipelineType
    from mage_ai.data_preparation.models.pipeline import Pipeline
    from mage_ai.streaming.samples import write_sample

    if name not in EXAMPLES:
        raise ExampleError(f'No stream example {name}; there are {", ".join(EXAMPLES)}.')
    example = EXAMPLES[name]
    pipeline_uuid = pipeline_name or f'{name}_stream'
    if os.path.isdir(os.path.join(repo_path, 'pipelines', pipeline_uuid)):
        raise ExampleError(f'The project has a pipeline {pipeline_uuid}; name another with '
                           '--pipeline.')
    for path, make in example.files.items():
        absolute = os.path.join(repo_path, path)
        os.makedirs(os.path.dirname(absolute), exist_ok=True)
        make(absolute)
    pipeline = Pipeline.create(pipeline_uuid, repo_path=repo_path,
                               pipeline_type=PipelineType.STREAMING,
                               description=example.title)
    uuids = {}
    for block_name, kind, language, content, upstream in example.blocks:
        block = Block.create(f'{pipeline_uuid}_{block_name}', kind, repo_path,
                             language=language)
        code = textwrap.dedent(content).lstrip()
        if language == 'python':
            code = _with_decorator_import(code)
        with open(block.file_path, 'w') as file:
            file.write(code)
        pipeline.add_block(block, upstream_block_uuids=[uuids[u] for u in upstream])
        uuids[block_name] = block.uuid
    rng = random.Random(42)
    make = example.sample_message or example.message
    sample = [make(rng, n) for n in range(1, example.sample_size + 1)]
    write_sample(repo_path, pipeline_uuid, 'example', sample)
    return dict(pipeline=pipeline_uuid, blocks=list(uuids.values()),
                files=list(example.files), variables=example.variables)


def messages(name: str, count: int, seed: Optional[int] = None) -> Iterator[Dict]:
    rng = random.Random(seed)
    for n in range(1, count + 1):
        yield EXAMPLES[name].message(rng, n)


def generate(name: str, rate: float = 50, seconds: float = 60, count: Optional[int] = None,
             seed: Optional[int] = None, bootstrap: Optional[str] = None,
             nats_url: Optional[str] = None, on_sent: Callable[[int], None] = None,
             target: Optional[str] = None, project: Optional[str] = None,
             pipeline: Optional[str] = None) -> int:
    """
    Sends synthetic messages to the example's source at `rate` per second; returns how many.
    `target` names another topic or subject.
    """
    import os

    if name not in EXAMPLES:
        raise ExampleError(f'No stream example {name}; there are {", ".join(EXAMPLES)}.')
    kind, where = EXAMPLES[name].target
    where = target or where
    total = count if count is not None else max(1, int(rate * seconds))
    if kind == 'postgres':
        return _insert_rows(name, where, total, rate, seed, on_sent)
    if kind == 'http':
        return _post_events(name, project or '.', pipeline or f'{name}_stream', total, rate,
                            seed, on_sent)
    if kind == 'kafka':
        from kafka import KafkaProducer

        server = bootstrap or os.environ.get('KAFKA_BOOTSTRAP')
        if not server:
            raise ExampleError('Set KAFKA_BOOTSTRAP or pass --bootstrap.')
        producer = KafkaProducer(bootstrap_servers=server)
        send = lambda data: producer.send(where, data)  # noqa: E731
        flush, close = producer.flush, producer.close
    else:
        send, flush, close = _nats_sender(nats_url or os.environ.get('NATS_URL'), where)
    sent = 0
    started = time.monotonic()
    try:
        for message in messages(name, total, seed):
            send(json.dumps(message).encode())
            sent += 1
            if rate and sent % max(1, int(rate // 10) or 1) == 0:
                flush()
                if on_sent:
                    on_sent(sent)
                ahead = sent / rate - (time.monotonic() - started)
                if ahead > 0:
                    time.sleep(ahead)
        flush()
    finally:
        close()
    return sent


def _post_events(name: str, project: str, pipeline: str, total: int, rate: float,
                 seed: Optional[int], on_sent: Optional[Callable[[int], None]]) -> int:
    """
    Posts the example's events to the stream running on the local engine (the Run tab), in
    batches of up to 50, each with its Idempotency-Key and the run's send-only token.
    """
    import os
    import urllib.request

    folder = os.path.join(project, '.mage', 'dev', pipeline)
    try:
        port = open(os.path.join(folder, 'port')).read().strip()
        token = open(os.path.join(folder, 'ingest_token')).read().strip()
    except FileNotFoundError:
        raise ExampleError(f'{pipeline} is not running on the local engine: start it from '
                           'its Run tab.') from None
    url = f'http://127.0.0.1:{port}/v1/streams/{pipeline}/messages'
    batch, sent, started = [], 0, time.monotonic()

    def post(events):
        request = urllib.request.Request(url, data=json.dumps(events).encode(), headers={
            'Authorization': f'Bearer {token}', 'Content-Type': 'application/json',
            'Idempotency-Key': f"{events[0]['event_id']}..{events[-1]['event_id']}",
        }, method='POST')
        for attempt in range(5):
            try:
                with urllib.request.urlopen(request, timeout=60):
                    return
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(1 + attempt)

    for event in messages(name, total, seed):
        batch.append(event)
        if len(batch) == 50:
            post(batch)
            sent += len(batch)
            batch = []
            if on_sent:
                on_sent(sent)
            if rate:
                ahead = sent / rate - (time.monotonic() - started)
                if ahead > 0:
                    time.sleep(ahead)
    if batch:
        post(batch)
        sent += len(batch)
    return sent


def _insert_rows(name: str, table: str, total: int, rate: float, seed: Optional[int],
                 on_sent: Optional[Callable[[int], None]]) -> int:
    """Inserts the example's rows into a PostgreSQL table (PG* variables), one commit each."""
    import os

    import psycopg2

    missing = [v for v in ('PGHOST', 'PGDATABASE', 'PGUSER') if not os.environ.get(v)]
    if missing:
        raise ExampleError(f'Set {", ".join(missing)} (and PGPORT, PGPASSWORD).')
    connection = psycopg2.connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS {table} (id bigint PRIMARY KEY, customer text, '
                'status text, amount numeric(12, 2))'
            )
        connection.commit()
        started = time.monotonic()
        sent = 0
        for row in messages(name, total, seed):
            with connection.cursor() as cursor:
                cursor.execute(
                    f'INSERT INTO {table} (id, customer, status, amount) '
                    'VALUES (%(id)s, %(customer)s, %(status)s, %(amount)s) '
                    'ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, '
                    'amount = EXCLUDED.amount', row,
                )
            connection.commit()
            sent += 1
            if on_sent:
                on_sent(sent)
            if rate:
                ahead = sent / rate - (time.monotonic() - started)
                if ahead > 0:
                    time.sleep(ahead)
        return sent
    finally:
        connection.close()


def _nats_sender(url: Optional[str], subject: str):
    """Publishes to a JetStream subject, creating the example's streams when missing."""
    import asyncio

    import nats
    from nats.js.errors import NotFoundError

    if not url:
        raise ExampleError('Set NATS_URL or pass --nats-url.')
    loop = asyncio.new_event_loop()
    connection = loop.run_until_complete(nats.connect(url))
    js = connection.jetstream()

    async def ensure():
        for stream, subjects in (('TELEMETRY', ['telemetry.readings']),
                                 ('TELEMETRY_OUT', ['telemetry_out.>'])):
            try:
                await js.stream_info(stream)
            except NotFoundError:
                await js.add_stream(name=stream, subjects=subjects)

    loop.run_until_complete(ensure())
    pending = []

    def send(data: bytes):
        pending.append(js.publish(subject, data))

    async def publish_all(coroutines):
        await asyncio.gather(*coroutines)

    def flush():
        if pending:
            loop.run_until_complete(publish_all(list(pending)))
            pending.clear()

    def close():
        flush()
        loop.run_until_complete(connection.close())
        loop.close()

    return send, flush, close
