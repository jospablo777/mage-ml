"""
Fills the playground database with a shop: customers, products, stores, orders, order
items and web events. The data is random but the same on every run (fixed seeds).

    python seed.py            # creates the shop schema once; does nothing if it is there
    python seed.py --force    # drops and creates it again

SEED_SCALE multiplies the row counts (1 by default: 400,000 orders, 1,000,000 events).
The connection comes from the PG* variables (PGHOST, PGUSER, PGPASSWORD, PGDATABASE).
"""
import argparse
import io
import json
import os
import sys
import time
import uuid

import numpy as np
import psycopg2
from faker import Faker

SEED_VERSION = 2
SCALE = float(os.getenv('SEED_SCALE', '1'))
COUNTS = dict(
    customers=int(50_000 * SCALE),
    products=int(2_000 * SCALE) or 1,
    stores=120,
    orders=int(400_000 * SCALE),
    events=int(1_000_000 * SCALE),
)
CHUNK = 200_000

SCHEMA = """
CREATE SCHEMA shop;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE shop.stores (
    store_id integer PRIMARY KEY,
    name text NOT NULL,
    city text NOT NULL,
    country text NOT NULL,
    latitude double precision NOT NULL,
    longitude double precision NOT NULL,
    opened_on date NOT NULL,
    square_meters integer
);

CREATE TABLE shop.customers (
    customer_id bigint PRIMARY KEY,
    full_name text NOT NULL,
    email text NOT NULL,
    city text NOT NULL,
    country text NOT NULL,
    segment text NOT NULL,
    birth_date date,
    signed_up_at timestamptz NOT NULL,
    is_active boolean NOT NULL,
    marketing_opt_in boolean,
    lifetime_value numeric(12, 2) NOT NULL,
    latitude double precision,
    longitude double precision,
    preferred_store_id integer REFERENCES shop.stores
);

CREATE TABLE shop.products (
    product_id integer PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    category text NOT NULL,
    price numeric(10, 2) NOT NULL,
    cost numeric(10, 2) NOT NULL,
    weight_kg real,
    launched_on date NOT NULL,
    tags text[] NOT NULL,
    attributes jsonb NOT NULL
);

CREATE TABLE shop.orders (
    order_id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES shop.customers,
    store_id integer REFERENCES shop.stores,
    ordered_at timestamptz NOT NULL,
    status text NOT NULL,
    channel text NOT NULL,
    discount_pct real NOT NULL,
    shipping_cost numeric(8, 2),
    delivered_on date,
    rating smallint
);

CREATE TABLE shop.order_items (
    order_id bigint NOT NULL REFERENCES shop.orders,
    line_number smallint NOT NULL,
    product_id integer NOT NULL REFERENCES shop.products,
    quantity integer NOT NULL,
    unit_price numeric(10, 2) NOT NULL,
    PRIMARY KEY (order_id, line_number)
);

CREATE TABLE shop.web_events (
    event_id bigint PRIMARY KEY,
    session_id uuid NOT NULL,
    customer_id bigint,
    occurred_at timestamptz NOT NULL,
    event_type text NOT NULL,
    page text NOT NULL,
    device text NOT NULL,
    duration_ms integer,
    scroll_depth double precision,
    properties jsonb
);

CREATE TABLE shop.seed_info (version integer, scale real, seeded_at timestamptz);
"""

INDEXES = """
CREATE INDEX ON shop.orders (customer_id);
CREATE INDEX ON shop.orders (ordered_at);
CREATE INDEX ON shop.order_items (product_id);
CREATE INDEX ON shop.web_events (customer_id);
CREATE INDEX ON shop.web_events (occurred_at);
ANALYZE;
"""

CITIES = [
    ('San José', 'Costa Rica', 9.9281, -84.0907),
    ('Heredia', 'Costa Rica', 9.9981, -84.1170),
    ('Alajuela', 'Costa Rica', 10.0163, -84.2116),
    ('Cartago', 'Costa Rica', 9.8644, -83.9194),
    ('Liberia', 'Costa Rica', 10.6346, -85.4407),
    ('Limón', 'Costa Rica', 9.9907, -83.0360),
    ('Puntarenas', 'Costa Rica', 9.9763, -84.8384),
    ('Panamá', 'Panama', 8.9824, -79.5199),
    ('Managua', 'Nicaragua', 12.1150, -86.2362),
    ('Ciudad de México', 'Mexico', 19.4326, -99.1332),
    ('Bogotá', 'Colombia', 4.7110, -74.0721),
    ('Madrid', 'Spain', 40.4168, -3.7038),
    ('Toronto', 'Canada', 43.6532, -79.3832),
    ('Austin', 'United States', 30.2672, -97.7431),
]
CITY_WEIGHTS = np.array([18, 9, 9, 7, 4, 3, 3, 8, 5, 10, 8, 6, 5, 5], dtype=float)
CATEGORIES = ['Coffee', 'Chocolate', 'Kitchen', 'Outdoor', 'Books', 'Apparel', 'Garden', 'Toys']
TAGS = ['organic', 'fair-trade', 'bestseller', 'new', 'gift', 'eco', 'limited', 'local']
SEGMENTS = ['consumer', 'small business', 'enterprise', 'student']
STATUSES = ['delivered', 'shipped', 'processing', 'cancelled', 'returned']
STATUS_WEIGHTS = np.array([70, 10, 8, 7, 5], dtype=float)
CHANNELS = ['web', 'mobile', 'store', 'phone']
EVENT_TYPES = ['page_view', 'search', 'add_to_cart', 'checkout', 'purchase', 'review']
EVENT_WEIGHTS = np.array([60, 15, 12, 6, 5, 2], dtype=float)
PAGES = ['/', '/search', '/cart', '/checkout', '/account', '/product', '/offers', '/help']
DEVICES = ['desktop', 'iphone', 'android', 'tablet']

START = np.datetime64('2023-01-01T00:00:00')
SPAN_SECONDS = int((np.datetime64('2025-06-30T00:00:00') - START) / np.timedelta64(1, 's'))


def connect():
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        port=int(os.getenv('PGPORT', '5432')),
        user=os.getenv('PGUSER', 'mage'),
        password=os.getenv('PGPASSWORD', 'mage'),
        dbname=os.getenv('PGDATABASE', 'playground'),
    )


def text(value) -> str:
    """A value in COPY text format."""
    if value is None:
        return r'\N'
    if isinstance(value, float) and np.isnan(value):
        return 'NaN'
    return (
        str(value).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
    )


def copy(cursor, table: str, columns, rows) -> None:
    buffer = io.StringIO()
    count = 0
    for row in rows:
        buffer.write('\t'.join(text(value) for value in row))
        buffer.write('\n')
        count += 1
        if count % CHUNK == 0:
            buffer.seek(0)
            cursor.copy_expert(f'COPY {table} ({", ".join(columns)}) FROM STDIN', buffer)
            buffer = io.StringIO()
    buffer.seek(0)
    cursor.copy_expert(f'COPY {table} ({", ".join(columns)}) FROM STDIN', buffer)


def timestamps(rng, count: int) -> np.ndarray:
    # More recent activity: a skew toward the end of the period.
    offsets = (rng.beta(2.2, 1.4, count) * SPAN_SECONDS).astype('int64')
    return START + offsets.astype('timedelta64[s]')


def iso(moment) -> str:
    return f'{str(moment)}+00'


def seed(cursor) -> None:
    fake = Faker(['es_ES', 'en_US'])
    Faker.seed(7)
    rng = np.random.default_rng(7)

    cursor.execute(SCHEMA)

    # Stores around their cities.
    city_index = rng.choice(len(CITIES), COUNTS['stores'], p=CITY_WEIGHTS / CITY_WEIGHTS.sum())
    store_rows = []
    for store_id, index in enumerate(city_index, start=1):
        city, country, lat, lon = CITIES[index]
        store_rows.append((
            store_id,
            f'{city} {fake.street_name()}',
            city,
            country,
            round(lat + rng.normal(0, 0.05), 6),
            round(lon + rng.normal(0, 0.05), 6),
            fake.date_between('-12y', '-1y'),
            None if store_id % 17 == 0 else int(rng.integers(80, 2500)),
        ))
    copy(cursor, 'shop.stores', [
        'store_id', 'name', 'city', 'country', 'latitude', 'longitude', 'opened_on',
        'square_meters',
    ], store_rows)
    print(f'stores: {len(store_rows):,}', flush=True)

    # Customers: names from Faker, the rest drawn in bulk.
    n = COUNTS['customers']
    first_names = [fake.first_name() for _ in range(3000)]
    last_names = [fake.last_name() for _ in range(3000)]
    domains = ['gmail.com', 'outlook.com', 'yahoo.com', 'proton.me', 'empresa.cr', 'icloud.com']
    city_index = rng.choice(len(CITIES), n, p=CITY_WEIGHTS / CITY_WEIGHTS.sum())
    signup = timestamps(rng, n)
    lifetime = np.round(rng.lognormal(5.5, 1.1, n), 2)
    birth_days = rng.integers(-25_000, -6_600, n)
    stores_by_city = {}
    for row in store_rows:
        stores_by_city.setdefault(row[2], []).append(row[0])

    def customers():
        for i in range(n):
            first = first_names[rng.integers(len(first_names))]
            last = last_names[rng.integers(len(last_names))]
            city, country, lat, lon = CITIES[city_index[i]]
            local = f'{first}.{last}{i}'.lower().replace(' ', '')
            nearby = stores_by_city.get(city)
            yield (
                i + 1,
                f'{first} {last}',
                f'{local}@{domains[i % len(domains)]}',
                city,
                country,
                SEGMENTS[int(rng.choice(4, p=[0.62, 0.22, 0.06, 0.10]))],
                None if i % 23 == 0 else str(np.datetime64('2026-01-01') + birth_days[i]),
                iso(signup[i]),
                bool(rng.random() < 0.86),
                None if i % 11 == 0 else bool(rng.random() < 0.4),
                f'{lifetime[i]:.2f}',
                None if i % 29 == 0 else round(lat + rng.normal(0, 0.08), 6),
                None if i % 29 == 0 else round(lon + rng.normal(0, 0.08), 6),
                int(nearby[i % len(nearby)]) if nearby and i % 5 else None,
            )

    copy(cursor, 'shop.customers', [
        'customer_id', 'full_name', 'email', 'city', 'country', 'segment', 'birth_date',
        'signed_up_at', 'is_active', 'marketing_opt_in', 'lifetime_value', 'latitude',
        'longitude', 'preferred_store_id',
    ], customers())
    print(f'customers: {n:,}', flush=True)

    # Products.
    m = COUNTS['products']
    prices = np.round(rng.lognormal(3.2, 0.8, m), 2)

    def products():
        for i in range(m):
            category = CATEGORIES[i % len(CATEGORIES)]
            tags = sorted(set(rng.choice(TAGS, int(rng.integers(0, 4))).tolist()))
            attributes = dict(
                color=fake.color_name(),
                origin=CITIES[int(rng.integers(len(CITIES)))][1],
                rating=round(float(rng.uniform(2.5, 5)), 1),
            )
            if i % 4 == 0:
                attributes['dimensions_cm'] = [int(x) for x in rng.integers(5, 80, 3)]
            yield (
                i + 1,
                f'{category[:3].upper()}-{i + 1:05d}',
                f'{fake.word().capitalize()} {category.lower()} {fake.word()}',
                category,
                f'{prices[i]:.2f}',
                f'{prices[i] * rng.uniform(0.35, 0.8):.2f}',
                None if i % 13 == 0 else round(float(rng.uniform(0.05, 25)), 3),
                fake.date_between('-6y', '-30d'),
                '{' + ','.join(tags) + '}',
                json.dumps(attributes, ensure_ascii=False),
            )

    copy(cursor, 'shop.products', [
        'product_id', 'sku', 'name', 'category', 'price', 'cost', 'weight_kg',
        'launched_on', 'tags', 'attributes',
    ], products())
    print(f'products: {m:,}', flush=True)

    # Orders and their items.
    o = COUNTS['orders']
    # Each order falls between its customer's sign-up and the end of the period; order
    # ids follow time.
    signup_seconds = ((signup - START) / np.timedelta64(1, 's')).astype('int64')
    order_customers = rng.zipf(1.3, o) % n + 1
    first = signup_seconds[order_customers - 1]
    offsets = first + (rng.beta(1.6, 1.0, o) * (SPAN_SECONDS - first)).astype('int64')
    by_time = np.argsort(offsets, kind='stable')
    order_customers = order_customers[by_time]
    ordered = START + offsets[by_time].astype('timedelta64[s]')
    statuses = rng.choice(len(STATUSES), o, p=STATUS_WEIGHTS / STATUS_WEIGHTS.sum())
    channels = rng.choice(len(CHANNELS), o, p=[0.48, 0.32, 0.15, 0.05])
    product_popularity = rng.zipf(1.5, 4 * o) % m + 1

    def orders():
        for i in range(o):
            status = STATUSES[statuses[i]]
            channel = CHANNELS[channels[i]]
            delivered = None
            if status in ('delivered', 'returned'):
                delivered = str((ordered[i] + np.timedelta64(int(rng.integers(1, 12)), 'D'))
                                .astype('datetime64[D]'))
            yield (
                i + 1,
                int(order_customers[i]),
                int(rng.integers(1, COUNTS['stores'] + 1)) if channel == 'store' else None,
                iso(ordered[i]),
                status,
                channel,
                float(rng.choice([0, 0, 0, 5, 10, 15, 25])),
                None if channel == 'store' else f'{rng.uniform(0, 18):.2f}',
                delivered,
                int(rng.integers(1, 6))
                if status == 'delivered' and rng.random() < 0.35 else None,
            )

    copy(cursor, 'shop.orders', [
        'order_id', 'customer_id', 'store_id', 'ordered_at', 'status', 'channel',
        'discount_pct', 'shipping_cost', 'delivered_on', 'rating',
    ], orders())
    print(f'orders: {o:,}', flush=True)

    lines_per_order = rng.integers(1, 6, o)
    items_count = int(lines_per_order.sum())

    def items():
        cursor_index = 0
        for i in range(o):
            for line in range(1, lines_per_order[i] + 1):
                product = int(product_popularity[cursor_index % len(product_popularity)])
                cursor_index += 1
                yield (
                    i + 1,
                    line,
                    product,
                    int(rng.choice([1, 1, 1, 2, 2, 3, 4, 6])),
                    f'{prices[product - 1]:.2f}',
                )

    copy(cursor, 'shop.order_items', [
        'order_id', 'line_number', 'product_id', 'quantity', 'unit_price',
    ], items())
    print(f'order items: {items_count:,}', flush=True)

    # Web events in sessions.
    e = COUNTS['events']
    occurred = np.sort(timestamps(rng, e))
    types = rng.choice(len(EVENT_TYPES), e, p=EVENT_WEIGHTS / EVENT_WEIGHTS.sum())
    session_customers = rng.integers(1, n + 1, e // 6 + 1)
    durations = rng.lognormal(8, 1.2, e).astype('int64')

    def events():
        session = uuid.UUID(int=int(rng.integers(1, 2**62)))
        customer = None
        for i in range(e):
            if i % 6 == 0:
                session = uuid.UUID(int=int(rng.integers(1, 2**62)) << 64 | i)
                customer = int(session_customers[i // 6]) if rng.random() < 0.7 else None
                # Visits before a customer signed up are anonymous.
                if customer is not None and occurred[i] < signup[customer - 1]:
                    customer = None
            event_type = EVENT_TYPES[types[i]]
            properties = None
            if event_type == 'search':
                properties = json.dumps(
                    {'query': fake.word(), 'results': int(rng.integers(0, 80))},
                )
            elif event_type in ('add_to_cart', 'purchase'):
                product = int(product_popularity[i])
                properties = json.dumps(
                    {'product_id': product, 'value': round(float(prices[product - 1]), 2)},
                )
            yield (
                i + 1,
                str(session),
                customer,
                iso(occurred[i]),
                event_type,
                PAGES[i % len(PAGES)] if event_type == 'page_view' else f'/{event_type}',
                DEVICES[int(rng.choice(4, p=[0.45, 0.25, 0.22, 0.08]))],
                None if i % 37 == 0 else int(durations[i]),
                float('nan') if i % 101 == 0 else round(float(rng.beta(2, 2)), 4),
                properties,
            )

    copy(cursor, 'shop.web_events', [
        'event_id', 'session_id', 'customer_id', 'occurred_at', 'event_type', 'page',
        'device', 'duration_ms', 'scroll_depth', 'properties',
    ], events())
    print(f'web events: {e:,}', flush=True)

    cursor.execute(INDEXES)
    cursor.execute(
        'INSERT INTO shop.seed_info VALUES (%s, %s, now())', (SEED_VERSION, SCALE),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    for _ in range(60):
        try:
            connection = connect()
            break
        except psycopg2.OperationalError:
            time.sleep(1)
    else:
        print('The database did not accept connections.', file=sys.stderr)
        return 1

    with connection, connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('shop.seed_info') IS NOT NULL")
        if cursor.fetchone()[0] and not args.force:
            cursor.execute('SELECT version, scale, seeded_at FROM shop.seed_info')
            version, scale, seeded_at = cursor.fetchone()
            print(f'The shop schema is seeded: version {version}, scale {scale}, at {seeded_at}.')
            if version != SEED_VERSION:
                print(f'This seed writes version {SEED_VERSION}; make playground-reseed '
                      'replaces the shop schema with it.')
            return 0
        cursor.execute('DROP SCHEMA IF EXISTS shop CASCADE')
        started = time.monotonic()
        seed(cursor)
    connection.close()
    print(f'Seeded in {time.monotonic() - started:.0f} s.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
