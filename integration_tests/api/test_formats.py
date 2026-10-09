"""
Data frames pulled from and pushed to a REST API in every common exchange format.

The service in integration_tests/services/api serves the orders dataset and stores what
clients send, returning a checksum per column. The tables below pin what each pandas
and Polars reader and writer keeps and loses; a library upgrade that changes one fails
here.
"""
import io

import pandas as pd
import polars as pl
import pytest
import requests

from integration_tests.api.canonical import mismatched_columns
from integration_tests.services.api.app import datasets, formats

ROWS = 60
JSON_COLUMNS = [c for c in datasets.SCHEMA if c != 'payload']

# Reader: (format, read, columns that do not come back exact).
READERS = {
    'pandas.read_parquet': (
        'parquet', lambda r: pd.read_parquet(io.BytesIO(r.content)),
        # NumPy integer columns cannot hold NULL, so they become float, which rounds
        # values above 2**53.
        {'big_id', 'small'},
    ),
    'pandas.read_parquet(dtype_backend=pyarrow)': (
        'parquet', lambda r: pd.read_parquet(io.BytesIO(r.content), dtype_backend='pyarrow'),
        set(),
    ),
    'pandas.read_feather(compat=oldest)': (
        'arrow', lambda r: pd.read_feather(io.BytesIO(r.content)), {'big_id', 'small'},
    ),
    'pandas.read_csv': (
        'csv', lambda r: pd.read_csv(io.StringIO(r.text)),
        # Floats for integers with NULL and for decimals, text for dates and times, and
        # NULL for empty strings.
        {'big_id', 'small', 'price', 'name', 'day', 'created_at', 'local_time'},
    ),
    'pandas.read_json': (
        'json', lambda r: pd.read_json(io.StringIO(r.text)),
        {'big_id', 'small', 'price', 'flag', 'day', 'payload'},
    ),
    'pandas.read_json(lines=True)': (
        'ndjson', lambda r: pd.read_json(io.StringIO(r.text), lines=True),
        {'big_id', 'small', 'price', 'flag', 'day', 'payload'},
    ),
    'pandas.read_json(orient=split)': (
        'json-split', lambda r: pd.read_json(io.StringIO(r.text), orient='split'),
        {'big_id', 'small', 'price', 'flag', 'day', 'payload'},
    ),
    'pandas.DataFrame(response.json())': (
        'json', lambda r: pd.DataFrame(r.json()),
        {'big_id', 'small', 'price', 'day', 'created_at', 'local_time', 'payload'},
    ),
    'polars.read_parquet': ('parquet', lambda r: pl.read_parquet(io.BytesIO(r.content)), set()),
    'polars.read_ipc': ('arrow', lambda r: pl.read_ipc(io.BytesIO(r.content)), set()),
    'polars.read_ipc_stream': (
        'arrow-stream', lambda r: pl.read_ipc_stream(io.BytesIO(r.content)), set(),
    ),
    'polars.read_csv': (
        'csv', lambda r: pl.read_csv(io.BytesIO(r.content)),
        # Without a schema: text for dates and times, float for decimals.
        {'price', 'day', 'created_at', 'local_time'},
    ),
    'polars.read_csv(schema)': (
        'csv',
        lambda r: pl.read_csv(
            io.BytesIO(r.content),
            schema={c: datasets.SCHEMA[c] for c in datasets.FLAT_COLUMNS},
        ),
        set(),
    ),
    'polars.read_json': (
        'json', lambda r: pl.read_json(io.BytesIO(r.content)),
        # JSON has no date, decimal or binary type; these stay text.
        {'price', 'day', 'created_at', 'local_time', 'payload'},
    ),
    'polars.read_ndjson': (
        'ndjson', lambda r: pl.read_ndjson(io.BytesIO(r.content)),
        {'price', 'day', 'created_at', 'local_time', 'payload'},
    ),
    'polars.DataFrame(response.json())': (
        'json', lambda r: pl.DataFrame(r.json()),
        {'price', 'day', 'created_at', 'local_time', 'payload'},
    ),
}


def _buffer(write):
    buffer = io.BytesIO()
    write(buffer)
    return buffer.getvalue()


def _pandas_frame() -> pd.DataFrame:
    """An exact pandas copy of the dataset, as read with dtype_backend='pyarrow'."""
    return datasets.orders(ROWS).to_pandas(use_pyarrow_extension_array=True)


# Writer: (content type, columns sent, body, columns that do not arrive exact or None
# when the API rejects the body).
WRITERS = {
    'pandas.to_parquet': (
        'application/vnd.apache.parquet', list(datasets.SCHEMA),
        lambda: _buffer(_pandas_frame().to_parquet), set(),
    ),
    'pandas.to_feather': (
        'application/vnd.apache.arrow.file', list(datasets.SCHEMA),
        lambda: _buffer(_pandas_frame().to_feather), set(),
    ),
    'pandas.to_csv': (
        'text/csv', datasets.FLAT_COLUMNS,
        lambda: _pandas_frame()[datasets.FLAT_COLUMNS].to_csv(index=False).encode(),
        # NULL and the empty string are both written as an empty field.
        {'name'},
    ),
    'pandas.to_json(orient=records)': (
        'application/json', JSON_COLUMNS,
        lambda: _pandas_frame()[JSON_COLUMNS].to_json(
            orient='records', date_format='iso', date_unit='us',
        ).encode(),
        # Dates are written as timestamps at midnight, which a date field rejects.
        None,
    ),
    'polars.write_parquet': (
        'application/vnd.apache.parquet', list(datasets.SCHEMA),
        lambda: _buffer(datasets.orders(ROWS).write_parquet), set(),
    ),
    'polars.write_ipc': (
        'application/vnd.apache.arrow.file', list(datasets.SCHEMA),
        lambda: _buffer(datasets.orders(ROWS).write_ipc), set(),
    ),
    'polars.write_ipc_stream': (
        'application/vnd.apache.arrow.stream', list(datasets.SCHEMA),
        lambda: _buffer(datasets.orders(ROWS).write_ipc_stream), set(),
    ),
    'polars.write_csv': (
        'text/csv', datasets.FLAT_COLUMNS,
        lambda: datasets.orders(ROWS).select(datasets.FLAT_COLUMNS).write_csv().encode(),
        set(),
    ),
    'polars.write_json': (
        'application/json', JSON_COLUMNS,
        lambda: datasets.orders(ROWS).select(JSON_COLUMNS).write_json().encode(), set(),
    ),
    'polars.write_ndjson': (
        'application/x-ndjson', JSON_COLUMNS,
        lambda: datasets.orders(ROWS).select(JSON_COLUMNS).write_ndjson().encode(), set(),
    ),
}


def get(api_url, fmt, **params):
    response = requests.get(
        f'{api_url}/datasets/orders', params=dict(format=fmt, rows=ROWS, **params), timeout=30,
    )
    response.raise_for_status()
    return response


@pytest.mark.parametrize('reader', sorted(READERS))
def test_reader_keeps_the_documented_columns(api_url, reader):
    fmt, read, losses = READERS[reader]
    params = dict(compat='oldest') if 'compat=oldest' in reader else {}
    frame = read(get(api_url, fmt, **params))

    columns = datasets.FLAT_COLUMNS if fmt in formats.TEXT_FORMATS else list(datasets.SCHEMA)
    problems = mismatched_columns(frame, datasets.orders(ROWS).select(columns))

    assert set(problems) == losses, problems


def test_pandas_cannot_read_string_view_lists_from_polars(api_url):
    """
    Polars 2 writes Arrow strings as string_view, which pyarrow cannot convert to
    pandas inside a list. compat=oldest makes the service write large_string.
    """
    import pyarrow

    with pytest.raises(pyarrow.ArrowNotImplementedError, match='string_view'):
        pd.read_feather(io.BytesIO(get(api_url, 'arrow').content))


@pytest.mark.parametrize('writer', sorted(WRITERS))
def test_writer_sends_the_documented_columns(api_url, collection, writer):
    content_type, columns, body, losses = WRITERS[writer]

    response = requests.post(
        f'{api_url}/collections/{collection}',
        data=body(),
        headers={'Content-Type': content_type},
        params=dict(schema='orders', columns=','.join(columns)),
        timeout=30,
    )

    if losses is None:
        assert response.status_code == 422, response.text
        assert response.headers['content-type'] == 'application/problem+json'
        return
    assert response.status_code == 201, response.text
    expected = formats.column_checksums(datasets.orders(ROWS).select(columns))
    differing = {c for c in columns if response.json()['checksums'][c] != expected[c]}
    assert differing == losses


@pytest.mark.parametrize('fmt', ['parquet', 'arrow', 'arrow-stream', 'json', 'ndjson', 'csv'])
def test_what_the_api_stored_reads_back_unchanged(api_url, collection, fmt):
    """Send in one format, read the stored collection back in another."""
    body, media = formats.encode(datasets.orders(ROWS), fmt, datasets.FLAT_COLUMNS)
    columns = datasets.FLAT_COLUMNS if fmt in formats.TEXT_FORMATS else list(datasets.SCHEMA)
    requests.post(
        f'{api_url}/collections/{collection}', data=body, headers={'Content-Type': media},
        params=dict(schema='orders', columns=','.join(columns)), timeout=30,
    ).raise_for_status()

    stored = requests.get(f'{api_url}/collections/{collection}', timeout=30)
    stored.raise_for_status()

    assert mismatched_columns(
        pl.read_parquet(io.BytesIO(stored.content)), datasets.orders(ROWS).select(columns),
    ) == {}


def test_multipart_upload(api_url, collection):
    body = _buffer(datasets.orders(ROWS).write_parquet)

    response = requests.post(
        f'{api_url}/collections/{collection}/upload',
        files={'file': ('orders.parquet', body, 'application/octet-stream')},
        params=dict(schema='orders'),
        timeout=30,
    )

    assert response.status_code == 201, response.text
    assert response.json()['checksums'] == formats.column_checksums(datasets.orders(ROWS))


def test_gzip_request_and_response(api_url, collection):
    import gzip

    body, media = formats.encode(datasets.orders(ROWS), 'ndjson', datasets.FLAT_COLUMNS)
    sent = requests.post(
        f'{api_url}/collections/{collection}', data=gzip.compress(body),
        headers={'Content-Type': media, 'Content-Encoding': 'gzip'}, timeout=30,
    )
    received = requests.get(f'{api_url}/gzip', timeout=30)

    assert sent.status_code == 201, sent.text
    # requests decompresses gzip responses.
    assert len(received.json()) == 50


def test_large_frames_in_binary_formats(api_url, collection):
    frame = datasets.orders(200_000)

    sent = requests.post(
        f'{api_url}/collections/{collection}',
        data=_buffer(frame.write_parquet),
        headers={'Content-Type': formats.MEDIA_TYPES['parquet']},
        timeout=120,
    )
    received = requests.get(
        f'{api_url}/collections/{collection}', params=dict(format='arrow'), timeout=120,
    )

    assert sent.status_code == 201, sent.text
    assert pl.read_ipc(io.BytesIO(received.content)).equals(frame)
