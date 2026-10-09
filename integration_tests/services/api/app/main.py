"""
A REST API for Mage's integration tests.

It serves datasets in every supported exchange format, stores what clients send so the
tests can compare it with what they meant to send, exchanges images, answers a small
prediction endpoint, and produces the failures clients must handle: client and server
errors with problem details, validation errors, rate limits, flaky and slow responses,
malformed bodies, redirects and authentication challenges.
"""
import asyncio
import base64
import gzip
import hashlib
import io
import json
import threading
from collections import defaultdict
from typing import Annotated, Dict, List, Optional

import polars as pl
from app import datasets, formats
from fastapi import FastAPI, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import (
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from PIL import Image, ImageOps
from pydantic import BaseModel, Field

app = FastAPI(title='Mage integration test API')

PROBLEM = 'application/problem+json'
TOKEN = 'test-token'
API_KEY = 'test-api-key'
BASIC = base64.b64encode(b'mage:secret').decode()

_lock = threading.Lock()
_collections: Dict[str, pl.DataFrame] = {}
_idempotent: Dict[str, Dict] = {}
_calls: Dict[str, int] = defaultdict(int)


def problem(status: int, title: str, detail: str = '', **extra) -> JSONResponse:
    """An RFC 9457 problem details response."""
    return JSONResponse(
        dict(type='about:blank', title=title, status=status, detail=detail, **extra),
        status_code=status,
        media_type=PROBLEM,
    )


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else json.dumps(exc.detail)
    return JSONResponse(
        dict(type='about:blank', title=_title(exc.status_code), status=exc.status_code,
             detail=detail),
        status_code=exc.status_code,
        media_type=PROBLEM,
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, exc: RequestValidationError):
    errors = [
        dict(location=list(e['loc']), message=e['msg'], type=e['type']) for e in exc.errors()
    ]
    return JSONResponse(
        dict(type='about:blank', title='Unprocessable Content', status=422,
             detail='The request does not match the expected shape.', errors=errors),
        status_code=422,
        media_type=PROBLEM,
    )


def _title(status: int) -> str:
    from http import HTTPStatus

    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return 'Error'


def _schema(name: Optional[str], columns: Optional[List[str]] = None):
    if name is None:
        return None
    if name not in datasets.DATASETS:
        raise HTTPException(404, f'Unknown schema {name!r}')
    schema = datasets.SCHEMA
    if columns:
        schema = {c: schema[c] for c in columns}
    return schema


def _format(fmt: str) -> str:
    if fmt not in formats.ENCODERS:
        raise HTTPException(
            400, f'Unknown format {fmt!r}; use one of {sorted(formats.ENCODERS)}',
        )
    return fmt


def _frame_response(
    frame: pl.DataFrame,
    fmt: str,
    headers: Optional[Dict] = None,
    compat: str = 'newest',
):
    if compat not in ('newest', 'oldest'):
        raise HTTPException(400, "compat must be 'newest' or 'oldest'")
    body, media = formats.encode(frame, fmt, datasets.FLAT_COLUMNS, compat)
    return Response(body, media_type=media, headers=headers)


async def _body(request: Request) -> bytes:
    body = await request.body()
    if request.headers.get('content-encoding', '').lower() == 'gzip':
        try:
            body = gzip.decompress(body)
        except OSError as err:
            raise HTTPException(400, f'Body is not valid gzip: {err}')
    return body


@app.get('/health')
def health():
    return dict(status='ok')


@app.get('/echo')
@app.post('/echo')
@app.put('/echo')
@app.patch('/echo')
@app.delete('/echo')
async def echo(request: Request):
    """What the server received, so clients can check their requests."""
    body = await request.body()
    return dict(
        method=request.method,
        path=request.url.path,
        query=dict(request.query_params),
        headers={k.lower(): v for k, v in request.headers.items()},
        body_size=len(body),
        body_sha256=hashlib.sha256(body).hexdigest(),
    )


# Datasets -------------------------------------------------------------------------------

@app.get('/datasets/{name}')
def get_dataset(
    name: str,
    format: str = 'json',
    rows: int = Query(50, ge=1, le=1_000_000),
    columns: Optional[str] = None,
    compat: str = 'newest',
):
    _format(format)
    try:
        frame = datasets.dataset(name, rows)
    except KeyError:
        raise HTTPException(404, f'Unknown dataset {name!r}')
    if columns:
        selected = columns.split(',')
        unknown = [c for c in selected if c not in frame.columns]
        if unknown:
            raise HTTPException(400, f'Unknown columns {unknown}')
        frame = frame.select(selected)
    return _frame_response(
        frame, format, headers={'X-Total-Count': str(frame.height)}, compat=compat,
    )


@app.get('/datasets/{name}/schema')
def get_schema(name: str):
    if name not in datasets.DATASETS:
        raise HTTPException(404, f'Unknown dataset {name!r}')
    return dict(columns={c: str(t) for c, t in datasets.SCHEMA.items()},
                flat_columns=datasets.FLAT_COLUMNS)


@app.get('/datasets/{name}/checksums')
def get_checksums(name: str, rows: int = 50):
    try:
        frame = datasets.dataset(name, rows)
    except KeyError:
        raise HTTPException(404, f'Unknown dataset {name!r}')
    return formats.column_checksums(frame)


@app.get('/datasets/{name}/pages')
def get_page(name: str, rows: int = 50, page_size: int = 10, cursor: Optional[str] = None):
    """Cursor pagination: the cursor is opaque to clients."""
    try:
        frame = datasets.dataset(name, rows)
    except KeyError:
        raise HTTPException(404, f'Unknown dataset {name!r}')
    if page_size < 1 or page_size > 1000:
        raise HTTPException(422, 'page_size must be between 1 and 1000')
    try:
        offset = int(base64.urlsafe_b64decode(cursor).decode()) if cursor else 0
    except Exception:
        raise HTTPException(400, 'Invalid cursor')
    page = frame.slice(offset, page_size)
    following = offset + page_size
    next_cursor = (
        base64.urlsafe_b64encode(str(following).encode()).decode()
        if following < frame.height else None
    )
    return dict(data=[{k: formats.to_json_value(v) for k, v in r.items()} for r in page.to_dicts()],
                next_cursor=next_cursor)


@app.get('/datasets/{name}/offset')
def get_offset_page(request: Request, name: str, rows: int = 50, offset: int = 0,
                    limit: int = 10):
    """Offset pagination with a Link header to the next page."""
    try:
        frame = datasets.dataset(name, rows)
    except KeyError:
        raise HTTPException(404, f'Unknown dataset {name!r}')
    page = frame.slice(offset, limit)
    headers = {'X-Total-Count': str(frame.height)}
    if offset + limit < frame.height:
        url = request.url.include_query_params(offset=offset + limit, limit=limit)
        headers['Link'] = f'<{url}>; rel="next"'
    body, media = formats.encode(page, 'json', datasets.FLAT_COLUMNS)
    return Response(body, media_type=media, headers=headers)


@app.get('/datasets/{name}/stream')
def stream_dataset(name: str, rows: int = 1000):
    """Newline-delimited JSON sent in chunks, for clients that read a stream."""
    try:
        frame = datasets.dataset(name, rows)
    except KeyError:
        raise HTTPException(404, f'Unknown dataset {name!r}')

    def chunks():
        for start in range(0, frame.height, 100):
            yield formats.ENCODERS['ndjson'](frame.slice(start, 100))

    return StreamingResponse(chunks(), media_type='application/x-ndjson')


# Collections: what clients send -----------------------------------------------------------

@app.post('/collections/{name}', status_code=201)
async def post_collection(
    request: Request,
    name: str,
    schema: Optional[str] = None,
    columns: Optional[str] = None,
    mode: str = 'replace',
    idempotency_key: Optional[str] = Header(None),
):
    """
    Store a data frame sent in any supported format. With schema=orders the values must
    match the dataset's types, and the response is 422 with each mismatch otherwise.
    """
    if mode not in ('replace', 'append'):
        raise HTTPException(400, "mode must be 'replace' or 'append'")
    with _lock:
        if idempotency_key and idempotency_key in _idempotent:
            return JSONResponse(_idempotent[idempotency_key], status_code=200,
                                headers={'Idempotent-Replayed': 'true'})
    body = await _body(request)
    if not body:
        raise HTTPException(400, 'Empty body')
    content_type = request.headers.get('content-type', '')
    try:
        frame = formats.decode(
            body,
            content_type,
            _schema(schema, columns.split(',') if columns else None),
        )
    except formats.DecodeError as err:
        status = 415 if 'Unsupported content type' in str(err) else 422
        return problem(status, _title(status), str(err), errors=err.errors)
    return _store(name, frame, mode, idempotency_key)


@app.post('/collections/{name}/upload', status_code=201)
async def upload_collection(
    name: str,
    file: Annotated[UploadFile, File()],
    schema: Optional[str] = None,
):
    """multipart/form-data upload; the file extension selects the format."""
    extension = (file.filename or '').rsplit('.', 1)[-1].lower()
    media = {
        'json': 'application/json', 'ndjson': 'application/x-ndjson', 'jsonl':
        'application/x-ndjson', 'csv': 'text/csv', 'parquet': 'application/vnd.apache.parquet',
        'arrow': 'application/vnd.apache.arrow.file', 'feather':
        'application/vnd.apache.arrow.file', 'arrows': 'application/vnd.apache.arrow.stream',
    }.get(extension)
    if media is None:
        return problem(415, _title(415), f'Unsupported file extension {extension!r}')
    try:
        frame = formats.decode(await file.read(), media, _schema(schema))
    except formats.DecodeError as err:
        return problem(422, _title(422), str(err), errors=err.errors)
    return _store(name, frame, 'replace', None)


def _store(name: str, frame: pl.DataFrame, mode: str, idempotency_key: Optional[str]):
    with _lock:
        if mode == 'append' and name in _collections:
            try:
                frame = pl.concat([_collections[name], frame], how='vertical_relaxed')
            except Exception as err:
                return problem(409, _title(409), f'Cannot append: {err}')
        _collections[name] = frame
        result = dict(
            collection=name,
            rows=frame.height,
            columns=frame.columns,
            schema={c: str(t) for c, t in frame.schema.items()},
            checksums=formats.column_checksums(frame),
        )
        if idempotency_key:
            _idempotent[idempotency_key] = result
    return JSONResponse(result, status_code=201)


@app.get('/collections/{name}')
def get_collection(name: str, format: str = 'parquet', compat: str = 'newest'):
    _format(format)
    if name not in _collections:
        raise HTTPException(404, f'Unknown collection {name!r}')
    return _frame_response(_collections[name], format, compat=compat)


@app.delete('/collections/{name}', status_code=204)
def delete_collection(name: str):
    with _lock:
        if _collections.pop(name, None) is None:
            raise HTTPException(404, f'Unknown collection {name!r}')
    return Response(status_code=204)


# Predictions ----------------------------------------------------------------------------

class Instance(BaseModel):
    x1: float
    x2: float
    segment: str = Field(min_length=1)


class PredictRequest(BaseModel):
    instances: List[Instance] = Field(min_length=1, max_length=10_000)


@app.post('/predict')
def predict(request: PredictRequest):
    """A linear model: 2 * x1 + 3 * x2 + 1, plus 10 for segment 'b'."""
    return dict(predictions=[
        2 * i.x1 + 3 * i.x2 + 1 + (10 if i.segment == 'b' else 0) for i in request.instances
    ], model_version='1')


# Images ---------------------------------------------------------------------------------

IMAGE_FORMATS = {'png': 'image/png', 'jpeg': 'image/jpeg', 'webp': 'image/webp',
                 'tiff': 'image/tiff', 'bmp': 'image/bmp'}


def gradient(width: int, height: int) -> Image.Image:
    """A deterministic RGB image: red follows x, green follows y, blue is constant."""
    image = Image.new('RGB', (width, height))
    image.putdata([
        (x * 255 // max(width - 1, 1), y * 255 // max(height - 1, 1), 128)
        for y in range(height) for x in range(width)
    ])
    return image


def _image_bytes(image: Image.Image, fmt: str) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format='JPEG' if fmt == 'jpeg' else fmt.upper())
    return buffer.getvalue()


def _image_info(data: bytes) -> Dict:
    try:
        image = Image.open(io.BytesIO(data))
        image.load()
    except Exception as err:
        raise HTTPException(422, f'Not a readable image: {err}')
    pixels = image.convert('RGB').tobytes()
    return dict(width=image.width, height=image.height, mode=image.mode,
                format=(image.format or '').lower(), size=len(data),
                sha256=hashlib.sha256(data).hexdigest(),
                pixels_sha256=hashlib.sha256(pixels).hexdigest())


@app.get('/images/gradient')
def get_image(format: str = 'png', width: int = 32, height: int = 16):
    if format not in IMAGE_FORMATS:
        raise HTTPException(400, f'Unknown image format {format!r}')
    if not (1 <= width <= 4096 and 1 <= height <= 4096):
        raise HTTPException(422, 'width and height must be between 1 and 4096')
    return Response(_image_bytes(gradient(width, height), format),
                     media_type=IMAGE_FORMATS[format])


@app.get('/images/gradient/base64')
def get_image_base64(format: str = 'png', width: int = 32, height: int = 16):
    response = get_image(format, width, height)
    return dict(media_type=response.media_type, width=width, height=height,
                data=base64.b64encode(response.body).decode())


@app.post('/images')
async def post_image(request: Request):
    """An image as the raw body (image/*) or as JSON {"data": base64}."""
    content_type = request.headers.get('content-type', '').split(';')[0]
    body = await _body(request)
    if content_type.startswith('image/'):
        return _image_info(body)
    if content_type == 'application/json':
        try:
            payload = json.loads(body)
            data = base64.b64decode(payload['data'], validate=True)
        except (ValueError, KeyError, TypeError) as err:
            raise HTTPException(422, f'Expected {{"data": base64}}: {err}')
        return _image_info(data)
    return problem(415, _title(415), f'Unsupported content type {content_type!r}')


@app.post('/images/upload')
async def upload_image(file: Annotated[UploadFile, File()]):
    return dict(filename=file.filename, **_image_info(await file.read()))


@app.post('/images/invert')
async def invert_image(request: Request, format: str = 'png'):
    """Returns the inverted image, for clients that send an image and read one back."""
    body = await _body(request)
    _image_info(body)
    image = ImageOps.invert(Image.open(io.BytesIO(body)).convert('RGB'))
    return Response(_image_bytes(image, format), media_type=IMAGE_FORMATS[format])


@app.post('/images/pixels')
async def pixels_to_image(request: Request, width: int, height: int, format: str = 'png'):
    """
    Pixels sent as a data frame with columns x, y, r, g, b in any supported format,
    returned as an image.
    """
    try:
        frame = formats.decode(await _body(request), request.headers.get('content-type', ''))
    except formats.DecodeError as err:
        return problem(422, _title(422), str(err), errors=err.errors)
    expected = {'x', 'y', 'r', 'g', 'b'}
    if set(frame.columns) != expected or frame.height != width * height:
        return problem(422, _title(422),
                       f'Expected {width * height} rows with columns {sorted(expected)}')
    image = Image.new('RGB', (width, height))
    for row in frame.iter_rows(named=True):
        image.putpixel((row['x'], row['y']), (row['r'], row['g'], row['b']))
    return Response(_image_bytes(image, format), media_type=IMAGE_FORMATS[format])


# Failures -------------------------------------------------------------------------------

@app.api_route('/status/{code}', methods=['GET', 'POST', 'PUT', 'PATCH', 'DELETE'])
def status(code: int, retry_after: Optional[int] = None):
    if not 200 <= code <= 599:
        raise HTTPException(400, 'code must be between 200 and 599')
    if code == 204:
        return Response(status_code=204)
    if code < 400:
        return JSONResponse(dict(status=code), status_code=code)
    headers = {}
    if code in (429, 503):
        headers['Retry-After'] = str(retry_after if retry_after is not None else 1)
    return JSONResponse(
        dict(type='about:blank', title=_title(code), status=code,
             detail=f'Requested status {code}'),
        status_code=code, media_type=PROBLEM, headers=headers,
    )


@app.api_route('/flaky/{key}', methods=['GET', 'POST'])
async def flaky(request: Request, key: str, failures: int = 2, code: int = 503):
    """Fails the first `failures` calls for a key, then returns the dataset."""
    with _lock:
        _calls[key] += 1
        call = _calls[key]
    if call <= failures:
        headers = {'Retry-After': '0'} if code in (429, 503) else {}
        return JSONResponse(
            dict(type='about:blank', title=_title(code), status=code,
                 detail=f'Failure {call} of {failures}'),
            status_code=code, media_type=PROBLEM, headers=headers,
        )
    return dict(calls=call, data=[{'id': 1}, {'id': 2}])


@app.get('/flaky/{key}/calls')
def flaky_calls(key: str):
    return dict(calls=_calls.get(key, 0))


@app.delete('/flaky/{key}', status_code=204)
def reset_flaky(key: str):
    with _lock:
        _calls.pop(key, None)
    return Response(status_code=204)


@app.get('/slow')
async def slow(seconds: float = 2.0):
    await asyncio.sleep(min(seconds, 30))
    return dict(slept=seconds)


@app.get('/malformed')
def malformed(kind: str = 'json'):
    """A 200 response whose body cannot be parsed as declared."""
    bodies = {
        'json': (b'{"data": [1, 2,', 'application/json'),
        'csv': (b'id,name\n1,"unterminated\n', 'text/csv'),
        'parquet': (b'PAR1 not really parquet', 'application/vnd.apache.parquet'),
        'html': (b'<html><body>Gateway error</body></html>', 'application/json'),
    }
    if kind not in bodies:
        raise HTTPException(400, f'kind must be one of {sorted(bodies)}')
    body, media = bodies[kind]
    return Response(body, media_type=media)


@app.get('/redirect')
def redirect(to: str = '/health', code: int = 307):
    return RedirectResponse(to, status_code=code)


@app.get('/gzip')
def gzipped():
    body, _ = formats.encode(datasets.orders(50), 'json', datasets.FLAT_COLUMNS)
    return Response(gzip.compress(body), media_type='application/json',
                    headers={'Content-Encoding': 'gzip'})


# Authentication -------------------------------------------------------------------------

@app.get('/secure/bearer')
def secure_bearer(authorization: Optional[str] = Header(None)):
    if authorization != f'Bearer {TOKEN}':
        raise HTTPException(401, 'Missing or invalid bearer token',
                            headers={'WWW-Authenticate': 'Bearer'})
    return dict(authenticated=True, scheme='bearer')


@app.get('/secure/api-key')
def secure_api_key(x_api_key: Optional[str] = Header(None)):
    if x_api_key is None:
        raise HTTPException(401, 'Missing X-API-Key header')
    if x_api_key != API_KEY:
        raise HTTPException(403, 'Invalid API key')
    return dict(authenticated=True, scheme='api-key')


@app.get('/secure/basic')
def secure_basic(authorization: Optional[str] = Header(None)):
    if authorization != f'Basic {BASIC}':
        raise HTTPException(401, 'Missing or invalid credentials',
                            headers={'WWW-Authenticate': 'Basic realm="mage"'})
    return dict(authenticated=True, scheme='basic')
