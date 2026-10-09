"""
Images exchanged with a REST API: downloaded, read with Pillow and NumPy, turned into
Polars and pandas frames of pixels, sent as JSON or Arrow, and read back.
"""
import base64
import hashlib
import io

import numpy as np
import polars as pl
import pytest
import requests
from PIL import Image

from integration_tests.services.api.app import formats

WIDTH, HEIGHT = 32, 16
LOSSLESS = ['png', 'tiff', 'bmp']


def expected_pixels() -> np.ndarray:
    x = np.arange(WIDTH) * 255 // (WIDTH - 1)
    y = np.arange(HEIGHT) * 255 // (HEIGHT - 1)
    pixels = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    pixels[:, :, 0] = x[None, :]
    pixels[:, :, 1] = y[:, None]
    pixels[:, :, 2] = 128
    return pixels


def download(api_url, fmt='png') -> bytes:
    response = requests.get(
        f'{api_url}/images/gradient', params=dict(format=fmt, width=WIDTH, height=HEIGHT),
        timeout=10,
    )
    response.raise_for_status()
    assert response.headers['content-type'] == f'image/{fmt}'
    return response.content


def to_frame(pixels: np.ndarray) -> pl.DataFrame:
    height, width, _ = pixels.shape
    ys, xs = np.mgrid[0:height, 0:width]
    return pl.DataFrame({
        'x': xs.ravel().astype(np.int64),
        'y': ys.ravel().astype(np.int64),
        'r': pixels[:, :, 0].ravel().astype(np.int64),
        'g': pixels[:, :, 1].ravel().astype(np.int64),
        'b': pixels[:, :, 2].ravel().astype(np.int64),
    })


@pytest.mark.parametrize('fmt', LOSSLESS)
def test_lossless_downloads_keep_every_pixel(api_url, fmt):
    pixels = np.asarray(Image.open(io.BytesIO(download(api_url, fmt))).convert('RGB'))

    assert pixels.shape == (HEIGHT, WIDTH, 3)
    np.testing.assert_array_equal(pixels, expected_pixels())


@pytest.mark.parametrize('fmt', ['jpeg', 'webp'])
def test_lossy_downloads_stay_close(api_url, fmt):
    pixels = np.asarray(Image.open(io.BytesIO(download(api_url, fmt))).convert('RGB'))

    difference = np.abs(pixels.astype(int) - expected_pixels().astype(int))
    assert pixels.shape == (HEIGHT, WIDTH, 3)
    assert difference.mean() < 4


def test_base64_json_in_both_directions(api_url):
    payload = requests.get(
        f'{api_url}/images/gradient/base64', params=dict(width=WIDTH, height=HEIGHT), timeout=10,
    ).json()
    data = base64.b64decode(payload['data'])

    info = requests.post(
        f'{api_url}/images', json=dict(data=base64.b64encode(data).decode()), timeout=10,
    ).json()

    assert payload['media_type'] == 'image/png'
    assert info['sha256'] == hashlib.sha256(data).hexdigest()
    assert (info['width'], info['height'], info['format']) == (WIDTH, HEIGHT, 'png')


def test_raw_body_and_multipart_uploads(api_url):
    data = download(api_url)

    raw = requests.post(
        f'{api_url}/images', data=data, headers={'Content-Type': 'image/png'}, timeout=10,
    ).json()
    upload = requests.post(
        f'{api_url}/images/upload', files={'file': ('gradient.png', data, 'image/png')},
        timeout=10,
    ).json()

    assert raw['pixels_sha256'] == upload['pixels_sha256']
    assert upload['filename'] == 'gradient.png'


def test_image_sent_and_processed_image_read_back(api_url):
    response = requests.post(
        f'{api_url}/images/invert', data=download(api_url),
        headers={'Content-Type': 'image/png'}, timeout=10,
    )

    inverted = np.asarray(Image.open(io.BytesIO(response.content)).convert('RGB'))
    np.testing.assert_array_equal(inverted, 255 - expected_pixels())


@pytest.mark.parametrize('fmt', ['json', 'parquet', 'arrow'])
def test_pixels_as_a_data_frame(api_url, fmt):
    """Image to NumPy to a Polars frame of pixels, sent as data, returned as an image."""
    pixels = np.asarray(Image.open(io.BytesIO(download(api_url))).convert('RGB'))
    body, media = formats.encode(to_frame(pixels), fmt, [])

    response = requests.post(
        f'{api_url}/images/pixels', data=body, headers={'Content-Type': media},
        params=dict(width=WIDTH, height=HEIGHT), timeout=30,
    )

    assert response.status_code == 200, response.text
    rebuilt = np.asarray(Image.open(io.BytesIO(response.content)).convert('RGB'))
    np.testing.assert_array_equal(rebuilt, expected_pixels())


def test_pixels_from_pandas(api_url):
    pixels = expected_pixels()
    frame = to_frame(pixels).to_pandas()

    response = requests.post(
        f'{api_url}/images/pixels', data=frame.to_json(orient='records'),
        headers={'Content-Type': 'application/json'},
        params=dict(width=WIDTH, height=HEIGHT), timeout=30,
    )

    rebuilt = np.asarray(Image.open(io.BytesIO(response.content)).convert('RGB'))
    np.testing.assert_array_equal(rebuilt, pixels)


def test_wrong_pixel_count_is_rejected(api_url):
    body, media = formats.encode(to_frame(expected_pixels()).head(10), 'json', [])

    response = requests.post(
        f'{api_url}/images/pixels', data=body, headers={'Content-Type': media},
        params=dict(width=WIDTH, height=HEIGHT), timeout=10,
    )

    assert response.status_code == 422


@pytest.mark.parametrize('body, content_type', [
    (b'not an image', 'image/png'),
    (b'{"data": "%%%"}', 'application/json'),
    (b'GIF89a', 'image/gif'),
])
def test_unreadable_images_are_rejected(api_url, body, content_type):
    response = requests.post(
        f'{api_url}/images', data=body, headers={'Content-Type': content_type}, timeout=10,
    )

    assert response.status_code == 422


def test_invalid_sizes(api_url):
    response = requests.get(
        f'{api_url}/images/gradient', params=dict(width=0, height=10), timeout=10,
    )

    assert response.status_code == 422
