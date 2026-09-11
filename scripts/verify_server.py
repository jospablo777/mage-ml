import re
import sys
import time
from urllib.error import URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import urlopen


def fetch(url):
    with urlopen(url, timeout=5) as response:
        if response.status != 200:
            raise RuntimeError(f'{url}: HTTP {response.status}')
        return response.read()


def verify_server(base_url):
    deadline = time.monotonic() + 120
    while True:
        try:
            html = fetch(f'{base_url}/sign-in').decode()
            break
        except (URLError, TimeoutError, ConnectionError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)

    asset_prefix = f'{urlsplit(base_url).path.rstrip("/")}/_next/'
    assets = [
        asset for asset in re.findall(r'<script[^>]+src="([^"]+)"', html)
        if asset.startswith(asset_prefix)
    ]
    if not assets:
        raise RuntimeError('Login page does not reference frontend scripts')
    for asset in assets:
        fetch(urljoin(base_url, asset))
    for _ in range(3):
        time.sleep(1)
        fetch(f'{base_url}/sign-in')
    print('Server startup, frontend scripts, and request handling passed')


if __name__ == '__main__':
    verify_server(sys.argv[1].rstrip('/'))
