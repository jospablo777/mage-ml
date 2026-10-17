import json
from typing import Dict

import requests

from mage_ai.shared.hash import ignore_keys_with_blank_values


class HttpClient:
    BASE_URL = None

    def __init__(self):
        self.headers = {
            'Content-Type': 'application/json',
        }

    def make_request(
        self,
        url_path: str,
        method: str = 'GET',
        params: Dict = dict(),
        payload: Dict = dict()
    ):
        url = self.BASE_URL + url_path
        response = None
        if method == 'GET':
            response = requests.get(
                url,
                headers=self.headers,
                params=ignore_keys_with_blank_values(params),
            )
        elif method == 'POST':
            response = requests.post(
                url,
                data=json.dumps(ignore_keys_with_blank_values(payload)),
                headers=self.headers,
            )
        if response is not None:
            response.raise_for_status()
            if response.text:
                return response.json()
        return response


def client_session(timeout_seconds: float = 30):
    """
    An aiohttp session that uses HTTP_PROXY, HTTPS_PROXY and NO_PROXY (sign-in failed behind
    proxies, upstream #5748) and gives up after timeout_seconds instead of aiohttp's 5
    minutes, which held a sign-in that long.
    """
    import aiohttp

    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout_seconds),
        trust_env=True,
    )
