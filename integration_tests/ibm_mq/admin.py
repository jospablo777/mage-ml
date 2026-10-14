"""
IBM MQ for tests through the queue manager's REST API, so tests need no MQ client in Python:
queues are defined and inspected with MQSC, messages put and got with the messaging API.
"""
import ssl
import tempfile
import time
import uuid
from typing import Dict, Optional, Tuple

import requests

CSRF = {'ibm-mq-rest-csrf-token': 'mage'}


class IbmMqAdmin:
    def __init__(self, port: int, web_port: int, user: str, password: str,
                 admin_password: str, library: str, queue_manager: str = 'QM1'):
        self.port = port
        self.user = user
        self.password = password
        self.library = library
        self.queue_manager = queue_manager
        self.web_port = web_port
        # The developer image's web server has a self-signed certificate for localhost: the
        # client pins it (wait_ready) and connects by that name, so TLS is verified.
        rest = f'https://localhost:{web_port}/ibmmq/rest'
        self.mqsc_url = f'{rest}/v2/admin/action/qmgr/{queue_manager}/mqsc'
        self.messages_url = f'{rest}/v3/messaging/qmgr/{queue_manager}/queue/{{}}/message'
        self.admin = requests.Session()
        self.admin.auth = ('admin', admin_password)
        self.app = requests.Session()
        self.app.auth = (user, password)

    def wait_ready(self, timeout: float = 180) -> 'IbmMqAdmin':
        """The web server starts after the queue manager reports healthy."""
        deadline = time.time() + timeout
        while True:
            try:
                self._pin_certificate()
                self.mqsc('display', 'qmgr', '')
                return self
            except (OSError, requests.RequestException):
                if time.time() > deadline:
                    raise
                time.sleep(2)

    def _pin_certificate(self) -> None:
        if self.admin.verify is not True:
            return
        pem = ssl.get_server_certificate(('127.0.0.1', self.web_port), timeout=10)
        with tempfile.NamedTemporaryFile('w', suffix='.pem', delete=False) as file:
            file.write(pem)
        self.admin.verify = self.app.verify = file.name

    def mqsc(self, command: str, qualifier: str, name: str, **parameters) -> Dict:
        body = dict(type='runCommandJSON', command=command, qualifier=qualifier)
        if name:
            body['name'] = name
        if parameters:
            body['parameters'] = parameters
        response = self.admin.post(self.mqsc_url, json=body, headers=CSRF, timeout=30)
        response.raise_for_status()
        return response.json()

    def define(self, depth: int = 100_000) -> str:
        """A new local queue; the developer setup authorizes the app user on DEV.**."""
        name = f'DEV.IT.{uuid.uuid4().hex[:12].upper()}'
        result = self.mqsc('define', 'qlocal', name, defpsist='yes', maxdepth=depth)
        assert result['overallCompletionCode'] == 0, result
        return name

    def delete(self, name: str) -> None:
        self.mqsc('delete', 'qlocal', name, purge='yes')

    def put_inhibit(self, name: str) -> None:
        self.mqsc('alter', 'qlocal', name, put='disabled')

    def depth(self, name: str) -> Tuple[int, str]:
        """Messages on the queue and whether some are uncommitted."""
        result = self.mqsc('display', 'qstatus', name)
        parameters = result['commandResponse'][0]['parameters']
        return int(parameters['curdepth']), parameters['uncom']

    def put(self, queue: str, body: str) -> None:
        response = self.app.post(
            self.messages_url.format(queue), data=body.encode(), timeout=30,
            headers={**CSRF, 'Content-Type': 'text/plain;charset=utf-8',
                     'ibm-mq-md-persistence': 'persistent'},
        )
        assert response.status_code == 201, response.text

    def get(self, queue: str, wait_ms: int = 0) -> Optional[Tuple[str, Dict[str, str]]]:
        """Destructively gets the next message: its body and MQMD headers."""
        response = self.app.delete(
            self.messages_url.format(queue), params=dict(wait=wait_ms) if wait_ms else None,
            headers=CSRF, timeout=60,
        )
        if response.status_code == 204:
            return None
        assert response.status_code == 200, response.text
        return response.content.decode(), {
            k.lower(): v for k, v in response.headers.items() if k.lower().startswith('ibm-mq-md')
        }
