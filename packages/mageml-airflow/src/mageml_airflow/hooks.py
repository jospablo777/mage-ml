"""A MageML service's API, from an Airflow connection of type MageML service."""
from typing import Any, Dict, List, Optional, Tuple

import requests

try:  # Airflow 3
    from airflow.sdk import BaseHook
except ImportError:  # Airflow 2
    from airflow.hooks.base import BaseHook


class MageMLServiceError(Exception):
    """The service refused or failed a request; the message is the service's."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class MageMLServiceHook(BaseHook):
    """
    Calls a MageML service. The connection's host is the service URL (http://host:8080, or
    the host with the port and schema fields), its password the service token
    (MAGE_SERVICE_TOKEN), so no token is written in a DAG.
    """

    conn_name_attr = 'mageml_conn_id'
    default_conn_name = 'mageml_default'
    conn_type = 'mageml_service'
    hook_name = 'MageML service'

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        return {
            'hidden_fields': ['login', 'extra'],
            'relabeling': {
                'host': 'Service URL',
                'password': 'Service token',
                'schema': 'Scheme (http or https, when the URL has none)',
            },
            'placeholders': {'host': 'http://shop-live.internal:8080'},
        }

    def __init__(self, mageml_conn_id: str = default_conn_name, timeout: float = 30.0):
        super().__init__()
        self.mageml_conn_id = mageml_conn_id
        self.timeout = timeout
        self._session: Optional[requests.Session] = None
        self._base_url: Optional[str] = None

    def _connection(self) -> Tuple[str, Optional[str]]:
        if self._base_url is None:
            connection = self.get_connection(self.mageml_conn_id)
            host = (connection.host or '').rstrip('/')
            if not host:
                raise MageMLServiceError(
                    f'Connection {self.mageml_conn_id} has no host: set it to the service '
                    'URL, such as http://shop-live.internal:8080.'
                )
            if '://' not in host:
                host = f'{connection.schema or "http"}://{host}'
                if connection.port:
                    host = f'{host}:{connection.port}'
            self._base_url = host
            self._token = connection.password or None
        return self._base_url, self._token

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        base_url, token = self._connection()
        if self._session is None:
            self._session = requests.Session()
        headers = kwargs.pop('headers', {})
        if token:
            headers['Authorization'] = f'Bearer {token}'
        try:
            response = self._session.request(
                method, f'{base_url}{path}', headers=headers, timeout=self.timeout, **kwargs,
            )
        except requests.RequestException as error:
            raise MageMLServiceError(f'{method} {base_url}{path}: {error}')
        if response.status_code >= 400:
            try:
                message = response.json().get('error') or response.text
            except ValueError:
                message = response.text
            raise MageMLServiceError(
                f'{method} {path} answered {response.status_code}: {message}',
                status=response.status_code,
            )
        return response

    def start_run(
        self,
        pipeline: str,
        variables: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Starts a run; a key that started one before returns that run."""
        body: Dict[str, Any] = {'variables': variables or {}}
        if idempotency_key:
            body['idempotency_key'] = idempotency_key
        return self._request('POST', f'/v1/pipelines/{pipeline}/runs', json=body).json()

    def run(self, run_id: str) -> Dict[str, Any]:
        return self._request('GET', f'/v1/runs/{run_id}').json()

    def logs(self, run_id: str, after: int = 0) -> Tuple[List[Dict[str, Any]], int]:
        """The run's log lines after the first `after`, and the count to ask after next."""
        body = self._request('GET', f'/v1/runs/{run_id}/logs', params={'after': after}).json()
        lines = body.get('lines') or []
        return lines, int(body.get('next', after + len(lines)))

    def output(self, run_id: str, block: str) -> Tuple[Any, int]:
        """A block's output as JSON (a frame as rows), and its size in bytes."""
        response = self._request(
            'GET', f'/v1/runs/{run_id}/output', params={'block': block, 'format': 'json'},
        )
        return response.json(), len(response.content)

    def cancel(self, run_id: str) -> Dict[str, Any]:
        return self._request('POST', f'/v1/runs/{run_id}/cancel').json()

    def test_connection(self) -> Tuple[bool, str]:
        try:
            self._request('GET', '/v1/pipelines')
        except MageMLServiceError as error:
            return False, str(error)
        return True, 'The service answered.'
