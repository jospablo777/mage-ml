"""A MageML service's API, from an Airflow connection of type MageML service."""
import json
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


# The client id the MageML app's own frontend sends; servers accept it from any caller.
APP_CLIENT_ID = 'zkWlN0PkIKSN0C11CfUHUj84OT5XOJ6tDZ6bDRO2'
TRIGGER_NAME = 'Airflow'
# The run variable that holds a task instance's key, so a retry finds the run it started.
RUN_KEY_VARIABLE = 'mageml_airflow_key'


def parse_log_line(line: str) -> Tuple[float, Optional[str], str]:
    """A server log line (time, then a JSON record) as (timestamp, block, message)."""
    _, _, record = line.partition(' ')
    try:
        data = json.loads(record)
    except ValueError:
        return 0.0, None, line
    if not isinstance(data, dict):
        return 0.0, None, line
    return float(data.get('timestamp') or 0), data.get('block_uuid'), str(data.get('message', ''))


class MageMLServerHook(BaseHook):
    """
    Calls a MageML server (the app, not an exported service). The connection's host is the
    server URL, its login and password a user's email and password; the user needs the
    editor role to start and cancel runs. Runs go under the pipeline's API trigger named
    Airflow, which the hook makes when the pipeline has none.
    """

    conn_name_attr = 'mageml_server_conn_id'
    default_conn_name = 'mageml_server_default'
    conn_type = 'mageml_server'
    hook_name = 'MageML server'

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict[str, Any]:
        return {
            'hidden_fields': ['extra'],
            'relabeling': {
                'host': 'Server URL',
                'login': 'Email',
                'password': 'Password',
                'schema': 'Scheme (http or https, when the URL has none)',
            },
            'placeholders': {'host': 'http://mageml.internal:6789'},
        }

    def __init__(self, mageml_server_conn_id: str = default_conn_name, timeout: float = 30.0):
        super().__init__()
        self.mageml_server_conn_id = mageml_server_conn_id
        self.timeout = timeout
        self._session: Optional[requests.Session] = None
        self._base_url: Optional[str] = None
        self._token: Optional[str] = None

    def _connection(self) -> str:
        if self._base_url is None:
            connection = self.get_connection(self.mageml_server_conn_id)
            host = (connection.host or '').rstrip('/')
            if not host:
                raise MageMLServiceError(
                    f'Connection {self.mageml_server_conn_id} has no host: set it to the '
                    'server URL, such as http://mageml.internal:6789.'
                )
            if '://' not in host:
                host = f'{connection.schema or "http"}://{host}'
                if connection.port:
                    host = f'{host}:{connection.port}'
            self._base_url = host
            self._credentials = (connection.login or '', connection.password or '')
        return self._base_url

    def _call(self, method: str, path: str, signed_in: bool = True, **kwargs) -> Dict[str, Any]:
        base_url = self._connection()
        if self._session is None:
            self._session = requests.Session()
        if signed_in and self._token is None:
            self.sign_in()
        headers = kwargs.pop('headers', {})
        headers['X-API-KEY'] = APP_CLIENT_ID
        cookies = {'oauth_token': self._token} if signed_in and self._token else None
        try:
            response = self._session.request(
                method, f'{base_url}/api/{path}', headers=headers, cookies=cookies,
                timeout=self.timeout, **kwargs,
            )
        except requests.RequestException as error:
            raise MageMLServiceError(f'{method} {base_url}/api/{path}: {error}')
        try:
            body = response.json()
        except ValueError:
            raise MageMLServiceError(
                f'{method} /api/{path} answered {response.status_code}: {response.text[:500]}',
                status=response.status_code,
            )
        error = body.get('error') if isinstance(body, dict) else None
        if error or response.status_code >= 400:
            message = (error or {}).get('message') or (error or {}).get('exception') or \
                response.text[:500]
            raise MageMLServiceError(f'{method} /api/{path}: {message}',
                                     status=(error or {}).get('code') or response.status_code)
        return body

    def sign_in(self) -> None:
        self._connection()
        email, password = self._credentials
        if not email:
            raise MageMLServiceError(
                f'Connection {self.mageml_server_conn_id} has no login: set it to the email '
                'of a MageML user with the editor role.'
            )
        body = self._call('POST', 'sessions', signed_in=False,
                          json={'session': {'email': email, 'password': password}})
        self._token = body['session']['token']

    def trigger(self, pipeline: str) -> Dict[str, Any]:
        """The pipeline's API trigger named Airflow, made active when missing."""
        body = self._call('GET', f'pipelines/{pipeline}/pipeline_schedules')
        for schedule in body.get('pipeline_schedules') or []:
            if schedule.get('name') == TRIGGER_NAME and schedule.get('schedule_type') == 'api':
                if schedule.get('status') != 'active':
                    self._call('PUT', f"pipeline_schedules/{schedule['id']}",
                               json={'pipeline_schedule': {'status': 'active'}})
                return schedule
        return self._call('POST', f'pipelines/{pipeline}/pipeline_schedules', json={
            'pipeline_schedule': {
                'description': 'Runs started by Airflow (mageml-airflow).',
                'name': TRIGGER_NAME,
                'schedule_type': 'api',
                'status': 'active',
            },
        })['pipeline_schedule']

    def find_run(self, schedule_id: int, key: str) -> Optional[Dict[str, Any]]:
        """The trigger's latest run started with this key, when there is one."""
        body = self._call('GET', f'pipeline_schedules/{schedule_id}/pipeline_runs',
                          params={'_limit': 100})
        for run in body.get('pipeline_runs') or []:
            if (run.get('variables') or {}).get(RUN_KEY_VARIABLE) == key:
                return run
        return None

    def start_run(self, schedule_id: int, variables: Dict[str, Any]) -> Dict[str, Any]:
        return self._call('POST', f'pipeline_schedules/{schedule_id}/pipeline_runs',
                          json={'pipeline_run': {'variables': variables}})['pipeline_run']

    def run(self, run_id: int) -> Dict[str, Any]:
        return self._call('GET', f'pipeline_runs/{run_id}')['pipeline_run']

    def block_runs(self, run_id: int) -> List[Dict[str, Any]]:
        return self._call('GET', f'pipeline_runs/{run_id}/block_runs').get('block_runs') or []

    def logs(self, pipeline: str, run_id: int) -> List[Tuple[Optional[str], str]]:
        """The run's block log lines as (block, message), in time order."""
        body = self._call('GET', f'pipelines/{pipeline}/logs',
                          params={'pipeline_run_id[]': str(run_id)})
        lines = []
        for entry in body.get('logs') or []:
            for log in entry.get('block_run_logs') or []:
                for line in (log.get('content') or '').splitlines():
                    lines.append(parse_log_line(line))
        lines.sort(key=lambda line: line[0])
        return [(block, message) for _, block, message in lines]

    def cancel(self, run_id: int) -> None:
        self._call('PUT', f'pipeline_runs/{run_id}',
                   json={'pipeline_run': {'status': 'cancelled'}})

    def test_connection(self) -> Tuple[bool, str]:
        try:
            self.sign_in()
            self._call('GET', 'statuses')
        except MageMLServiceError as error:
            return False, str(error)
        return True, 'Signed in to the server.'
