"""
mage service runs and logs: the client of a running service's API, which finds the local
run or the deployment, and says why it cannot reach it.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from mage_ai.pipeline_services import client, deploy
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.pipeline_services.test_deploy import FakeDocker

RUNS = [
    dict(id='r2', pipeline='orders', status='failed', source='schedule:hourly',
         created_at='2026-10-17T10:00:00Z', duration_ms=1530,
         error='ValueError: bad row\nTraceback ...'),
    dict(id='r1', pipeline='orders', status='completed', source='api',
         created_at='2026-10-17T09:00:00Z', duration_ms=None, error=None),
]


class FakeApi(BaseHTTPRequestHandler):
    token = 't0ken'
    requests = []

    def do_GET(self):
        url = urlparse(self.path)
        query = {k: v[0] for k, v in parse_qs(url.query).items()}
        FakeApi.requests.append((url.path, query))
        if self.headers.get('Authorization') != f'Bearer {self.token}':
            return self._send(401, {'error': 'a valid token is required'})
        if url.path == '/v1/runs':
            return self._send(200, {'runs': RUNS[:int(query.get('limit', 50))]})
        if url.path == '/v1/runs/r2/logs':
            body = '2026-10-17T10:00:01Z [load] reading\n2026-10-17T10:00:02Z [load] failed\n'
            return self._send(200, body, 'text/plain')
        return self._send(404, {'error': f'no run {url.path.split("/")[3]}'})

    def _send(self, code, body, kind='application/json'):
        data = (json.dumps(body) if kind == 'application/json' else body).encode()
        self.send_response(code)
        self.send_header('Content-Type', kind)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class ClientTest(TestCase):
    def setUp(self):
        super().setUp()
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), FakeApi)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.port = self.server.server_address[1]
        FakeApi.requests = []

    def api(self, token='t0ken'):
        return client.Endpoint(f'http://127.0.0.1:{self.port}', token)

    def test_runs_are_listed_as_a_table_with_the_first_line_of_errors(self):
        table = client.runs_table(client.runs(self.api(), status='failed', limit=5))

        self.assertEqual(FakeApi.requests[-1], ('/v1/runs', {'status': 'failed', 'limit': '5'}))
        lines = table.splitlines()
        self.assertTrue(lines[0].startswith('RUN'))
        self.assertIn('schedule:hourly', lines[1])
        self.assertIn('1.5 s', lines[1])
        self.assertTrue(lines[1].endswith('ValueError: bad row'))
        self.assertNotIn('Traceback', table)

    def test_the_latest_run_log_is_the_default(self):
        text = client.run_log(self.api(), tail=10)

        self.assertIn('[load] failed', text)
        self.assertEqual(FakeApi.requests[-1],
                         ('/v1/runs/r2/logs', {'format': 'text', 'tail': '10'}))

    def test_errors_carry_the_api_message(self):
        with self.assertRaisesRegex(client.ClientError, '404: no run nope'):
            client.run_log(self.api(), run_id='nope')
        with self.assertRaisesRegex(client.ClientError, '401: a valid token is required'):
            client.runs(self.api(token='wrong'))
        self.server.shutdown()
        self.server.server_close()
        with self.assertRaisesRegex(client.ClientError, 'No service answers on'):
            client.runs(self.api())

    def test_the_local_run_and_the_deployment_are_found(self):
        root = Path(self.repo_path)
        with self.assertRaisesRegex(client.ClientError, 'mage service run shop'):
            client.endpoint(self.repo_path, 'shop')
        folder = root / '.mage' / 'run' / 'shop'
        folder.mkdir(parents=True)
        self.addCleanup(__import__('shutil').rmtree, root / '.mage', True)
        (folder / 'port').write_text(str(self.port))
        (folder / 'token').write_text('t0ken\n')
        self.assertEqual(client.endpoint(self.repo_path, 'shop'), self.api())

        docker = FakeDocker()
        target = deploy.DockerTarget(runner=docker, health=lambda port, timeout: True)
        with self.assertRaisesRegex(client.ClientError, 'not deployed in prod'):
            client.endpoint(self.repo_path, 'shop', 'prod', target=target)
        target.deploy('shop', 'prod', 'mage-shop:1', 7005, {}, {})
        with self.assertRaisesRegex(client.ClientError, 'No API token'):
            client.endpoint(self.repo_path, 'shop', 'prod', target=target)
        token_file = deploy.token_file(self.repo_path, 'shop', 'prod')
        token_file.parent.mkdir(parents=True)
        token_file.write_text('d3ploy')
        self.assertEqual(client.endpoint(self.repo_path, 'shop', 'prod', target=target),
                         client.Endpoint('http://127.0.0.1:7005', 'd3ploy'))
        self.assertEqual(target.log_command('shop', 'prod', tail=50, follow=True),
                         ['docker', 'logs', '--tail', '50', '--follow', 'mage-shop-prod'])
        with self.assertRaisesRegex(deploy.DeployError, 'not deployed in dev'):
            target.log_command('shop', 'dev')
