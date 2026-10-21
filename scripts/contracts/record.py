"""
Records the contracts the Rust control plane is held to (ROADMAP P0) into contracts/:

    policy_matrix.json  what every API policy allows, by role, resource, action and setting
                        (mage_ai/api/contracts/policy_matrix.py)
    openapi.json        the API's routes and operations, with response schemas recorded
                        from a server it starts on a scratch project
                        (mage_ai/api/contracts/openapi.py)

    .venv/bin/python scripts/contracts/record.py

It runs in a scratch folder with its own database. The contract tests fail when a checked-in
file differs from what the code records.
"""
import json
import os
import queue
import sys
import tempfile
import threading
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = ROOT / 'contracts'


def main() -> int:
    scratch = tempfile.mkdtemp(prefix='mage-contracts-')
    os.environ['ENV'] = 'test_mage'
    # The policy matrix turns authentication on for its checks; the server runs without it.
    os.environ['REQUIRE_USER_AUTHENTICATION'] = '0'
    os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'
    os.chdir(scratch)
    sys.path.insert(0, str(ROOT))

    import logging

    from mage_ai.data_preparation.repo_manager import init_project_uuid
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.database_manager import database_manager
    from mage_ai.settings.repo import set_repo_path

    project = Path(scratch) / 'project'
    project.mkdir()
    set_repo_path(str(project))
    init_project_uuid()
    database_manager.run_migrations(log_level=logging.ERROR)
    db_connection.start_session(force=True)

    from mage_ai.api.contracts import policy_matrix

    matrix = policy_matrix.build(policy_matrix.make_users())
    CONTRACTS.mkdir(exist_ok=True)
    (CONTRACTS / 'policy_matrix.json').write_text(policy_matrix.dumps(matrix))
    print(f"policy_matrix.json: {matrix['policies']} policies, {len(matrix['actions'])} "
          f"action rows, {len(matrix['attributes'])} attribute rows")

    spec = record_openapi()
    (CONTRACTS / 'openapi.json').write_text(openapi_dumps(spec))
    print(f"openapi.json: {len(spec['paths'])} paths, {len(spec['x-mage-resources'])} "
          f"resources, {len(spec['x-mage-unrecorded'])} without a recorded response")
    return 0


def serve(ports: queue.Queue) -> None:
    import asyncio

    from tornado.httpserver import HTTPServer
    from tornado.testing import bind_unused_port

    from mage_ai.server.server import make_app

    asyncio.set_event_loop(asyncio.new_event_loop())
    sock, port = bind_unused_port()
    HTTPServer(make_app()).add_sockets([sock])
    ports.put(port)
    asyncio.get_event_loop().run_forever()


def record_openapi():
    from mage_ai.api.contracts import openapi
    from mage_ai.orchestration.db.models.schedules import PipelineRun
    from mage_ai.settings.repo import get_repo_path

    ports = queue.Queue()
    threading.Thread(target=serve, args=(ports,), daemon=True).start()
    base = f'http://127.0.0.1:{ports.get(timeout=60)}'

    def fetch(method, path, body=None):
        request = urllib.request.Request(
            base + path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Content-Type': 'application/json'},
            method=method.upper(),
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read() or b'null')

    def make_run(schedule_id):
        return PipelineRun.create(
            pipeline_schedule_id=schedule_id,
            pipeline_uuid=openapi.PIPELINE,
            variables={},
        ).id

    worker = Path(get_repo_path()) / 'python_workers' / 'contract_worker'
    worker.mkdir(parents=True)
    (worker / 'pyproject.toml').write_text(
        '[project]\nname = "contract-worker"\nversion = "0"\nrequires-python = ">=3.12"\n'
        'dependencies = []\n',
    )

    spec = openapi.static_spec()
    recorder, unreached = openapi.record(fetch, spec, make_run)
    for method, path, outcome in recorder.calls:
        print(f'  {method.upper()} {path}: {outcome}')
    return openapi.apply(spec, recorder, unreached)


def openapi_dumps(spec):
    from mage_ai.api.contracts import openapi

    return openapi.dumps(spec)


if __name__ == '__main__':
    sys.exit(main())
