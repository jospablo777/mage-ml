"""
MAGE_GATEWAY=on: the web server listens on a loopback port of its own and the Rust gateway
(`mage-service gateway`, ADR-0007) takes the port users open, passing every request and
WebSocket to it. Route families move into the gateway one at a time later; for now it
changes nothing the browser sees. It needs the service engine (mage-ml[services]).
"""
import atexit
import os
import socket
import subprocess
import time
from typing import Optional

from mage_ai.pipeline_services.engine import engine_path

READY_SECONDS = 15
# The routes the gateway answers too, from the metadata database, and compares with the
# server's answers (gateway_shadow.rs); MAGE_GATEWAY_SHADOW=off turns them off.
SHADOW_ROUTES = 'block_runs,pipeline_run,pipeline_runs,pipeline_schedules'


def enabled() -> bool:
    return os.getenv('MAGE_GATEWAY', 'off').strip().lower() in ('1', 'on', 'true', 'yes')


def free_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def metadata_url() -> Optional[str]:
    """The server's metadata database as the gateway reads it; None when it cannot."""
    from mage_ai.orchestration.db import engine

    url = engine.url
    if url.get_backend_name() == 'sqlite' and url.database:
        return f'sqlite:///{os.path.abspath(url.database)}'
    if url.get_backend_name() == 'postgresql':
        return url.set(drivername='postgresql').render_as_string(hide_password=False)
    return None


def shadow_settings() -> tuple:
    """
    The gateway's --shadow arguments and environment. The database URL goes in the
    environment: on the command line, any local user would read its password with ps.
    """
    routes = os.getenv('MAGE_GATEWAY_SHADOW', SHADOW_ROUTES).strip()
    url = metadata_url()
    if routes.lower() in ('', 'off', '0', 'false') or not url:
        return [], {}
    from mage_ai.data_preparation.repo_manager import get_project_uuid
    from mage_ai.settings import (
        OAUTH2_APPLICATION_CLIENT_ID,
        REQUIRE_USER_AUTHENTICATION,
    )
    from mage_ai.settings.platform import project_platform_activated
    from mage_ai.settings.repo import get_repo_path
    from mage_ai.settings.server import HIDE_API_TRIGGER_TOKEN

    # What the server's decisions depend on, so the gateway decides as it does
    # (gateway_auth.rs); JWT_SECRET is in the environment the gateway inherits.
    serve = os.getenv('MAGE_GATEWAY_SERVE', '').strip()
    arguments = ['--shadow', routes]
    if serve and serve.lower() not in ('off', '0', 'false'):
        # Routes the gateway answers itself when its decision allows the read; off by
        # default until their shadow reads have stayed equal where they run.
        arguments += ['--serve', serve]
    return arguments, dict(
        MAGE_GATEWAY_METADATA_DB=url,
        MAGE_GATEWAY_REQUIRE_AUTHENTICATION='1' if REQUIRE_USER_AUTHENTICATION else '0',
        MAGE_GATEWAY_CLIENT_ID=OAUTH2_APPLICATION_CLIENT_ID,
        MAGE_GATEWAY_PROJECT_UUID=get_project_uuid() or '',
        MAGE_GATEWAY_HIDE_API_TRIGGER_TOKEN='1' if HIDE_API_TRIGGER_TOKEN else '0',
        # The pipelines' names and tags; none on the project platform, whose runs the
        # gateway leaves to the server.
        **({} if project_platform_activated() else dict(MAGE_GATEWAY_REPO_PATH=get_repo_path())),
    )


def start(host: Optional[str], port: int, upstream_port: int) -> subprocess.Popen:
    """
    Starts the gateway on host:port for the server on 127.0.0.1:upstream_port and waits
    until it accepts connections. Without a host (or with localhost) it listens on every
    interface, as the server does. It stops when this process exits.
    """
    listen_host = '0.0.0.0' if host in (None, '', 'localhost') else host
    listen = f'[{listen_host}]:{port}' if ':' in listen_host else f'{listen_host}:{port}'
    shadow, environment = shadow_settings()
    process = subprocess.Popen([
        str(engine_path()), 'gateway', '--listen', listen,
        '--upstream', f'127.0.0.1:{upstream_port}', '--exit-with-parent', *shadow,
    ], env={**os.environ, **environment})
    atexit.register(stop, process)
    probe = '127.0.0.1' if listen_host in ('0.0.0.0', '::') else listen_host
    deadline = time.monotonic() + READY_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f'The gateway exited with code {process.returncode} before it listened on '
                f'{listen}; its log is above.'
            )
        try:
            with socket.create_connection((probe, port), timeout=0.5):
                return process
        except OSError:
            time.sleep(0.1)
    stop(process)
    raise RuntimeError(f'The gateway did not listen on {listen} within {READY_SECONDS} s.')


def stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
