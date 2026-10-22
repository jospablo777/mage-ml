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
SHADOW_ROUTES = 'block_runs,pipeline_run'


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
    return ['--shadow', routes], dict(MAGE_GATEWAY_METADATA_DB=url)


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
