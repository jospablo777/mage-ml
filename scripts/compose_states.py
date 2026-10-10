"""
Prints the services of a Docker Compose project that are not running and healthy as
GitHub Actions annotations, with the last lines of their logs, so a failed CI start-up
names the service and the reason.

    python scripts/compose_states.py integration_tests/compose.yaml
"""
import json
import subprocess
import sys

LOG_LINES = 25


def _escape(text: str) -> str:
    return text.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')


def services(compose_file: str):
    result = subprocess.run(
        ['docker', 'compose', '-f', compose_file, 'ps', '--all', '--format', 'json'],
        capture_output=True, text=True, check=True,
    )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if len(lines) == 1 and lines[0].startswith('['):
        return json.loads(lines[0])
    return [json.loads(line) for line in lines]


def logs(compose_file: str, service: str) -> str:
    result = subprocess.run(
        ['docker', 'compose', '-f', compose_file, 'logs', '--no-color', '--tail',
         str(LOG_LINES), service],
        capture_output=True, text=True,
    )
    return (result.stdout + result.stderr).strip()


def main(compose_file: str) -> int:
    for service in services(compose_file):
        state = service.get('State', '')
        health = service.get('Health', '')
        # One-shot setup containers finish with 0.
        if state == 'exited' and service.get('ExitCode') == 0:
            continue
        if state != 'running' or health not in ('', 'healthy'):
            name = service.get('Service', '?')
            text = f'{state} {health} {service.get("Status", "")}\n{logs(compose_file, name)}'
            print(f'::error title=Service {name}::{_escape(text)}')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else 'integration_tests/compose.yaml'))
