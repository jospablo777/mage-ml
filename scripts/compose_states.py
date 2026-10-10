"""
Prints the services of a Docker Compose project that are not running and healthy as
GitHub Actions annotations, so a failed CI start-up names the service.

    docker compose -f integration_tests/compose.yaml ps --all --format json \
        | python scripts/compose_states.py
"""
import json
import sys


def main(text: str) -> int:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) == 1 and lines[0].startswith('['):
        services = json.loads(lines[0])
    else:
        services = [json.loads(line) for line in lines]
    for service in services:
        state = service.get('State', '')
        health = service.get('Health', '')
        # One-shot setup containers finish with 0.
        if state == 'exited' and service.get('ExitCode') == 0:
            continue
        if state != 'running' or health not in ('', 'healthy'):
            name = service.get('Service', '?')
            print(f'::error title=Service {name}::{state} {health} {service.get("Status", "")}')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.stdin.read()))
