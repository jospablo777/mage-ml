"""
Kubernetes diagnostics without credentials.

The Kubernetes client logs every response body at DEBUG ("response body: ..."), and its
ApiException prints the response headers and body. A Job's or a workload's body holds its
environment, so with SERVER_VERBOSITY=DEBUG, or in a logged traceback, credentials passed to
a block or a workspace reached the logs.

protect() caps the client's response logger at INFO and makes ApiException print its status,
its reason and the paths of the fields Kubernetes rejected, never values, headers or the body.
The exception keeps .status, .reason, .headers and .body for code that reads them.
"""
import json
import logging

RESPONSE_LOGGER = 'kubernetes.client.rest'


def describe(error) -> str:
    """The status, the reason and the rejected field paths of a Kubernetes API error."""
    message = f'Kubernetes API error ({error.status}): {error.reason}'
    try:
        status = json.loads(error.body) if error.body else {}
    except (TypeError, ValueError):
        status = {}
    if isinstance(status, dict):
        if status.get('reason'):
            message += f'; {status["reason"]}'
        details = status.get('details') if isinstance(status.get('details'), dict) else {}
        if details.get('kind'):
            message += f'; {details["kind"]} {details.get("name") or ""}'.rstrip()
        fields = [
            f'{cause.get("reason") or "Invalid"} {cause["field"]}'
            for cause in details.get('causes') or []
            if isinstance(cause, dict) and cause.get('field')
        ]
        if fields:
            message += '; fields: ' + ', '.join(fields)
    return message


def _safe_str(error) -> str:
    return describe(error)


def protect() -> None:
    """Idempotent; called where Mage creates Kubernetes clients and when logging is set up."""
    response_logger = logging.getLogger(RESPONSE_LOGGER)
    if response_logger.level == logging.NOTSET or response_logger.level < logging.INFO:
        response_logger.setLevel(logging.INFO)
    try:
        from kubernetes.client.exceptions import ApiException
    except ImportError:
        return
    if ApiException.__str__ is not _safe_str:
        ApiException.__str__ = _safe_str
