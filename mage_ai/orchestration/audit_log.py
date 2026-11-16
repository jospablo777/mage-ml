"""
The audit log: every create, update and delete through the API, with who made it, what it
wrote and how it ended, in the metadata database (AuditEvent). BaseOperation.execute calls
record() once per write; recording never fails the request.

Values are not kept as they are when they could hold secrets or are long: a secret's value,
and any attribute whose name says password, secret, token, key or credential, is [hidden];
a value longer than VALUE_LIMIT (code, a file) is kept as its length and SHA-256. Writes of
the same object by the same user and action within MERGE_SECONDS are one event with a count:
the editor saves a block as it is typed.

MAGE_AUDIT_LOG=off turns it off. Events older than MAGE_AUDIT_LOG_RETENTION_DAYS (365) are
deleted, at most once an hour.
"""
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

WRITE_ACTIONS = ('create', 'update', 'delete')
VALUE_LIMIT = 200
MERGE_SECONDS = 60
PRUNE_EVERY_SECONDS = 3600
SENSITIVE = re.compile(r'pass(word|wd)?|secret|token|api_?key|private_?key|credential', re.I)
# Values of these resources are all hidden.
SECRET_RESOURCES = ('secrets', 'oauth_tokens')
HIDDEN = '[hidden]'

_last_prune = 0.0


def enabled() -> bool:
    return os.getenv('MAGE_AUDIT_LOG', 'on').strip().lower() not in ('off', '0', 'false')


def retention_days() -> int:
    try:
        return max(1, int(os.getenv('MAGE_AUDIT_LOG_RETENTION_DAYS', '365')))
    except ValueError:
        return 365


def _digest(text: str) -> Dict:
    return dict(length=len(text), sha256=hashlib.sha256(text.encode('utf-8')).hexdigest())


def summarize(value: Any, name: str = '', hide: bool = False) -> Any:
    """What the log keeps of a written value."""
    if hide or SENSITIVE.search(name or ''):
        return HIDDEN
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, dict):
        return {k: summarize(v, k) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        items = [summarize(v, name) for v in value]
        text = json.dumps(items, sort_keys=True, default=str)
        return items if len(text) <= VALUE_LIMIT else _digest(text)
    text = value if isinstance(value, str) else str(value)
    return text if len(text) <= VALUE_LIMIT else _digest(text)


def changes_of(resource: str, payload: Dict) -> Optional[Dict]:
    """The attributes a write's payload sets: {"<resource>": {...}} besides api_key."""
    if not isinstance(payload, dict):
        return None
    bodies = [v for k, v in payload.items() if k != 'api_key' and isinstance(v, dict)]
    if not bodies:
        return None
    hide = resource in SECRET_RESOURCES
    return {k: summarize(v, k, hide=hide) for body in bodies for k, v in body.items()}


def record(
    action: str,
    resource: str,
    object_id: Any = None,
    parent: Optional[str] = None,
    parent_id: Any = None,
    user=None,
    payload: Optional[Dict] = None,
    outcome: str = 'succeeded',
    error: Optional[str] = None,
    source_ip: Optional[str] = None,
) -> None:
    if action not in WRITE_ACTIONS or not resource or not enabled():
        return
    try:
        _record(action, resource, object_id, parent, parent_id, user, payload, outcome,
                error, source_ip)
    except Exception as err:
        logging.getLogger(__name__).warning('The audit log could not record a %s of %s: %s',
                                            action, resource, err)
        try:
            from mage_ai.orchestration.db import db_connection

            db_connection.session.rollback()
        except Exception:
            pass


def _record(action, resource, object_id, parent, parent_id, user, payload, outcome, error,
            source_ip) -> None:
    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.models.audit_events import AuditEvent

    now = datetime.now(timezone.utc)
    user_id = getattr(user, 'id', None)
    name = getattr(user, 'email', None) or getattr(user, 'username', None)
    if name is None and resource == 'sessions':
        # A sign-in: the user is who it names, signed in or refused.
        session = (payload or {}).get('session') or {}
        name = session.get('email') or session.get('username')
        name = str(name)[:255] if name else None
    object_id = None if object_id is None else str(object_id)[:255]
    parent_id = None if parent_id is None else str(parent_id)[:255]
    changes = changes_of(resource, payload or {})
    error = (str(error)[:2000] if error else None)

    session = db_connection.session
    recent = session.query(AuditEvent).filter(
        AuditEvent.user_id == user_id,
        AuditEvent.action == action,
        AuditEvent.object_type == resource,
        AuditEvent.object_id == object_id,
        AuditEvent.outcome == outcome,
    ).order_by(AuditEvent.id.desc()).first()
    last = recent and (recent.last_at or recent.created_at)
    if last is not None and last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    if recent is not None and last and now - last <= timedelta(seconds=MERGE_SECONDS) \
            and action == 'update':
        merged = dict(recent.changes or {})
        merged.update(changes or {})
        recent.changes = merged
        recent.count = (recent.count or 1) + 1
        recent.last_at = now
        recent.error = error
        session.commit()
    else:
        AuditEvent.create(
            user_id=user_id, user_name=name, action=action, object_type=resource,
            object_id=object_id, parent=parent, parent_id=parent_id, outcome=outcome,
            error=error, changes=changes, source_ip=(source_ip or None), count=1, last_at=now,
        )
    prune()


def prune(force: bool = False) -> int:
    """Deletes events past the retention, at most once an hour. Returns how many."""
    global _last_prune

    from mage_ai.orchestration.db import db_connection
    from mage_ai.orchestration.db.models.audit_events import AuditEvent

    if not force and time.monotonic() - _last_prune < PRUNE_EVERY_SECONDS:
        return 0
    _last_prune = time.monotonic()
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days())
    deleted = db_connection.session.query(AuditEvent).filter(
        AuditEvent.created_at < cutoff,
    ).delete(synchronize_session=False)
    db_connection.session.commit()
    return deleted
