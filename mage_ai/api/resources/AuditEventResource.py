from datetime import datetime

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.DatabaseResource import DatabaseResource
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.audit_events import AuditEvent

FILTERS = ('action', 'outcome', 'object_id', 'object_type', 'user_name')


def _first(query, name):
    value = (query or {}).get(name)
    if isinstance(value, list):
        value = value[0] if value else None
    return value if value not in (None, '') else None


def _time(value, name):
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        raise ApiError(dict(ApiError.RESOURCE_INVALID, message=(
            f'{name} must be a date or time in ISO format, such as 2026-11-16T09:00:00Z.'
        )))


class AuditEventResource(DatabaseResource):
    """
    GET /api/audit_events: the audit log of API writes, newest first (audit_log.py),
    filtered by user_name, object_type, object_id, action, outcome, since and until.
    """

    DEFAULT_LIMIT = 50
    model_class = AuditEvent

    @classmethod
    @safe_db_query
    def collection(cls, query_arg, meta, user, **kwargs):
        query = AuditEvent.query
        for name in FILTERS:
            value = _first(query_arg, name)
            if value is not None:
                query = query.filter(getattr(AuditEvent, name) == str(value))
        since = _first(query_arg, 'since')
        if since:
            query = query.filter(AuditEvent.created_at >= _time(since, 'since'))
        until = _first(query_arg, 'until')
        if until:
            query = query.filter(AuditEvent.created_at <= _time(until, 'until'))
        return query.order_by(AuditEvent.id.desc())
