from mage_integrations.sources.constants import (
    REPLICATION_METHOD_FULL_TABLE,
    REPLICATION_METHOD_INCREMENTAL,
)

STREAM_EVENTS = 'events'
STREAM_ACTIVE_USERS = 'active_users'
STREAM_NEW_USERS = 'new_users'
STREAM_AVERAGE_SESSION_LENGTH = 'average_session_length'
STREAM_SESSIONS_PER_USER = 'sessions_per_user'
STREAM_EVENT_TYPES = 'event_types'
STREAM_EVENT_PROPERTIES = 'event_properties'
STREAM_USER_PROPERTIES = 'user_properties'
STREAM_ANNOTATIONS = 'annotations'
STREAM_COHORTS = 'cohorts'
STREAM_COHORT_MEMBERS = 'cohort_members'

METRIC_STREAMS = (
    STREAM_ACTIVE_USERS,
    STREAM_NEW_USERS,
    STREAM_AVERAGE_SESSION_LENGTH,
    STREAM_SESSIONS_PER_USER,
)

TABLE_KEY_PROPERTIES = {
    STREAM_EVENTS: ['uuid'],
    STREAM_ACTIVE_USERS: ['date', 'segment'],
    STREAM_NEW_USERS: ['date', 'segment'],
    STREAM_AVERAGE_SESSION_LENGTH: ['date', 'segment'],
    STREAM_SESSIONS_PER_USER: ['date', 'segment'],
    STREAM_EVENT_TYPES: ['event_type'],
    STREAM_EVENT_PROPERTIES: ['event_type', 'event_property'],
    STREAM_USER_PROPERTIES: ['user_property'],
    STREAM_ANNOTATIONS: ['id'],
    STREAM_COHORTS: ['id'],
    STREAM_COHORT_MEMBERS: ['cohort_id', 'amplitude_id'],
}

# Events are exported by the hour Amplitude received them; metrics by day.
REPLICATION_KEYS = {
    STREAM_EVENTS: 'server_upload_time',
    STREAM_ACTIVE_USERS: 'date',
    STREAM_NEW_USERS: 'date',
    STREAM_AVERAGE_SESSION_LENGTH: 'date',
    STREAM_SESSIONS_PER_USER: 'date',
}

REPLICATION_METHODS = {
    stream: REPLICATION_METHOD_INCREMENTAL if stream in REPLICATION_KEYS
    else REPLICATION_METHOD_FULL_TABLE
    for stream in TABLE_KEY_PROPERTIES
}

# The first sync, without start_date, reads this far back.
DEFAULT_START_DAYS = 30
# Metrics of recent days change as late events arrive: each sync reads them again.
DEFAULT_METRICS_LOOKBACK_DAYS = 3
SAMPLE_ROWS = 100
