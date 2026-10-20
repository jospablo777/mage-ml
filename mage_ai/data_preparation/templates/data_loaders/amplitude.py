{% extends "data_loaders/default.jinja" %}
{% block imports %}
from datetime import timedelta

from mage_ai.io.amplitude import Amplitude
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path
{{ super() -}}
{% endblock %}


{% block content %}
@data_loader
def load_data_from_amplitude(*args, **kwargs):
    """
    Events uploaded to Amplitude in a range of hours (UTC), one row per event.
    Set AMPLITUDE_API_KEY, AMPLITUDE_SECRET_KEY and AMPLITUDE_REGION (US or EU) in
    io_config.yaml: Amplitude > Settings > Projects > your project > General.

    Other reads: amplitude.active_users(start, end), amplitude.average_session_length(...),
    amplitude.event_totals('Event name', start, end), amplitude.event_types(),
    amplitude.user_properties(), amplitude.cohorts(), amplitude.cohort_members(cohort_id).
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    amplitude = Amplitude.with_config(ConfigFileLoader(config_path, config_profile))

    # Yesterday by default; Amplitude finishes an hour about two hours after it ends.
    start = kwargs.get('start') or kwargs['execution_date'] - timedelta(days=1)
    end = kwargs.get('end') or start + timedelta(hours=23)
    return amplitude.load_events(
        start,
        end,
        # event_types=['Purchase'],     # only these events
        # expand_properties=True,      # event_properties.<name> columns
        # polars=True,                 # a Polars frame
    )
{% endblock %}
