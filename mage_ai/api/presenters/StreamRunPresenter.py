from mage_ai.api.presenters.BasePresenter import BasePresenter


class StreamRunPresenter(BasePresenter):
    default_attributes = [
        'acknowledged',
        'alerts',
        'assigned',
        'batches',
        'blocks',
        'connections',
        'dead_lettered_batches',
        'dead_lettered_messages',
        'error',
        'exit_code',
        'lag',
        'lag_history',
        'last_error',
        'logs',
        'messages_in',
        'messages_out',
        'notes',
        'paused',
        'pipeline_uuid',
        'rate_in',
        'rate_out',
        'retries',
        'running',
        'started_at',
        'state',
        'tap',
    ]
