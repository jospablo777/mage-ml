from mage_ai.api.presenters.BasePresenter import BasePresenter


class StreamDeadLetterPresenter(BasePresenter):
    default_attributes = [
        'letters',
        'pipeline_uuid',
        'redacted',
        'replayed',
        'sample',
        'source_topic',
        'topic',
        'total',
    ]
