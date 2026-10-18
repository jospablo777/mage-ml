from mage_ai.api.presenters.BasePresenter import BasePresenter


class StreamSamplePresenter(BasePresenter):
    default_attributes = [
        'message_count',
        'messages',
        'name',
        'partitions',
        'redacted',
        'pipeline_uuid',
        'size_bytes',
        'updated_at',
    ]
