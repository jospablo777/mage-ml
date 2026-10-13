from mage_ai.api.presenters.BasePresenter import BasePresenter


class StreamSampleRunPresenter(BasePresenter):
    default_attributes = [
        'block_uuid',
        'blocks',
        'error',
        'failing_message',
        'input_messages',
        'logs',
        'messages',
        'outputs',
        'sample',
        'seconds',
    ]
