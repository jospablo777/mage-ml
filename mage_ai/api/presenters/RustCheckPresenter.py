from mage_ai.api.presenters.BasePresenter import BasePresenter


class RustCheckPresenter(BasePresenter):
    default_attributes = [
        'block_uuid',
        'diagnostics',
        'pipeline_uuid',
        'seconds',
    ]
