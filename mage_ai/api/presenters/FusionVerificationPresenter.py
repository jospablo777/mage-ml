from mage_ai.api.presenters.BasePresenter import BasePresenter


class FusionVerificationPresenter(BasePresenter):
    default_attributes = [
        'exact',
        'finished_at',
        'id',
        'log',
        'pipeline_uuid',
        'report',
        'started_at',
        'status',
    ]
