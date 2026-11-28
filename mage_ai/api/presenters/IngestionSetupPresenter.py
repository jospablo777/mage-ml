from mage_ai.api.presenters.BasePresenter import BasePresenter


class IngestionSetupPresenter(BasePresenter):
    default_attributes = [
        'action',
        'blocks',
        'checks',
        'columns',
        'pipeline_uuid',
        'ready',
        'review',
        'rows',
        'table',
        'tables',
    ]
