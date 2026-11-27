from mage_ai.api.presenters.BasePresenter import BasePresenter


class IncidentPresenter(BasePresenter):
    default_attributes = [
        'blocks',
        'failed_runs',
        'id',
        'latest_at',
        'latest_run_id',
        'partition',
        'pipeline_uuid',
        'recovered_by',
    ]
