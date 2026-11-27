from mage_ai.api.presenters.BasePresenter import BasePresenter


class ChangePreviewPresenter(BasePresenter):
    default_attributes = [
        'baseline_partition',
        'baseline_run_id',
        'blocks',
        'changed_files',
        'created_at',
        'created_by',
        'error',
        'finished_at',
        'id',
        'keys',
        'pipeline_uuid',
        'started_at',
        'state',
    ]
