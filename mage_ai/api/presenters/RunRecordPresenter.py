from mage_ai.api.presenters.BasePresenter import BasePresenter


class RunRecordPresenter(BasePresenter):
    default_attributes = [
        'captured_at',
        'captures',
        'code',
        'code_changed_between_starts',
        'comparison',
        'comparison_error',
        'environment',
        'experiments',
        'git',
        'id',
        'outputs',
        'pipeline_run_id',
        'pipeline_uuid',
        'releases',
        'reproduction',
        'variables',
    ]
