from mage_ai.api.presenters.BasePresenter import BasePresenter


class DatasetPresenter(BasePresenter):
    default_attributes = [
        'block',
        'block_run_id',
        'checks',
        'columns',
        'data_through',
        'dependencies',
        'description',
        'freshness',
        'id',
        'max_age',
        'name',
        'output',
        'owner',
        'pipeline',
        'pipeline_run_id',
        'published_at',
        'rows',
        'runs',
        'source',
        'time_column',
    ]
