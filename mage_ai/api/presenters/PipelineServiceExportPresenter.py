from mage_ai.api.presenters.BasePresenter import BasePresenter


class PipelineServiceExportPresenter(BasePresenter):
    # An export just made carries files, notes, packages and blocks per pipeline; a listed
    # one carries counts and when it was exported.
    default_attributes = [
        'blocks',
        'download_token',
        'environment',
        'exported_at',
        'files',
        'models',
        'name',
        'notes',
        'packages',
        'path',
        'pipelines',
        'problems',
        'progress',
        'schedules',
        'stage',
        'state',
    ]
