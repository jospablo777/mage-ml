from mage_ai.api.presenters.BasePresenter import BasePresenter


class MonitorReportPresenter(BasePresenter):
    default_attributes = [
        'html',
        'id',
        'monitor',
        'pipeline_run_id',
        'pipeline_uuid',
        'report',
    ]
