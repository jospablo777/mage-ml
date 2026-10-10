import os

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path


class MonitorReportResource(GenericResource):
    """
    The HTML report an engine of a monitor saved for a pipeline run (Evidently, NannyML):
    GET /api/monitor_reports/<monitor>?pipeline_run_id=<id>&report=<file>.
    Only reports the run's record lists are served.
    """

    @classmethod
    def member(cls, pk, user, **kwargs) -> 'MonitorReportResource':
        from mage_ai.data_preparation.models.pipeline import Pipeline
        from mage_ai.orchestration import monitors

        query = kwargs.get('query') or {}

        def first(key):
            value = query.get(key)
            return value[0] if isinstance(value, list) and value else value

        repo_path = get_repo_path(user=user)
        try:
            monitor = monitors.load_monitor(pk, repo_path)
            run_id = int(first('pipeline_run_id'))
        except (monitors.MonitorError, TypeError, ValueError) as error:
            raise ApiError(dict(code=404, message=str(error)))
        report = first('report')
        pipeline = Pipeline.get(monitor.pipeline, repo_path=repo_path, check_if_exists=True)
        if pipeline is None:
            raise ApiError(dict(code=404, message='The pipeline does not exist.'))
        record = next(
            (r for r in monitors.saved(pipeline, monitor) if r['pipeline_run_id'] == run_id),
            None,
        )
        listed = {
            name for engine in ((record or {}).get('engines') or {}).values()
            for name in engine.get('reports') or []
        }
        if report not in listed:
            raise ApiError(dict(code=404, message='The run has no such report.'))
        path = os.path.join(monitors._records_dir(pipeline, monitor), str(run_id), report)
        storage = pipeline.variable_manager.storage
        if not storage.path_exists(path):
            raise ApiError(dict(code=404, message='The report file is gone.'))
        html = storage.read_bytes(path).decode('utf-8', errors='replace')
        return cls(dict(
            id=pk, monitor=pk, pipeline_run_id=run_id, pipeline_uuid=monitor.pipeline,
            report=report, html=html,
        ), user, **kwargs)
