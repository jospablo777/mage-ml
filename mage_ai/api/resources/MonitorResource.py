import os

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.GenericResource import GenericResource
from mage_ai.settings.repo import get_repo_path

HISTORY = 50


def _history(record):
    return dict(
        baseline_run_id=record.get('baseline_run_id'),
        checked_at=record.get('checked_at'),
        checks=[
            {k: check.get(k) for k in ('check', 'column', 'detail', 'result', 'segment', 'value')}
            for check in record.get('checks') or []
        ],
        engines=record.get('engines') or {},
        pipeline_run_id=record.get('pipeline_run_id'),
        result=record.get('result'),
        silenced_until=record.get('silenced_until'),
    )


class MonitorResource(GenericResource):
    """
    The project's monitors (monitors/<name>.yaml): GET /api/monitors lists them with their
    latest result; GET /api/monitors/<name> returns the checks of the latest runs; PUT
    /api/monitors/<name> with {silence: {hours, reason}} silences its notifications, and
    {silence: null} ends the silence.
    """

    @classmethod
    def _load(cls, name: str, repo_path: str):
        from mage_ai.data_preparation.models.pipeline import Pipeline
        from mage_ai.orchestration import monitors

        monitor = monitors.load_monitor(name, repo_path)
        pipeline = Pipeline.get(monitor.pipeline, repo_path=repo_path, check_if_exists=True)
        return monitor, pipeline

    @classmethod
    def collection(cls, query, meta, user, **kwargs):
        from mage_ai.orchestration import monitors

        repo_path = get_repo_path(user=user)
        folder = os.path.join(repo_path, monitors.MONITORS_FOLDER)
        names = sorted(
            os.path.splitext(n)[0] for n in (os.listdir(folder) if os.path.isdir(folder) else [])
            if n.endswith(('.yaml', '.yml'))
        )
        listed = []
        for name in names:
            entry = dict(id=name, name=name)
            try:
                monitor, pipeline = cls._load(name, repo_path)
                entry.update(pipeline_uuid=monitor.pipeline, block_uuid=monitor.block)
                if pipeline is None:
                    entry['error'] = f'Pipeline {monitor.pipeline} does not exist.'
                else:
                    records = monitors.saved(pipeline, monitor)
                    entry['runs'] = len(records)
                    entry['silence'] = monitors.silenced(pipeline, monitor)
                    if records:
                        entry['latest'] = _history(records[-1])
            except monitors.MonitorError as error:
                entry['error'] = str(error)
            listed.append(entry)
        return cls.build_result_set(listed, user, **kwargs)

    @classmethod
    def member(cls, pk, user, **kwargs):
        from mage_ai.orchestration import monitors

        repo_path = get_repo_path(user=user)
        try:
            monitor, pipeline = cls._load(pk, repo_path)
        except monitors.MonitorError as error:
            raise ApiError(dict(code=404, message=str(error)))
        if pipeline is None:
            raise ApiError(dict(code=404, message=f'Pipeline {monitor.pipeline} does not exist.'))
        records = monitors.saved(pipeline, monitor)[-HISTORY:]
        return cls(dict(
            id=pk,
            name=pk,
            pipeline_uuid=monitor.pipeline,
            block_uuid=monitor.block,
            baseline=monitor.baseline,
            history=[_history(r) for r in records],
            runs=len(records),
            silence=monitors.silenced(pipeline, monitor),
        ), user, **kwargs)

    def update(self, payload, **kwargs):
        from mage_ai.orchestration import monitors

        if 'silence' not in payload:
            raise ApiError(dict(code=400, message='Send silence: {hours, reason}, or null.'))
        repo_path = get_repo_path(user=self.current_user)
        monitor, pipeline = self._load(self.model['name'], repo_path)
        settings = payload.get('silence')
        try:
            if settings is None:
                monitors.silence(pipeline, monitor, None)
            else:
                monitors.silence(
                    pipeline, monitor, settings.get('hours'), settings.get('reason') or '',
                    by=getattr(self.current_user, 'username', None),
                )
        except monitors.MonitorError as error:
            raise ApiError(dict(code=400, message=str(error)))
        self.model['silence'] = monitors.silenced(pipeline, monitor)
