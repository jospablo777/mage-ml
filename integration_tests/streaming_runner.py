"""
Run a streaming pipeline the two ways Mage does, until the test stops it:

- trigger: the scheduler's run_pipeline for a pipeline run, in its own process, as a
  trigger's job runs it.
- notebook: the websocket server's run_pipeline in a multiprocessing.Process, as the
  notebook's Execute pipeline button runs it, with the messages it sends to the notebook.
  Cancel terminates the process.
"""
import multiprocessing
import queue as queue_module
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta

MODES = ['trigger', 'notebook']


def _run_trigger(pipeline_run_id, variables):
    from mage_ai.orchestration.db.process import start_session_and_run
    from mage_ai.orchestration.pipeline_scheduler_original import run_pipeline

    # Mage's job workers run jobs this way.
    start_session_and_run(run_pipeline, pipeline_run_id, variables, {})


class Run:
    def __init__(self, process, messages=None):
        self.process = process
        self.messages = messages if messages is not None else []

    def text(self):
        lines = []
        for message in list(self.messages):
            value = message.get('message')
            lines.extend(value if isinstance(value, list) else [str(value)])
        return '\n'.join(lines)

    def check_alive(self):
        assert self.process.is_alive(), f'The pipeline stopped:\n{self.text()[-4000:]}'


@contextmanager
def streaming_pipeline(pipeline_uuid, mode, **variables):
    from mage_ai.data_preparation.models.pipeline import Pipeline

    if mode == 'trigger':
        from mage_ai.orchestration.triggers.api import trigger_pipeline

        pipeline_run = trigger_pipeline(pipeline_uuid, variables=variables)
        process = multiprocessing.Process(
            target=_run_trigger, args=(pipeline_run.id, pipeline_run.get_variables()),
        )
        run = Run(process)
    else:
        from mage_ai.server.websocket_server import run_pipeline

        # The variables the websocket server adds for a notebook run.
        now = datetime.now()
        global_vars = dict(
            variables, env='dev', execution_date=now,
            interval_end_datetime=now + timedelta(days=1), interval_seconds=None,
            interval_start_datetime=now, interval_start_datetime_previous=None, event={},
        )
        output = multiprocessing.Queue()
        process = multiprocessing.Process(
            target=run_pipeline,
            args=(Pipeline.get(pipeline_uuid), None, global_vars, output),
        )
        run = Run(process)

        def collect():
            while process.is_alive() or not output.empty():
                try:
                    run.messages.append(output.get(timeout=0.2))
                except queue_module.Empty:
                    pass

    process.start()
    if mode == 'notebook':
        collector = threading.Thread(target=collect, daemon=True)
        collector.start()
    try:
        yield run
    finally:
        process.terminate()
        process.join(15)
        if process.is_alive():
            process.kill()
            process.join(5)


def wait_until(predicate, run, timeout=90, message='condition'):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = predicate()
        if result:
            return result
        run.check_alive()
        time.sleep(0.5)
    raise AssertionError(f'Timed out waiting for {message}:\n{run.text()[-4000:]}')
