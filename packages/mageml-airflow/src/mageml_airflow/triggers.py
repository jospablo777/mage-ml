"""Waits for a MageML run in Airflow's triggerer, so a deferred task holds no worker slot."""
import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

FINISHED = ('completed', 'failed', 'cancelled')


class MageMLRunTrigger(BaseTrigger):
    def __init__(self, run_id: str, mageml_conn_id: str, poll_interval: float = 5.0):
        super().__init__()
        self.run_id = run_id
        self.mageml_conn_id = mageml_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            'mageml_airflow.triggers.MageMLRunTrigger',
            dict(run_id=self.run_id, mageml_conn_id=self.mageml_conn_id,
                 poll_interval=self.poll_interval),
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from mageml_airflow.hooks import MageMLServiceError, MageMLServiceHook

        hook = MageMLServiceHook(self.mageml_conn_id)
        while True:
            try:
                run = await asyncio.to_thread(hook.run, self.run_id)
            except MageMLServiceError as error:
                # A service that restarts answers again; a run it does not know is final.
                if error.status == 404:
                    yield TriggerEvent(dict(run_id=self.run_id, status='error',
                                            message=str(error)))
                    return
                await asyncio.sleep(self.poll_interval)
                continue
            if run.get('status') in FINISHED:
                yield TriggerEvent(dict(run_id=self.run_id, status=run['status']))
                return
            await asyncio.sleep(self.poll_interval)
