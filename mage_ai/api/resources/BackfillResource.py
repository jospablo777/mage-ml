from datetime import datetime

import pytz
from sqlalchemy import desc

from mage_ai.api.errors import ApiError
from mage_ai.api.resources.DatabaseResource import DatabaseResource
from mage_ai.api.resources.shared.parents import parent_of
from mage_ai.orchestration.backfills.service import (
    BackfillError,
    cancel_backfill,
    start_backfill,
)
from mage_ai.orchestration.db import safe_db_query
from mage_ai.orchestration.db.models.schedules import Backfill
from mage_ai.shared.hash import extract, merge_dict

ALLOWED_PAYLOAD_KEYS = [
    'block_uuid',
    'end_datetime',
    'interval_type',
    'interval_units',
    'name',
    'settings',
    'start_datetime',
    'variables',
]


def checked(payload: dict) -> dict:
    """The payload with its interval units as a positive whole number. Zero or a negative
    number made building the backfill's dates loop forever."""
    units = payload.get('interval_units')
    if units is None:
        return payload
    try:
        number = int(units)
        valid = not isinstance(units, bool) and float(units) == number and number > 0
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ApiError(dict(
            code=400,
            message=f'The interval units must be a positive whole number, not {units!r}.',
            type='invalid_interval_units',
        ))
    return merge_dict(payload, dict(interval_units=number))


class BackfillResource(DatabaseResource):
    datetime_keys = ['start_datetime', 'end_datetime']
    model_class = Backfill

    @classmethod
    @safe_db_query
    def collection(self, query_arg, meta, user, **kwargs):
        results = Backfill.query

        pipeline_uuid = query_arg.get('pipeline_uuid', [None])
        if pipeline_uuid:
            pipeline_uuid = pipeline_uuid[0]
        if pipeline_uuid:
            results = results.filter(Backfill.pipeline_uuid == pipeline_uuid)

        return results.order_by(desc(Backfill.created_at))

    @classmethod
    @safe_db_query
    def create(self, payload, user, **kwargs):
        pipeline_uuid = parent_of(kwargs, 'backfills', 'pipelines').uuid

        return super().create(merge_dict(
            checked(extract(payload, ALLOWED_PAYLOAD_KEYS)),
            dict(pipeline_uuid=pipeline_uuid),
        ), user, **kwargs)

    @safe_db_query
    def update(self, payload, **kwargs):
        pipeline_runs = []

        if 'status' in payload and payload['status'] != self.status:
            if Backfill.Status.INITIAL == payload['status']:
                try:
                    pipeline_runs += start_backfill(self.model)
                except BackfillError as error:
                    raise ApiError(dict(code=400, message=str(error)))
                return super().update(dict(
                    started_at=datetime.now(tz=pytz.UTC),
                    status=payload['status'],
                ))
            elif Backfill.Status.CANCELLED == payload['status']:
                cancel_backfill(self.model)
                return super().update(dict(status=payload['status']))

        return super().update(checked(extract(payload, ALLOWED_PAYLOAD_KEYS)))
