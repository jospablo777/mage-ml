"""
Start Feast as a microservice for the integration tests.

PostgreSQL holds the registry, the offline store (the driver_hourly_stats table) and the
online store. On start this seeds the offline table, applies the feature repository,
materializes the seed into the online store and serves Feast's feature server, plus two
routes Feast does not offer over HTTP: point-in-time historical retrieval and a listing
of the registry.
"""
import datetime as dt
import io
import json
import os
import subprocess
import time
from typing import Any, Dict, List, Optional

import dataset
import pandas as pd
import psycopg
import pyarrow as pa
import uvicorn
from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

REPO = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'feature_repo')
SETTINGS = dict(
    host=os.environ['FEAST_PG_HOST'],
    port=int(os.environ['FEAST_PG_PORT']),
    user=os.environ['FEAST_PG_USER'],
    password=os.environ['FEAST_PG_PASSWORD'],
)
DATABASE = os.environ['FEAST_PG_DB']

TABLE = """
CREATE TABLE IF NOT EXISTS driver_hourly_stats (
    driver_id bigint NOT NULL,
    event_timestamp timestamptz NOT NULL,
    created timestamptz NOT NULL,
    conv_rate real,
    acc_rate double precision,
    avg_daily_trips bigint,
    city text,
    active boolean
)
"""


def wait_for_postgres() -> None:
    for _ in range(120):
        try:
            with psycopg.connect(dbname='postgres', **SETTINGS):
                return
        except psycopg.OperationalError:
            time.sleep(1)
    raise RuntimeError('PostgreSQL did not start')


def seed() -> None:
    with psycopg.connect(dbname='postgres', autocommit=True, **SETTINGS) as conn:
        exists = conn.execute(
            'SELECT 1 FROM pg_database WHERE datname = %s', (DATABASE,),
        ).fetchone()
        if not exists:
            conn.execute(f'CREATE DATABASE {DATABASE}')
    with psycopg.connect(dbname=DATABASE, **SETTINGS) as conn:
        conn.execute(TABLE)
        conn.execute('TRUNCATE driver_hourly_stats')
        with conn.cursor().copy(
            f"COPY driver_hourly_stats ({', '.join(dataset.COLUMNS)}) FROM STDIN",
        ) as copy:
            for row in dataset.rows():
                copy.write_row([row[c] for c in dataset.COLUMNS])


def apply_and_materialize():
    from feast import FeatureStore

    subprocess.run(['feast', 'apply'], cwd=REPO, check=True)
    store = FeatureStore(repo_path=REPO)
    store.materialize(
        start_date=dataset.START - dt.timedelta(hours=1),
        end_date=dataset.START + dt.timedelta(hours=dataset.HOURS),
    )
    return store


def _json_default(value):
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    raise TypeError(f'{type(value).__name__} is not JSON serializable')


class HistoricalRequest(BaseModel):
    entity_rows: List[Dict[str, Any]] = Field(min_length=1)
    features: List[str] = []
    feature_service: Optional[str] = None
    full_feature_names: bool = False
    format: str = 'json'


def build_app(store):
    from feast.feature_server import get_app

    app = get_app(store)

    @app.post('/get-historical-features')
    def get_historical_features(request: HistoricalRequest):
        if bool(request.features) == bool(request.feature_service):
            raise HTTPException(422, 'Pass either features or feature_service')
        if request.format not in ('json', 'arrow'):
            raise HTTPException(422, "format must be 'json' or 'arrow'")
        entity_df = pd.DataFrame(request.entity_rows)
        if 'event_timestamp' not in entity_df:
            raise HTTPException(422, 'Each entity row needs an event_timestamp')
        entity_df['event_timestamp'] = pd.to_datetime(entity_df['event_timestamp'], utc=True)
        features = (
            store.get_feature_service(request.feature_service)
            if request.feature_service else request.features
        )
        try:
            # to_arrow keeps integers with NULL and zoned timestamps; to_df turns the
            # integers into float and the JSON writer of pandas 2 drops the offset.
            table = store.get_historical_features(
                entity_df=entity_df,
                features=features,
                full_feature_names=request.full_feature_names,
            ).to_arrow()
        except Exception as err:
            raise HTTPException(400, f'{type(err).__name__}: {err}')
        table = table.sort_by([('event_timestamp', 'ascending'), ('driver_id', 'ascending')])
        for index, field in enumerate(table.schema):
            if pa.types.is_timestamp(field.type) and field.type.tz is None:
                # The PostgreSQL offline store returns entity timestamps without a time
                # zone; Feast treats them as UTC.
                utc = pa.timestamp(field.type.unit, 'UTC')
                table = table.set_column(index, field.name, table.column(index).cast(utc))
        if request.format == 'arrow':
            sink = io.BytesIO()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return Response(sink.getvalue(), media_type='application/vnd.apache.arrow.stream')
        return Response(
            json.dumps(table.to_pylist(), default=_json_default, ensure_ascii=False),
            media_type='application/json',
        )

    @app.get('/registry')
    def registry():
        return JSONResponse(dict(
            project=store.project,
            entities=[
                dict(name=e.name, join_keys=[e.join_key], value_type=e.value_type.name)
                for e in store.list_entities()
            ],
            feature_views=[
                dict(
                    name=fv.name,
                    entities=fv.entities,
                    ttl_seconds=int(fv.ttl.total_seconds()) if fv.ttl else None,
                    features={f.name: str(f.dtype) for f in fv.features},
                    online=fv.online,
                )
                for fv in store.list_feature_views()
            ],
            feature_services=[
                dict(name=s.name, feature_views=[p.name for p in s.feature_view_projections])
                for s in store.list_feature_services()
            ],
            data_sources=[
                dict(name=s.name, type=type(s).__name__) for s in store.list_data_sources()
            ],
        ))

    return app


if __name__ == '__main__':
    wait_for_postgres()
    seed()
    store = apply_and_materialize()
    uvicorn.run(build_app(store), host='0.0.0.0', port=6566)
