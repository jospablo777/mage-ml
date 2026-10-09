from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, PushSource
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Bool, Float32, Float64, Int64, String
from feast.value_type import ValueType

driver = Entity(name='driver', join_keys=['driver_id'], value_type=ValueType.INT64)

driver_stats_source = PostgreSQLSource(
    name='driver_hourly_stats_source',
    query='SELECT * FROM driver_hourly_stats',
    timestamp_field='event_timestamp',
    created_timestamp_column='created',
)

# Rows pushed to this source go to the online store, the offline store, or both.
driver_stats_push = PushSource(name='driver_stats_push', batch_source=driver_stats_source)

driver_hourly_stats = FeatureView(
    name='driver_hourly_stats',
    entities=[driver],
    ttl=timedelta(days=3650),
    schema=[
        Field(name='conv_rate', dtype=Float32),
        Field(name='acc_rate', dtype=Float64),
        Field(name='avg_daily_trips', dtype=Int64),
        Field(name='city', dtype=String),
        Field(name='active', dtype=Bool),
    ],
    online=True,
    source=driver_stats_push,
)

driver_activity = FeatureService(name='driver_activity', features=[driver_hourly_stats])
