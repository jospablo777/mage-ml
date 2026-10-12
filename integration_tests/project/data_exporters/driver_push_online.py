from mage_ai.feature_stores.feast import FeastClient

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(features, *args, **kwargs):
    with FeastClient() as feast:
        feast.push('driver_stats_push', features)
