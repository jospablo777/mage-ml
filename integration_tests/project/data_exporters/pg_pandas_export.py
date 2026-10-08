from os import path

from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from mage_ai.settings.repo import get_repo_path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export(frame, **kwargs):
    upsert = bool(kwargs.get('upsert'))
    config = ConfigFileLoader(path.join(get_repo_path(), 'io_config.yaml'), 'default')
    with Postgres.with_config(config) as client:
        client.export(
            frame,
            schema_name=kwargs['schema'],
            table_name='dst',
            if_exists=kwargs.get('if_exists', 'replace'),
            unique_constraints=['id'] if upsert else None,
            unique_conflict_method='UPDATE' if upsert else None,
            verbose=False,
        )
