import os

from mage_ai.io.file import FileIO
from mage_ai.settings.repo import get_repo_path
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/design/data-loading.md#example-loading-data-from-a-file
    """
    # In the project folder; a relative path wrote to wherever the server was started.
    filepath = os.path.join(get_repo_path(), 'titanic_clean.csv')
    FileIO().export(df, filepath)
