{% extends "data_loaders/default.jinja" %}
{% block imports %}
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.db2 import Db2
from os import path
{{ super() -}}
{% endblock %}


{% block content %}
@data_loader
def load_data_from_db2(*args, **kwargs):
    """
    Template for loading data from an IBM Db2 database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/integrations/ibm-cloud.md#db2
    """
    query = 'your Db2 query'  # Specify your SQL query here
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Db2.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)
{% endblock %}
