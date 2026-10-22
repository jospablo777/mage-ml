import importlib
import json
import os
from typing import Dict, List, Optional

from mage_ai.data_integrations.utils.settings import get_uuid
from mage_ai.server.logger import Logger

logger = Logger().new_server_logger(__name__)


def read_docs(directory: str, uuid: str) -> str:
    """The connector's README, shown next to its settings; README.md or Readme.md."""
    for name in ('README.md', 'Readme.md', 'readme.md'):
        try:
            with open(f'{directory}/{name}', encoding='utf8') as f:
                return f.read()
        except FileNotFoundError:
            continue
    return f'{uuid} documentation'


def connector_folder(key: str, uuid: str) -> Optional[str]:
    """The connector's folder, found without importing the connector."""
    try:
        package = importlib.import_module(f'mage_integrations.{key}')
    except ImportError:
        return None
    folder = os.path.join(os.path.dirname(package.__file__), uuid)
    return folder if os.path.isdir(folder) else None


def templates_from_files(key: str, uuid: str) -> Optional[Dict]:
    """The connector's templates/*.json, read without importing it; None without the folder."""
    connector = connector_folder(key, uuid)
    folder = os.path.join(connector, 'templates') if connector else None
    if not folder or not os.path.isdir(folder):
        return None
    templates = {}
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        if name.endswith('.json') and os.path.isfile(path):
            with open(path, encoding='utf8') as f:
                templates[name[:-len('.json')]] = json.load(f)
    return templates


def build_integration_module_info(key: str, option: Dict) -> Optional[Dict]:
    """
    A connector as the lists show it, with its README and templates read from its folder.
    Importing each connector to ask for them loaded every connector's client libraries
    into the web server, and one of them left the process unable to fork: each terminal
    started after the integrations list died of SIGSEGV before its shell ran.
    """
    d = option.copy()
    uuid = get_uuid(d)
    d['uuid'] = uuid
    templates = templates_from_files(key, uuid)
    if templates is None:
        logger.error(f'Connector {uuid} has no templates folder in mage_integrations.{key}.')
        return None
    d['docs'] = read_docs(connector_folder(key, uuid), uuid)
    d['templates'] = templates
    return d


def get_collection(key: str, available_options: List[Dict]):
    collection = []

    for option in available_options:
        d = build_integration_module_info(key, option)
        if d:
            collection.append(d)

    return collection
