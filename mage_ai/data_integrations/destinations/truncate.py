import subprocess
from typing import Dict, List

from mage_ai.data_integrations.logger.utils import print_log_from_line
from mage_ai.shared.security import filter_out_config_values

# Destination config: empty the table once per run before a FULL_TABLE stream writes to it.
CONFIG_KEY_TRUNCATE_BEFORE_FULL_TABLE_SYNC = 'truncate_before_full_table_sync'
REPLICATION_METHOD_FULL_TABLE = 'FULL_TABLE'


def should_truncate(config: Dict, replication_method: str) -> bool:
    return bool(
        config
        and config.get(CONFIG_KEY_TRUNCATE_BEFORE_FULL_TABLE_SYNC)
        and replication_method == REPLICATION_METHOD_FULL_TABLE
    )


def truncate_destination_table(
    args: List[str],
    config: Dict,
    logger=None,
    logging_tags: Dict = None,
    tags: Dict = None,
) -> None:
    """Run the destination in truncate mode and wait for it.

    args is the destination command without an input file. The caller runs this once per
    sync, before any batch of the stream is written, so batches that run in parallel never
    lose rows to a later truncate.
    """
    proc = subprocess.Popen(
        args + ['--truncate_table'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    for line in proc.stdout:
        print_log_from_line(
            line,
            config=config,
            logger=logger,
            logging_tags=logging_tags,
            tags=tags,
        )
    proc.communicate()
    if proc.returncode:
        raise subprocess.CalledProcessError(
            proc.returncode,
            filter_out_config_values(str(proc.args), config),
        )
