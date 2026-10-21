"""
Item 56: an R block served by an exported service over HTTP. With resident R sessions (the
default) R and the packages a block loads start once per worker; with
MAGE_SERVICE_R_RESIDENT=off every request starts Rscript and loads them again. Prints the
p50 and p95 of each and checks that resident requests are faster.
"""
import pytest

from integration_tests.benchmark.test_resident_rust_latency import latencies, summary
from integration_tests.exported_services import export_service
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build

LOAD = '''
suppressPackageStartupMessages(library(tidyverse))

#* @data_loader
load_data <- function(...) {
  tibble(a = as.numeric(global_vars$a), doubled = as.numeric(global_vars$a) * 2)
}
'''


@pytest.mark.timeout(3600)
def test_a_resident_r_session_serves_requests_without_starting_r(mage_project, tmp_path):
    import statistics

    pipeline = build(mage_project, [dict(uuid='load', type='data_loader', language='r',
                                         code=LOAD)])
    directory = export_service(mage_project, pipeline, tmp_path)
    block = block_uuid(pipeline, 'load')

    resident = latencies(directory, tmp_path / 'resident', pipeline, block, check=check)
    per_run = latencies(directory, tmp_path / 'per_run', pipeline, block, check=check,
                        MAGE_SERVICE_R_RESIDENT='off')

    print(f'\nresident: {summary(resident)}\nper run:  {summary(per_run)}')
    assert statistics.median(resident) < statistics.median(per_run), (resident, per_run)


def check(run):
    assert run['output'][0]['doubled'] == run['output'][0]['a'] * 2, run
