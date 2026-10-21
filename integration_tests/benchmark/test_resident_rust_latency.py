"""
Item 56: a Rust block that scores one row with an ONNX model, served by an exported service
over HTTP. With resident programs (the default) the program and its ONNX session load once;
with MAGE_SERVICE_RUST_RESIDENT=off every request starts the program and loads the model
again. Prints the p50 and p95 of each and checks that resident requests are faster.
"""
import json
import shutil
import statistics
import time
import urllib.request
from pathlib import Path

import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    build_rust_blocks,
    export_service,
    service_binary,
)
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build

REPO = Path(__file__).resolve().parents[2]
MODEL = REPO / 'mage_ai/data_preparation/models/block/rust/sdk/tests/fixtures/classifier.onnx'

SCORE = '''
use mage::prelude::*;

fn load_data(vars: Vars) -> Result<DataFrame> {
    let a: f64 = vars.get_or("a", 0.0)?;
    let b: f64 = vars.get_or("b", 5.0)?;
    let row = df!("a" => [a], "b" => [b])?;
    let model = mage::onnx::model("models/classifier.onnx")?;
    Ok(model.predict(&row, &["a", "b"])?)
}
'''

REQUESTS = 40


def request(port: int, pipeline: str, block: str, a: float) -> dict:
    body = json.dumps(dict(variables=dict(a=a, b=5.0 - a))).encode()
    sent = urllib.request.Request(
        f'http://127.0.0.1:{port}/v1/pipelines/{pipeline}/runs?wait=60&output={block}',
        data=body, method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(sent, timeout=90) as response:
        return json.loads(response.read())


def latencies(directory: Path, data: Path, pipeline: str, block: str, check=None,
              **env) -> list:
    service = RunningService(service_binary(), directory, data, **env)
    service.start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                service.get('/healthz')
                break
            except OSError:
                time.sleep(0.25)
        run = request(service.port, pipeline, block, 0.0)  # warm: the first start
        assert run['status'] == 'completed', run
        (check or check_scores)(run)
        times = []
        for i in range(REQUESTS):
            started = time.perf_counter()
            run = request(service.port, pipeline, block, float(i % 6))
            times.append((time.perf_counter() - started) * 1000)
            assert run['status'] == 'completed', run
            (check or check_scores)(run)
        return times
    finally:
        service.stop()


def check_scores(run: dict) -> None:
    assert set(run['output'][0]) == {'label', 'probabilities_0', 'probabilities_1'}, run


def summary(times: list) -> str:
    ordered = sorted(times)
    return (f'p50 {statistics.median(ordered):.1f} ms, '
            f'p95 {ordered[int(len(ordered) * 0.95) - 1]:.1f} ms')


@pytest.mark.timeout(3600)
def test_a_resident_rust_program_scores_without_reloading_the_model(mage_project, tmp_path):
    from integration_tests.topologies.test_inference import enable_onnx

    if shutil.which('cargo') is None:
        pytest.skip('Rust is not installed')
    models = Path(mage_project) / 'models'
    models.mkdir(exist_ok=True)
    shutil.copy2(MODEL, models / 'classifier.onnx')
    enable_onnx(mage_project)
    pipeline = build(mage_project, [
        dict(uuid='score', type='data_loader', language='rust', code=SCORE),
    ])
    directory = export_service(mage_project, pipeline, tmp_path)
    build_rust_blocks(directory)
    block = block_uuid(pipeline, 'score')

    resident = latencies(directory, tmp_path / 'resident', pipeline, block)
    per_run = latencies(directory, tmp_path / 'per_run', pipeline, block,
                        MAGE_SERVICE_RUST_RESIDENT='off')

    print(f'\nresident: {summary(resident)}\nper run:  {summary(per_run)}')
    assert statistics.median(resident) < statistics.median(per_run), (resident, per_run)
