"""
Audit records from a Rust block of an exported service: the block scores with its own
code and records each decision with mage::models::audit; the engine finds the records by
key and checks their chain. A column hashed with MAGE_SERVICE_AUDIT_KEY in Rust gives the
hash Python gives the same value, so records from both languages match.
"""
import hashlib
import hmac
import json
import time
import urllib.request

import pytest

from integration_tests.exported_services import (
    TOKEN,
    RunningService,
    build_rust_blocks,
    service_binary,
)
from integration_tests.models.test_challengers import register
from integration_tests.postgres.test_sql_block_matrix import block_uuid, build

SCORE = '''
use mage::prelude::*;

fn load_data(context: BlockContext, vars: Vars) -> Result<DataFrame> {
    let ids: Vec<i64> = vars
        .get::<String>("ids")?
        .split(',')
        .map(|i| i.parse().map_err(|e| anyhow!("{e}")))
        .collect::<Result<_>>()?;
    let x: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
    let email: Vec<String> = ids.iter().map(|i| format!("person{i}@corp.org")).collect();
    let frame = df!("id" => &ids, "x" => &x, "email" => &email)?;
    let score = Series::new("score".into(), x.iter().map(|v| v * 2.0).collect::<Vec<f64>>());
    mage::models::audit(&context, "churn", &frame, "id", &["x", "email"], &score)?;
    Ok(frame.hstack(&[score.into()])?)
}
'''


def request(port, pipeline, block, ids):
    body = json.dumps(dict(variables=dict(ids=','.join(map(str, ids))))).encode()
    sent = urllib.request.Request(
        f'http://127.0.0.1:{port}/v1/pipelines/{pipeline}/runs?wait=120&output={block}',
        data=body, method='POST',
        headers={'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'},
    )
    with urllib.request.urlopen(sent, timeout=180) as response:
        return json.loads(response.read())


@pytest.mark.timeout(1800)
def test_a_rust_block_records_its_decisions(mage_project, mlflow_url, tmp_path, monkeypatch):
    from mage_ai.pipeline_services import export

    registered = register(mlflow_url, monkeypatch)
    pipeline = build(mage_project, [dict(
        uuid='score', type='data_loader', code=SCORE, language='rust',
    )])
    captured = export.capture(
        mage_project, [pipeline], name='rust-scoring',
        models=[f'churn=models:/{registered}/1'],
        audits=dict(churn=dict(features='values', mask=[], hash=['email'], reasons=0)),
    )
    directory = export.write(captured, str(tmp_path / 'service'), force=True)
    build_rust_blocks(directory)
    service = RunningService(
        service_binary(), directory, tmp_path / 'data', MAGE_SERVICE_AUDIT_KEY='audit-key',
    )
    service.start()
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                service.get('/healthz')
                break
            except OSError:
                time.sleep(0.25)
        block = block_uuid(pipeline, 'score')
        for ids in ([1, 2, 3], [4, 5]):
            run = request(service.port, pipeline, block, ids)
            assert run['status'] == 'completed', run

        [record] = service.get('/v1/audit/churn?key=4')['records']
        manifest = json.loads((directory / 'service.json').read_text())
        [scored] = manifest['pipelines'][0]['blocks']
        assert (record['output'], record['features']['x'], record['version']) == (
            8.0, 4.0, '1')
        assert record['features']['email'] == hmac.new(
            b'audit-key', json.dumps('person4@corp.org').encode(), hashlib.sha256,
        ).hexdigest()
        assert (record['pipeline'], record['block']) == (pipeline, block)
        assert record['block_sha256'] == scored['sha256']
        assert record['service'] == 'rust-scoring' and record['run_id']
        verified = service.get('/v1/audit/churn/verify')
        assert verified['intact'] and sum(f['records'] for f in verified['files']) == 5
    finally:
        service.stop()
