"""
Runs inside an exported stream service's image: the Python worker runs the exported stream
blocks on two messages, with only the packages the image installed.

    docker run --rm -v "$PWD/scripts/ci:/check:ro" --entrypoint python3 IMAGE \
        /check/stream_worker_check.py
"""
import json
import sys

sys.path.insert(0, '/srv/mage-service/python')

from mage_ai.pipeline_services.runtime import worker  # noqa: E402

manifest = json.load(open('/srv/mage-service/service.json'))
pipeline = next(p for p in manifest['pipelines'] if p.get('stream'))
project = manifest['service']['project']
chain = [dict(b, file=f"/srv/mage-service/{project}/{b['file']}") for b in pipeline['blocks']]
result = worker.run_stream_batch(dict(
    chain=chain, messages=[{'amount': 3}, {'amount': None}], outputs=[chain[-1]['uuid']],
))
assert result['messages'] == [{'amount': 3}], result
print(f"The stream blocks ran in the image: {result['blocks']}")
