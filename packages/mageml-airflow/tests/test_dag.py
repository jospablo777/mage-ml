"""A DAG with the operator, run by Airflow itself (airflow dags test): the task runs the
pipeline, its log has the run's lines, and the next task reads the block's output from XCom."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

DAG_FILE = '''
from airflow.sdk import DAG, task

from mageml_airflow.operators import MageMLServiceRunOperator

with DAG('mageml_dag_test', schedule=None):
    total = MageMLServiceRunOperator(
        task_id='sum', pipeline='sum', variables={'n': 6}, output_block='total',
        poll_interval=0.2,
    )

    @task
    def check(value):
        assert value == {'total': 15}, value
        print('the output arrived', value)

    check(total.output)
'''


def test_a_dag_runs_the_pipeline_and_passes_its_output(service):
    home = Path(tempfile.mkdtemp(prefix='airflow-home-'))
    (home / 'dags').mkdir()
    (home / 'dags' / 'mageml_dag_test.py').write_text(DAG_FILE)
    env = dict(
        os.environ,
        AIRFLOW_HOME=str(home),
        AIRFLOW__CORE__DAGS_FOLDER=str(home / 'dags'),
        AIRFLOW__CORE__LOAD_EXAMPLES='False',
    )
    airflow = [sys.executable, '-m', 'airflow']
    migrated = subprocess.run([*airflow, 'db', 'migrate'], capture_output=True, text=True,
                              env=env)
    assert migrated.returncode == 0, migrated.stdout[-2000:] + migrated.stderr[-2000:]
    ran = subprocess.run([*airflow, 'dags', 'test', 'mageml_dag_test'], capture_output=True,
                         text=True, env=env, timeout=600)
    printed = ran.stdout + ran.stderr
    assert ran.returncode == 0, printed[-4000:]
    assert '[load] loading 6' in printed, printed[-4000:]
    assert "the output arrived {'total': 15}" in printed, printed[-4000:]
