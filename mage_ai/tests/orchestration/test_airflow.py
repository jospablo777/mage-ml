"""
The inherited Airflow helper (orchestration/airflow.py, I-26): a DAG per pipeline, one task per
block, with the schedule under the name the installed Airflow takes.
"""
from datetime import datetime

from mage_ai.orchestration.airflow import create_dags
from mage_ai.tests.base_test import TestCase
from mage_ai.tests.factory import create_pipeline_with_blocks


class Airflow3Dag:
    """Airflow 3's DAG signature: `schedule` only (airflow.sdk.DAG in 3.3)."""

    def __init__(self, dag_id, description=None, start_date=None, schedule=None,
                 catchup=None, tags=None):
        self.dag_id = dag_id
        self.settings = dict(description=description, start_date=start_date,
                             schedule=schedule, catchup=catchup, tags=tags)
        self.dependencies = []

    def set_dependency(self, upstream, downstream):
        self.dependencies.append((upstream, downstream))


class Airflow2Dag(Airflow3Dag):
    """Airflow before 2.4: `schedule_interval` only."""

    def __init__(self, dag_id, description=None, start_date=None, schedule_interval=None,
                 catchup=None):
        super().__init__(dag_id, description, start_date, schedule_interval, catchup)
        self.settings['schedule_interval'] = self.settings.pop('schedule')


class PythonOperator:
    def __init__(self, dag, task_id, python_callable):
        self.dag = dag
        self.task_id = task_id
        self.python_callable = python_callable


class AirflowHelperTest(TestCase):
    def dags(self, dag_class, **settings):
        create_pipeline_with_blocks(f'airflow_{dag_class.__name__.lower()}', self.repo_path)
        found = {}
        create_dags(self.repo_path, dag_class, PythonOperator, globals_dict=found, **settings)
        return found[f'mage_pipeline_airflow_{dag_class.__name__.lower()}']

    def test_airflow_3_gets_schedule(self):
        dag = self.dags(Airflow3Dag)
        self.assertEqual(dag.settings['schedule'], '@once')
        self.assertFalse(dag.settings['catchup'])
        self.assertIn(('block1', 'block2'), dag.dependencies)
        self.assertEqual(dag.settings['start_date'], datetime(2022, 7, 14))

    def test_a_schedule_interval_setting_is_passed_as_schedule_to_airflow_3(self):
        dag = self.dags(Airflow3Dag, dag_settings=dict(schedule_interval='@daily', tags=['a']))
        self.assertEqual(dag.settings['schedule'], '@daily')
        self.assertEqual(dag.settings['tags'], ['a'])

    def test_older_airflow_gets_schedule_interval(self):
        dag = self.dags(Airflow2Dag, dag_settings=dict(schedule='@hourly'))
        self.assertEqual(dag.settings['schedule_interval'], '@hourly')
