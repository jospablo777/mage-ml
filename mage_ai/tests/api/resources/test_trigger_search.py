from mage_ai.api.resources.PipelineScheduleResource import PipelineScheduleResource
from mage_ai.data_preparation.models.triggers import ScheduleType
from mage_ai.orchestration.db.models.schedules import PipelineSchedule
from mage_ai.tests.base_test import DBTestCase


class TriggerSearchTest(DBTestCase):
    def test_triggers_are_found_by_name_or_pipeline_ignoring_case(self):
        """The triggers list had no search (upstream #5697)."""
        for name, pipeline in (
            ('nightly_load', 'orders'), ('hourly', 'NightShift_report'), ('weekly', 'sales'),
        ):
            PipelineSchedule.create(
                name=f'{self._testMethodName}_{name}', pipeline_uuid=pipeline,
                schedule_type=ScheduleType.TIME, repo_path=self.repo_path,
            )

        def names(text):
            result = PipelineScheduleResource.collection({'search': [text]}, {}, None)
            prefix = f'{self._testMethodName}_'
            found = [r.model.name if hasattr(r.model, 'name') else r.model['name']
                     for r in result]
            return sorted(n.replace(prefix, '') for n in found if n.startswith(prefix))

        self.assertEqual(names('NIGHT'), ['hourly', 'nightly_load'])
        self.assertEqual(names('sales'), ['weekly'])
        self.assertEqual(len(names('')), 3)
