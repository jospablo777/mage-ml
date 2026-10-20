import threading

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.repo_manager import get_repo_config
from mage_ai.shared import read_cache
from mage_ai.tests.base_test import DBTestCase


class ReadCacheTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def get(self):
        return Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

    def test_a_pass_reads_a_pipeline_and_the_config_once(self):
        with read_cache.one_pass():
            self.assertIs(self.get(), self.get())
            self.assertIs(get_repo_config(self.repo_path), get_repo_config(self.repo_path))

    def test_outside_a_pass_every_read_goes_to_disk(self):
        self.assertIsNot(self.get(), self.get())
        with read_cache.one_pass():
            first = self.get()
        with read_cache.one_pass():
            self.assertIsNot(self.get(), first)

    def test_a_thread_started_in_a_pass_reads_for_itself(self):
        found = []
        with read_cache.one_pass():
            mine = self.get()
            thread = threading.Thread(target=lambda: found.append(self.get()))
            thread.start()
            thread.join()
        self.assertIsNot(found[0], mine)

    def test_reads_with_context_data_are_not_kept(self):
        with read_cache.one_pass():
            self.assertIsNot(
                Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path, context_data={}),
                Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path, context_data={}),
            )
