import os
import tempfile
from types import SimpleNamespace

from mage_ai.data_preparation.models.block.dynamic import (
    all_variable_uuids,
    reduce_output_from_block,
)
from mage_ai.tests.base_test import TestCase


class ReduceOrderTest(TestCase):
    def block(self, children: int):
        directory = tempfile.mkdtemp()
        # Outputs are stored as <parent index>/<child index>/<output>. They are created in
        # reverse so the order on disk differs from the child order.
        for index in reversed(range(children)):
            for output in ('output_1', 'output_0'):
                os.makedirs(os.path.join(directory, '0', str(index), output))
        pipeline = SimpleNamespace(
            get_block_variable=lambda block_uuid, variable_uuid, **kwargs: block_uuid,
        )
        return SimpleNamespace(
            uuid='child',
            pipeline=pipeline,
            get_variable_object=lambda **kwargs: SimpleNamespace(variable_dir_path=directory),
        )

    def test_reduced_outputs_follow_the_child_order(self):
        """os.walk listed the children in file system order, such as 0, 11, 1, 10."""
        outputs = reduce_output_from_block(self.block(12), 'output_0')

        self.assertEqual(outputs, [f'child:0:{index}' for index in range(12)])

    def test_variable_uuids_keep_their_order(self):
        self.assertEqual(all_variable_uuids(self.block(3)), ['output_0', 'output_1'])
