import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from watchdog.observers import Observer

from mage_ai.server.file_observer import MetadataEventHandler


class MetadataEventHandlerTest(unittest.TestCase):
    # FSEvents reports no events for a watch on a single file with watchdog 4 or 6, only for a
    # watch on a directory. Linux uses inotify, which reports them.
    @unittest.skipIf(sys.platform == 'darwin', 'FSEvents ignores watches on a single file')
    def test_editing_the_watched_file_reloads_settings(self):
        # The server schedules the observer on metadata.yaml itself, not on its directory.
        reloaded = threading.Event()
        with tempfile.TemporaryDirectory() as directory, patch(
            'mage_ai.server.file_observer.update_settings_on_metadata_change',
            side_effect=lambda: reloaded.set(),
        ):
            metadata_file = os.path.join(directory, 'metadata.yaml')
            with open(metadata_file, 'w') as f:
                f.write('project_uuid: before\n')

            observer = Observer()
            observer.schedule(MetadataEventHandler(), path=metadata_file)
            observer.start()
            try:
                time.sleep(1)
                with open(metadata_file, 'w') as f:
                    f.write('project_uuid: after\n')

                self.assertTrue(reloaded.wait(timeout=10))
            finally:
                observer.stop()
                observer.join(timeout=10)


if __name__ == '__main__':
    unittest.main()
