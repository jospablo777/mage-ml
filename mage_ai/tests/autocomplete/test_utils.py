"""
Autocomplete reads classes from the source: building it imported every io module on the
server's event loop, with the SDKs they import (one pulled in torch), and held every other
request for seconds while the editor opened.
"""
import json
import subprocess
import sys
import textwrap

from mage_ai.tests.base_test import TestCase

SCRIPT = textwrap.dedent('''
    import asyncio, json, sys, time
    from mage_ai.autocomplete.utils import (
        FILES_TO_READ, PATHS_TO_TRAVERSE, build_file_content_mapping,
    )
    before = set(sys.modules)
    started = time.monotonic()
    mapping = asyncio.run(build_file_content_mapping(PATHS_TO_TRAVERSE, FILES_TO_READ))
    seconds = time.monotonic() - started
    imported = sorted(m for m in set(sys.modules) - before
                      if m.startswith(('mage_ai.io.', 'transformers', 'torch', 'qdrant')))
    redshift = next(v for k, v in mapping.items() if k.endswith('mage_ai/io/redshift.py'))
    print(json.dumps(dict(seconds=seconds, imported=imported, files=len(mapping),
                          redshift=redshift['methods_for_class'].get('Redshift', []))))
''')


class AutocompleteTest(TestCase):
    def test_the_library_is_read_without_importing_it(self):
        result = subprocess.run(
            [sys.executable, '-c', SCRIPT], capture_output=True, text=True, check=True,
        )
        out = json.loads(result.stdout.strip().splitlines()[-1])
        self.assertEqual(out['imported'], [])
        self.assertGreater(out['files'], 30)
        self.assertLess(out['seconds'], 5)
        # Its own methods and those of the classes it inherits from.
        self.assertIn('load', out['redshift'])
        self.assertIn('export', out['redshift'])
        self.assertIn('with_config', out['redshift'])
