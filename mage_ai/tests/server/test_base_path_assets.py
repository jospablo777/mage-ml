import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.server.server import replace_base_path


class BasePathAssetsTest(unittest.TestCase):
    def test_fonts_are_prefixed_in_minified_and_formatted_css(self):
        with tempfile.TemporaryDirectory(prefix='mage-base-path-') as directory:
            root = Path(directory)
            source = root / 'source'
            source.mkdir()
            css = '''
                a{src:url(/fonts/regular.ttf)}
                b{src: url('/fonts/bold.ttf')}
                c{src:url( "/fonts/italic.ttf")}
                d{src:url(https://example.com/fonts/external.ttf)}
            '''
            (source / 'styles.css').write_text(css)
            with patch('mage_ai.server.server.get_variables_dir', return_value=str(root)), \
                    patch('mage_ai.server.server.BASE_PATH_TEMPLATE_EXPORTS_FOLDER', str(source)):
                destination = Path(replace_base_path('office'))
            result = (destination / 'styles.css').read_text()
            for name in ('regular', 'bold', 'italic'):
                self.assertIn(f'/office/fonts/{name}.ttf', result)
            self.assertIn('https://example.com/fonts/external.ttf', result)
