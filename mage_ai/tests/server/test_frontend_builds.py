"""
The two committed frontend builds come from the same source: frontend_dist (yarn
export_prod) and the base path template (yarn export_prod_base_path). Their page chunks are
named by their content, the same in both; a change rebuilt into one build only shipped the
old pages to servers with MAGE_BASE_PATH (I-455).
"""
import unittest
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / 'server'


def page_chunks(build: str):
    pages = SERVER / build / '_next' / 'static' / 'chunks' / 'pages'
    return sorted(str(path.relative_to(pages)) for path in pages.rglob('*.js'))


class FrontendBuildsTest(unittest.TestCase):
    def test_both_builds_have_the_same_pages(self):
        built = page_chunks('frontend_dist')
        self.assertTrue(built)
        self.assertEqual(
            built, page_chunks('frontend_dist_base_path_template'),
            'Rebuild both: cd mage_ai/frontend && yarn export_prod && yarn export_prod_base_path',
        )
