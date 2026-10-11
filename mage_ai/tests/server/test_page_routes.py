"""
Every page of the app has a server route: the built app is static, and the server renders
index.html for the paths it knows and its 404 page for the others. /services shipped once
without a route.
"""
import re
import unittest
from pathlib import Path

from mage_ai.server import server

PAGES = Path(server.__file__).resolve().parents[1] / 'frontend' / 'pages'
# Not pages people open by path: Next's own files, the root, and an unlinked experiment.
NOT_ROUTED = {'_app', '_document', '404', 'index', 'block-layout'}


def top_level_paths():
    """/name for each page; /folder/page for a folder of pages without its own index."""
    paths = {f'/{p.stem}' for p in PAGES.glob('*.tsx') if p.stem not in NOT_ROUTED}
    for folder in PAGES.iterdir():
        if not folder.is_dir():
            continue
        if (folder / 'index.tsx').is_file():
            paths.add(f'/{folder.name}')
        else:
            paths.update(f'/{folder.name}/{p.stem}' for p in folder.glob('*.tsx'))
    # Dynamic segments such as [uuid] or [...slug] take a sample value.
    return sorted(re.sub(r'\[[^\]]+\]', 'sample', p) for p in paths)


class PageRoutesTest(unittest.TestCase):
    def test_every_top_level_page_has_a_route(self):
        source = Path(server.__file__).read_text()
        patterns = re.findall(r"\(r'(/[^']*)',\s*\w*PageHandler\)", source)
        compiled = [re.compile(f'^{p}$') for p in patterns]

        missing = [
            path for path in top_level_paths()
            if not any(c.match(path) for c in compiled)
        ]

        self.assertEqual(missing, [], f'Pages without a route in server.py: {missing}')
