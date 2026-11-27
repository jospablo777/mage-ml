"""
The two committed frontend builds, frontend_dist (yarn export_prod) and the base path
template (yarn export_prod_base_path), are built from the current frontend sources. Each
records the digest of the sources it was built from (scripts/export.js); a change rebuilt
into one build only, or not rebuilt at all, shipped old pages (I-455).
"""
import hashlib
import re
import subprocess
import unittest
from pathlib import Path

SERVER = Path(__file__).resolve().parents[2] / 'server'
FRONTEND = SERVER.parent / 'frontend'
# As NOT_BUILT in mage_ai/frontend/scripts/export.js.
NOT_BUILT = [r'^tests/', r'\.md$', r'^playwright[^/]*$', r'^\.storybook/', r'\.stories\.tsx?$']


def source_digest() -> str:
    """sha256 over "path\\0sha256(content)\\n" for each source file, sorted by path."""
    listed = subprocess.run(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', '.'],
        cwd=FRONTEND, capture_output=True, check=True,
    ).stdout.decode().split('\0')
    files = sorted(
        name for name in listed
        if name and not any(re.search(p, name) for p in NOT_BUILT)
        and (FRONTEND / name).is_file()
    )
    digest = hashlib.sha256()
    for name in files:
        content = hashlib.sha256((FRONTEND / name).read_bytes()).hexdigest()
        digest.update(f'{name}\0{content}\n'.encode())
    return digest.hexdigest()


class FrontendBuildsTest(unittest.TestCase):
    def test_both_builds_come_from_the_current_sources(self):
        current = source_digest()
        for build in ('frontend_dist', 'frontend_dist_base_path_template'):
            recorded = (SERVER / build / 'source-digest.txt').read_text().strip()
            self.assertEqual(
                recorded, current,
                f'{build} was built from other sources: cd mage_ai/frontend && '
                'yarn export_prod && yarn export_prod_base_path',
            )


class PageRoutesTest(unittest.TestCase):
    """
    Every page of the build has a route in the server: a page without one sent people to
    the pipelines list (the Incidents page, before its route).
    """

    def test_every_top_level_page_is_served(self):
        text = (SERVER / 'server.py').read_text()
        routes = set(re.findall(r"\(r'(/[^'(]*?)(?:/\(\.\*\))?', \w*PageHandler\)", text))
        pages = sorted(
            f'/{page.stem}' for page in (SERVER / 'frontend_dist').glob('*.html')
            if page.stem not in ('index', '404') and '[' not in page.stem
        )
        self.assertTrue(pages)
        missing = [page for page in pages if page not in routes]
        self.assertEqual(missing, [], 'add them to make_app as MainPageHandler routes')
