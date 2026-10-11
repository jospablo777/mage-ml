"""The Markdown export of the docs (scripts/docs/export_markdown.py), which GitHub shows."""
import importlib.util
import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'export_markdown', ROOT / 'scripts' / 'docs' / 'export_markdown.py',
)
export_markdown = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(export_markdown)


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.docs = self.root / 'docs'
        self.out = self.root / 'docs-md'
        (self.docs / 'guides').mkdir(parents=True)
        (self.docs / 'images').mkdir()
        (self.docs / 'images' / 'a.light.svg').write_text('<svg/>')
        (self.docs / 'images' / 'a.dark.svg').write_text('<svg/>')
        (self.docs / 'docs.json').write_text(json.dumps({
            'navigation': {'tabs': [{'tab': 'Guides', 'groups': [
                {'group': 'Start', 'pages': ['guides/page', 'guides/other']},
            ]}]},
            'redirects': [{'source': '/old', 'destination': '/guides/other'}],
        }))
        (self.docs / 'guides' / 'other.mdx').write_text('---\ntitle: "Other"\n---\nText.\n')

    def page(self, body: str) -> str:
        (self.docs / 'guides' / 'page.mdx').write_text(
            '---\ntitle: "Page"\ndescription: "About [it](/guides/other)."\n---\n\n'
            + textwrap.dedent(body)
        )
        problems = export_markdown.export(self.docs, self.out)
        self.assertEqual(problems, [])
        return (self.out / 'guides' / 'page.md').read_text()

    def test_title_description_and_links(self):
        text = self.page('See [other](/guides/other#part) and [old](/old).\n')
        self.assertTrue(text.startswith('# Page\n\n*About [it](other.md).*'))
        self.assertIn('[other](other.md#part)', text)
        self.assertIn('[old](other.md)', text)

    def test_callouts_become_github_alerts(self):
        text = self.page("""
            <Note>
              One line.

              Two lines.
            </Note>
            <Warning>Careful.</Warning>
        """)
        self.assertIn('> [!NOTE]\n> One line.\n>\n> Two lines.', text)
        self.assertIn('> [!WARNING]\n> Careful.', text)

    def test_light_and_dark_images_become_a_picture(self):
        text = self.page("""
            <Frame caption="A diagram">
              <img className="block dark:hidden" src="/images/a.light.svg" alt="A" />
              <img className="hidden dark:block" src="/images/a.dark.svg" alt="A" />
            </Frame>
        """)
        self.assertIn(
            '<picture>\n'
            '  <source media="(prefers-color-scheme: dark)" '
            'srcset="../../docs/images/a.dark.svg">\n'
            '  <img src="../../docs/images/a.light.svg" alt="A" />\n'
            '</picture>',
            text,
        )
        self.assertIn('*A diagram*', text)

    def test_components_are_dedented_and_code_is_kept(self):
        text = self.page("""
            <Tabs>
              <Tab title="macOS">
                Install it:

                ```rust
                fn transform(data: LazyFrame) -> Result<LazyFrame> { Ok(data) }
                ```
              </Tab>
            </Tabs>
            <AccordionGroup>
              <Accordion title="More">
                Hidden text.
              </Accordion>
            </AccordionGroup>
            <Steps>
              <Step title="First">One.</Step>
              <Step title="Second">Two.</Step>
            </Steps>
        """)
        self.assertIn('**macOS**\n\nInstall it:\n\n```rust\nfn transform(data: LazyFrame)', text)
        self.assertIn('<details><summary>More</summary>\n\nHidden text.\n\n</details>', text)
        self.assertIn('**1. First**', text)
        self.assertIn('**2. Second**', text)
        self.assertNotIn('<Tab', text)

    def test_the_index_follows_the_navigation(self):
        self.page('Text.\n')
        index = (self.out / 'README.md').read_text()
        self.assertIn('## Guides', index)
        self.assertIn('- [Page](guides/page.md)\n- [Other](guides/other.md)', index)


class CommittedExportTest(unittest.TestCase):
    def test_docs_md_matches_docs(self):
        """docs-md/ is regenerated with the docs: run scripts/docs/export_markdown.py."""
        out = Path(tempfile.mkdtemp()) / 'docs-md'
        self.addCleanup(shutil.rmtree, out.parent, True)
        problems = export_markdown.export(export_markdown.DOCS, out)
        self.assertEqual(problems, [])
        committed = export_markdown.OUT
        expected = {p.relative_to(out): p.read_text() for p in out.rglob('*.md')}
        actual = {p.relative_to(committed): p.read_text() for p in committed.rglob('*.md')}
        self.assertEqual(sorted(actual), sorted(expected))
        stale = [str(p) for p in expected if actual.get(p) != expected[p]]
        self.assertEqual(stale, [])

    @unittest.skipUnless(
        shutil.which('git') and (ROOT / '.git').exists(), 'needs a git checkout',
    )
    def test_git_tracks_every_exported_page(self):
        """
        .gitignore had scratch* for scratch files, which also ignored the exported
        docs-md/design/blocks/scratchpad.md: it was never committed, and the test above
        failed on every fresh clone.
        """
        out = Path(tempfile.mkdtemp()) / 'docs-md'
        self.addCleanup(shutil.rmtree, out.parent, True)
        export_markdown.export(export_markdown.DOCS, out)
        pages = [str(Path('docs-md', p.relative_to(out))) for p in out.rglob('*.md')]
        ignored = subprocess.run(
            ['git', 'check-ignore', '--no-index', '--stdin'],
            cwd=ROOT, input='\n'.join(pages), capture_output=True, text=True,
        )
        self.assertEqual(ignored.stdout.split(), [])
