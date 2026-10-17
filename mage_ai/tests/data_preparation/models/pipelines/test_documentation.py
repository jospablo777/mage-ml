"""
A pipeline's README: read and saved beside the pipeline's code, images served only from
the pipeline folder, Quarto rendered without running code.
"""
import os
import shutil
import stat
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.data_preparation.models.pipelines import documentation
from mage_ai.tests.base_test import DBTestCase

PNG = bytes.fromhex(
    '89504e470d0a1a0a0000000d4948445200000001000000010806000000'
    '1f15c4890000000d4944415478da63f8ffff3f0005fe02fea7d6a4a60000000049454e44ae426082'
)


class DocumentationTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = Pipeline.create(self._testMethodName, repo_path=self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def test_a_pipeline_without_documentation_gets_a_template_of_its_blocks(self):
        loader = Block.create(f'{self._testMethodName}_orders', 'data_loader', self.repo_path)
        exporter = Block.create(f'{self._testMethodName}_save', 'data_exporter', self.repo_path)
        self.pipeline.add_block(loader)
        self.pipeline.add_block(exporter, upstream_block_uuids=[loader.uuid])
        pipeline = Pipeline.get(self.pipeline.uuid, repo_path=self.repo_path)

        doc = documentation.read(pipeline)
        self.assertEqual((doc.format, doc.source, doc.path), (None, '', None))
        self.assertFalse(documentation.exists(pipeline))
        template = documentation.template(pipeline)
        self.assertIn(f'# {self._testMethodName}', template)
        self.assertIn(f'- `{loader.uuid}`: ', template.split('## Outputs')[0])
        self.assertIn(f'- `{exporter.uuid}`: ', template.split('## Outputs')[1])

    def test_documentation_is_saved_beside_the_pipeline_and_switches_format(self):
        doc = documentation.save(self.pipeline, '# Sales\n\nDaily sales.', 'markdown')
        folder = Path(self.pipeline.dir_path)
        self.assertEqual((doc.format, doc.path), ('markdown', 'README.md'))
        self.assertEqual((folder / 'README.md').read_text(), '# Sales\n\nDaily sales.')
        self.assertTrue(documentation.exists(self.pipeline))

        doc = documentation.save(self.pipeline, '---\ntitle: Sales\n---\n', 'quarto')
        self.assertEqual(doc.format, 'quarto')
        self.assertTrue((folder / 'README.qmd').is_file())
        self.assertFalse((folder / 'README.md').exists(), 'one README at a time')

        with self.assertRaisesRegex(documentation.DocumentationError, 'format must be'):
            documentation.save(self.pipeline, 'x', 'pdf')
        with self.assertRaisesRegex(documentation.DocumentationError, 'larger than'):
            documentation.save(self.pipeline, 'x' * (documentation.MAX_SOURCE_BYTES + 1))

    def test_an_uploaded_html_file_is_kept_as_it_is(self):
        html = '<!doctype html><h1>Sales</h1><img src="data:image/png;base64,AAAA">'
        doc = documentation.save(self.pipeline, html, 'html')
        self.assertEqual((doc.format, doc.path, doc.source), ('html', 'README.html', ''))
        self.assertEqual(documentation.html_document(self.pipeline), html)
        # Larger than Markdown may be, since such files embed their images.
        documentation.save(self.pipeline, 'x' * (documentation.MAX_SOURCE_BYTES + 1), 'html')
        documentation.save(self.pipeline, '# Sales\n', 'markdown')
        self.assertFalse((Path(self.pipeline.dir_path) / 'README.html').exists())
        with self.assertRaisesRegex(documentation.DocumentationError, 'no README.html'):
            documentation.html_document(self.pipeline)

    def test_only_images_inside_the_pipeline_folder_are_served(self):
        folder = Path(self.pipeline.dir_path)
        (folder / 'images').mkdir()
        (folder / 'images' / 'flow.png').write_bytes(PNG)
        self.assertEqual(documentation.image(self.pipeline, 'images/flow.png'),
                         (folder / 'images' / 'flow.png').resolve())

        outside = Path(self.repo_path) / 'secret.png'
        outside.write_bytes(PNG)
        self.addCleanup(outside.unlink)
        for path in ('../../secret.png', '/etc/passwd', 'metadata.yaml', 'missing.png', ''):
            with self.subTest(path=path):
                with self.assertRaises(documentation.DocumentationError):
                    documentation.image(self.pipeline, path)
        # A link inside the folder that points outside it is outside it.
        os.symlink(outside, folder / 'linked.png')
        with self.assertRaisesRegex(documentation.DocumentationError, 'inside the pipeline'):
            documentation.image(self.pipeline, 'linked.png')

    def test_quarto_renders_without_running_code_and_is_cached(self):
        documentation.save(self.pipeline, '---\ntitle: Sales\n---\n\n![Flow](flow.png)\n', 'quarto')
        (Path(self.pipeline.dir_path) / 'flow.png').write_bytes(PNG)
        fake = Path(self.repo_path) / 'fake-quarto'
        log = Path(self.repo_path) / 'quarto-calls.txt'
        fake.write_text(textwrap.dedent(f'''\
            #!/bin/sh
            if [ "$1" = "--version" ]; then echo 1.9.0; exit 0; fi
            echo "$@" >> "{log}"
            test -f flow.png || exit 3
            echo "<html><body><h1>Sales</h1></body></html>" > index.html
        '''))
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        cache = Path(self.repo_path) / 'doc-cache'
        with patch.dict(os.environ, {'MAGE_QUARTO': str(fake)}):
            html = documentation.render_quarto(self.pipeline, str(cache))
            again = documentation.render_quarto(self.pipeline, str(cache))
        self.assertIn('<h1>Sales</h1>', html)
        self.assertEqual(html, again)
        calls = log.read_text().splitlines()
        self.assertEqual(len(calls), 1, 'the second render comes from the cache')
        self.assertIn('--no-execute', calls[0])
        self.assertIn('embed-resources:true', calls[0])

    def fake_quarto(self, body: str) -> Path:
        fake = Path(self.repo_path) / f'fake-quarto-{self._testMethodName}'
        fake.write_text('#!/bin/sh\n' + textwrap.dedent(body))
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
        return fake

    def test_a_render_gets_the_folders_includes_and_metadata(self):
        # Only images were copied: includes, styles and _quarto.yml were missing (I-117).
        documentation.save(self.pipeline, '{{< include _part.qmd >}}\n', 'quarto')
        folder = Path(self.pipeline.dir_path)
        (folder / '_part.qmd').write_text('Part')
        (folder / '_quarto.yml').write_text('format: html\n')
        (folder / '.hidden').write_text('no')
        fake = self.fake_quarto('''
            if [ "$1" = "--version" ]; then echo 1.9.0; exit 0; fi
            test -f _part.qmd && test -f _quarto.yml && test ! -f .hidden || exit 3
            echo "<html>ok</html>" > index.html
        ''')
        with patch.dict(os.environ, {'MAGE_QUARTO': str(fake)}):
            html = documentation.render_quarto(self.pipeline, str(Path(self.repo_path) / 'c1'))
        self.assertIn('ok', html)

    def test_a_new_quarto_version_renders_again(self):
        documentation.save(self.pipeline, '# Sales', 'quarto')
        log = Path(self.repo_path) / 'renders.txt'
        cache = str(Path(self.repo_path) / 'c2')
        for version in ('1.8.0', '1.9.0'):
            # The upgrade replaces the file: a later time.
            time.sleep(0.01)
            fake = self.fake_quarto(f'''
                if [ "$1" = "--version" ]; then echo {version}; exit 0; fi
                echo render >> "{log}"
                echo "<html>ok</html>" > index.html
            ''')
            with patch.dict(os.environ, {'MAGE_QUARTO': str(fake)}):
                documentation.render_quarto(self.pipeline, cache)
                documentation.render_quarto(self.pipeline, cache)
        self.assertEqual(len(log.read_text().splitlines()), 2)

    def test_an_unwritable_quarto_cache_gets_a_private_home(self):
        documentation.save(self.pipeline, '# Sales', 'quarto')
        fake = self.fake_quarto('''
            if [ "$1" = "--version" ]; then echo 1.9.0; exit 0; fi
            case "$HOME" in
              *quarto-home*) echo "<html>ok</html>" > index.html ;;
              *) echo "ERROR: Permission denied (os error 13): sass cache database" >&2; exit 1 ;;
            esac
        ''')
        with patch.dict(os.environ, {'MAGE_QUARTO': str(fake)}):
            html = documentation.render_quarto(self.pipeline, str(Path(self.repo_path) / 'c3'))
        self.assertIn('ok', html)

    def test_an_error_names_the_pipelines_files(self):
        documentation.save(self.pipeline, '# Sales', 'quarto')
        fake = self.fake_quarto('''
            if [ "$1" = "--version" ]; then echo 1.9.0; exit 0; fi
            echo "ERROR: index.qmd: include $PWD/_missing.qmd not found" >&2; exit 1
        ''')
        with patch.dict(os.environ, {'MAGE_QUARTO': str(fake)}):
            with self.assertRaises(documentation.DocumentationError) as caught:
                documentation.render_quarto(self.pipeline, str(Path(self.repo_path) / 'c4'))
        message = str(caught.exception)
        self.assertIn('README.qmd', message)
        self.assertIn(str(Path(self.pipeline.dir_path).resolve() / '_missing.qmd'), message)

    def test_a_hidden_second_readme_is_reported(self):
        folder = Path(self.pipeline.dir_path)
        (folder / 'README.md').write_text('# Shown')
        (folder / 'README.qmd').write_text('# Hidden')
        doc = documentation.read(self.pipeline)
        self.assertEqual(doc.path, 'README.md')
        self.assertEqual(doc.to_dict()['ignored'], ['README.qmd'])

    def test_without_quarto_the_error_says_how_to_get_it(self):
        documentation.save(self.pipeline, '# Sales', 'quarto')
        with patch.object(documentation, 'quarto_command', return_value=None):
            with self.assertRaisesRegex(documentation.DocumentationError, 'not installed'):
                documentation.render_quarto(self.pipeline, self.repo_path)

    @unittest.skipUnless(shutil.which('quarto'), 'needs Quarto')
    def test_a_real_quarto_document_renders_with_its_image_embedded_and_code_shown(self):
        documentation.save(self.pipeline, textwrap.dedent('''\
            ---
            title: Sales
            ---

            ## Flow

            ![Flow](flow.png)

            ```{python}
            raise SystemExit("this cell must not run")
            ```
        '''), 'quarto')
        (Path(self.pipeline.dir_path) / 'flow.png').write_bytes(PNG)
        html = documentation.render_quarto(self.pipeline, str(Path(self.repo_path) / 'cache'))
        self.assertIn('Sales', html)
        self.assertIn('data:image/png;base64', html, 'the image is embedded')
        self.assertIn('this cell must not run', html, 'the code is shown')
