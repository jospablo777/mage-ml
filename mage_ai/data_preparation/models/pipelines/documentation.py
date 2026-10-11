"""
A pipeline's documentation: README.md (Markdown) or README.qmd (Quarto) in the pipeline's
folder, with the images it shows beside it, so documentation versions with the pipeline.

Markdown renders in the browser. Quarto renders with the quarto command when it is
installed, without running code cells, into one HTML file with its images embedded; without
Quarto, a .qmd reads as Markdown. Images are served only from inside the pipeline's folder
and only as image files.
"""
import hashlib
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

FILES = {'markdown': 'README.md', 'quarto': 'README.qmd'}
IMAGE_TYPES = {
    '.gif': 'image/gif',
    '.jpeg': 'image/jpeg',
    '.jpg': 'image/jpeg',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
    '.webp': 'image/webp',
}
MAX_SOURCE_BYTES = 1024 * 1024
MAX_IMAGE_BYTES = 10 * 1024 * 1024
QUARTO_TIMEOUT_SECONDS = 120


class DocumentationError(Exception):
    """A documentation request that cannot be served, with the reason for the user."""


@dataclass
class Documentation:
    format: Optional[str]  # markdown, quarto or None when the pipeline has none
    source: str
    path: Optional[str]
    updated_at: Optional[float]

    def to_dict(self) -> Dict:
        return dict(format=self.format, path=self.path, source=self.source,
                    updated_at=self.updated_at)


def _folder(pipeline) -> Path:
    return Path(pipeline.dir_path).resolve()


def read(pipeline) -> Documentation:
    """The pipeline's documentation; the Markdown file when both exist."""
    for fmt, name in FILES.items():
        path = _folder(pipeline) / name
        if path.is_file():
            if path.stat().st_size > MAX_SOURCE_BYTES:
                raise DocumentationError(
                    f'{name} is larger than {MAX_SOURCE_BYTES // 1024} KB; keep large content '
                    'in separate files and link them.',
                )
            return Documentation(fmt, path.read_text(encoding='utf-8', errors='replace'),
                                 name, path.stat().st_mtime)
    return Documentation(None, '', None, None)


def exists(pipeline) -> bool:
    folder = _folder(pipeline)
    return any((folder / name).is_file() for name in FILES.values())


def save(pipeline, source: str, fmt: str = 'markdown') -> Documentation:
    """Writes the documentation in the format given; switching formats renames the file."""
    if fmt not in FILES:
        raise DocumentationError(f'format must be one of {sorted(FILES)}, not {fmt!r}.')
    data = source.encode('utf-8')
    if len(data) > MAX_SOURCE_BYTES:
        raise DocumentationError(
            f'The documentation is larger than {MAX_SOURCE_BYTES // 1024} KB.',
        )
    folder = _folder(pipeline)
    target = folder / FILES[fmt]
    tmp = target.with_name(f'.{target.name}.tmp')
    tmp.write_bytes(data)
    os.replace(tmp, target)
    for other, name in FILES.items():
        if other != fmt and (folder / name).is_file():
            (folder / name).unlink()
    return read(pipeline)


def template(pipeline) -> str:
    """A first README naming the pipeline's loaders and exporters, to fill in."""
    blocks = list((getattr(pipeline, 'blocks_by_uuid', None) or {}).values())

    def names(block_type: str) -> str:
        found = [b.uuid for b in blocks if str(getattr(b.type, 'value', b.type)) == block_type]
        return '\n'.join(f'- `{uuid}`: ' for uuid in found) or '- '

    title = getattr(pipeline, 'name', None) or pipeline.uuid
    return f'''# {title}

What this pipeline does and why it exists.

## Inputs

{names('data_loader')}

## Outputs

{names('data_exporter')}

## Schedule

When it runs, and what triggers it.

## Owners

Who to ask, and who is on call.

## Operations

How to rerun it, what to check when it fails, and what depends on it.

<!-- Images: put them in this pipeline's folder and show them with ![Description](image.png). -->
'''


def image(pipeline, relative: str) -> Path:
    """The image file a document shows, refused unless it is an image inside the pipeline's
    folder."""
    folder = _folder(pipeline)
    if not relative or relative.startswith(('/', '\\')) or '\0' in relative:
        raise DocumentationError('Image paths are relative to the pipeline folder.')
    path = (folder / relative).resolve()
    if folder != path and folder not in path.parents:
        raise DocumentationError('Images must be inside the pipeline folder.')
    if path.suffix.lower() not in IMAGE_TYPES:
        raise DocumentationError(
            f'{Path(relative).name} is not an image ({", ".join(sorted(IMAGE_TYPES))}).',
        )
    if not path.is_file():
        raise DocumentationError(f'{relative} does not exist in the pipeline folder.')
    if path.stat().st_size > MAX_IMAGE_BYTES:
        raise DocumentationError(f'{relative} is larger than {MAX_IMAGE_BYTES // 1048576} MB.')
    return path


def quarto_command() -> Optional[str]:
    return os.getenv('MAGE_QUARTO') or shutil.which('quarto')


def render_quarto(pipeline, cache_dir: str) -> str:
    """
    README.qmd rendered to one HTML file with its images embedded and code cells shown,
    not run. Cached by the content of the document and the pipeline folder's images.
    """
    command = quarto_command()
    if not command:
        raise DocumentationError(
            'Quarto is not installed, so the document shows as Markdown. Install Quarto '
            '(quarto.org) or set MAGE_QUARTO to its path to render it.',
        )
    folder = _folder(pipeline)
    source = folder / FILES['quarto']
    if not source.is_file():
        raise DocumentationError('This pipeline has no README.qmd.')
    digest = hashlib.sha256(source.read_bytes())
    for path in sorted(folder.rglob('*')):
        if path.suffix.lower() in IMAGE_TYPES and path.is_file():
            digest.update(f'{path.relative_to(folder)}:{path.stat().st_mtime_ns}'.encode())
    cached = Path(cache_dir) / f'{digest.hexdigest()[:32]}.html'
    if cached.is_file():
        return cached.read_text(encoding='utf-8')

    with tempfile.TemporaryDirectory(prefix='mage-docs-') as tmp:
        work = Path(tmp)
        shutil.copy2(source, work / 'index.qmd')
        for path in folder.rglob('*'):
            if path.suffix.lower() in IMAGE_TYPES and path.is_file():
                target = work / path.relative_to(folder)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
        try:
            result = subprocess.run(
                [command, 'render', 'index.qmd', '--to', 'html', '--no-execute',
                 '-M', 'embed-resources:true'],
                cwd=work, capture_output=True, text=True, timeout=QUARTO_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            raise DocumentationError(
                f'Quarto took longer than {QUARTO_TIMEOUT_SECONDS} s to render.',
            )
        output = work / 'index.html'
        if result.returncode != 0 or not output.is_file():
            raise DocumentationError(
                'Quarto could not render the document:\n'
                + (result.stderr or result.stdout).strip()[-3000:],
            )
        html = output.read_text(encoding='utf-8')
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_text(html, encoding='utf-8')
    return html
