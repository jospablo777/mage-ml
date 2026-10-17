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
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

FILES = {'markdown': 'README.md', 'quarto': 'README.qmd', 'html': 'README.html'}
IMAGE_TYPES = {
    '.gif': 'image/gif',
    '.jpeg': 'image/jpeg',
    '.jpg': 'image/jpeg',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
    '.webp': 'image/webp',
}
MAX_SOURCE_BYTES = 1024 * 1024
# An uploaded HTML file often embeds its images, so it may be larger.
MAX_HTML_BYTES = 20 * 1024 * 1024
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
    # Other README files in the folder, which the one shown hides.
    ignored: Optional[list] = None

    def to_dict(self) -> Dict:
        return dict(format=self.format, path=self.path, source=self.source,
                    updated_at=self.updated_at, ignored=self.ignored or [])


def _folder(pipeline) -> Path:
    return Path(pipeline.dir_path).resolve()


def _limit(fmt: str) -> int:
    return MAX_HTML_BYTES if fmt == 'html' else MAX_SOURCE_BYTES


def read(pipeline) -> Documentation:
    """
    The pipeline's documentation; the Markdown file when several exist. An HTML file is
    shown as it is, through html_document, so its source is not returned.
    """
    for fmt, name in FILES.items():
        path = _folder(pipeline) / name
        if path.is_file():
            if path.stat().st_size > _limit(fmt):
                raise DocumentationError(
                    f'{name} is larger than {_limit(fmt) // 1024} KB; keep large content '
                    'in separate files and link them.',
                )
            source = '' if fmt == 'html' else path.read_text(encoding='utf-8', errors='replace')
            ignored = [
                other for other in FILES.values()
                if other != name and (_folder(pipeline) / other).is_file()
            ]
            return Documentation(fmt, source, name, path.stat().st_mtime, ignored)
    return Documentation(None, '', None, None)


def exists(pipeline) -> bool:
    folder = _folder(pipeline)
    return any((folder / name).is_file() for name in FILES.values())


def save(pipeline, source: str, fmt: str = 'markdown') -> Documentation:
    """Writes the documentation in the format given; switching formats renames the file."""
    if fmt not in FILES:
        raise DocumentationError(f'format must be one of {sorted(FILES)}, not {fmt!r}.')
    data = source.encode('utf-8')
    if len(data) > _limit(fmt):
        raise DocumentationError(
            f'The documentation is larger than {_limit(fmt) // 1024} KB.',
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


# The first bytes of each binary image type; SVG is text and checked as such.
_SIGNATURES = {
    '.gif': (b'GIF87a', b'GIF89a'),
    '.jpeg': (b'\xff\xd8\xff',),
    '.jpg': (b'\xff\xd8\xff',),
    '.png': (b'\x89PNG\r\n\x1a\n',),
    '.webp': (b'RIFF',),
}


def save_image(pipeline, name: str, content: bytes) -> str:
    """
    Saves an uploaded image in the pipeline folder's images/ directory and returns its
    path relative to the folder, as a document links it. A name already taken gets a
    number, so an upload never replaces another image.
    """
    stem = re.sub(r'[^\w.-]+', '-', Path(name or '').stem).strip('.-')[:80] or 'image'
    suffix = Path(name or '').suffix.lower()
    if suffix not in IMAGE_TYPES:
        raise DocumentationError(
            f'{Path(name or "").name or "The file"} is not an image '
            f'({", ".join(sorted(IMAGE_TYPES))}).',
        )
    if not content:
        raise DocumentationError('The image is empty.')
    if len(content) > MAX_IMAGE_BYTES:
        raise DocumentationError(f'Images can be at most {MAX_IMAGE_BYTES // 1048576} MB.')
    if suffix == '.svg':
        if b'<svg' not in content[:4096].lower():
            raise DocumentationError(f'{stem}{suffix} is not an SVG image.')
    elif not content.startswith(_SIGNATURES[suffix]):
        raise DocumentationError(f'{stem}{suffix} is not a {suffix[1:].upper()} image.')
    folder = _folder(pipeline) / 'images'
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f'{stem}{suffix}'
    number = 1
    while path.exists():
        number += 1
        path = folder / f'{stem}-{number}{suffix}'
    path.write_bytes(content)
    return f'images/{path.name}'


def html_document(pipeline) -> str:
    """The uploaded README.html, which the documentation page shows in a sandbox."""
    path = _folder(pipeline) / FILES['html']
    if not path.is_file():
        raise DocumentationError('The pipeline has no README.html.')
    if path.stat().st_size > MAX_HTML_BYTES:
        raise DocumentationError(f'README.html is larger than {MAX_HTML_BYTES // 1048576} MB.')
    return path.read_text(encoding='utf-8', errors='replace')


def quarto_command() -> Optional[str]:
    return os.getenv('MAGE_QUARTO') or shutil.which('quarto')


_quarto_versions: Dict[str, str] = {}
# Files of the pipeline folder a render may read: includes, styles, bibliographies,
# metadata (_quarto.yml) and images. Hidden files and caches are left out.
MAX_RENDER_FILES = 200
MAX_RENDER_BYTES = 50 * 1024 * 1024


def quarto_version(command: str) -> str:
    """The Quarto version, read once per command: it is part of the cache key, so an
    upgrade renders again."""
    try:
        # An upgrade in place replaces the file: its time is part of the key.
        key = f'{command}:{os.stat(shutil.which(command) or command).st_mtime_ns}'
    except OSError:
        key = command
    if key not in _quarto_versions:
        try:
            _quarto_versions[key] = subprocess.run(
                [command, '--version'], capture_output=True, text=True, timeout=30,
            ).stdout.strip() or 'unknown'
        except Exception:
            _quarto_versions[key] = 'unknown'
    return _quarto_versions[key]


def _render_files(folder: Path):
    """The folder's files a render gets: not hidden, not caches, within the limits."""
    files, total = [], 0
    for path in sorted(folder.rglob('*')):
        relative = path.relative_to(folder)
        if not path.is_file() or any(
            part.startswith(('.', '__pycache__')) for part in relative.parts
        ):
            continue
        size = path.stat().st_size
        if len(files) >= MAX_RENDER_FILES or total + size > MAX_RENDER_BYTES:
            break
        files.append(path)
        total += size
    return files


def _quarto_run(command: str, work: Path, home: Optional[Path] = None):
    env = None
    if home is not None:
        # Quarto keeps its Sass cache under the user's cache folder (on macOS
        # ~/Library/Caches/quarto, whatever XDG_CACHE_HOME says); when that is not
        # writable, a private home gives it one.
        home.mkdir(parents=True, exist_ok=True)
        env = dict(os.environ, HOME=str(home), XDG_CACHE_HOME=str(home / '.cache'))
    return subprocess.run(
        [command, 'render', 'index.qmd', '--to', 'html', '--no-execute',
         '-M', 'embed-resources:true'],
        cwd=work, capture_output=True, text=True, timeout=QUARTO_TIMEOUT_SECONDS, env=env,
    )


def _cache_problem(output: str) -> bool:
    text = output.lower()
    return ('permission denied' in text or 'read-only' in text or 'readonly' in text) and (
        'cache' in text or 'sass' in text
    )


def render_quarto(pipeline, cache_dir: str) -> str:
    """
    README.qmd rendered to one HTML file with its images embedded and code cells shown,
    not run. The render reads the pipeline folder's files (includes, styles, metadata,
    images). Cached by those files and the Quarto version.
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
    files = _render_files(folder)
    digest = hashlib.sha256(quarto_version(command).encode())
    for path in files:
        digest.update(f'{path.relative_to(folder)}:{path.stat().st_mtime_ns}'.encode())
    digest.update(source.read_bytes())
    cached = Path(cache_dir) / f'{digest.hexdigest()[:32]}.html'
    if cached.is_file():
        return cached.read_text(encoding='utf-8')

    with tempfile.TemporaryDirectory(prefix='mage-docs-') as tmp:
        work = Path(tmp)
        for path in files:
            target = work / path.relative_to(folder)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        shutil.copy2(source, work / 'index.qmd')
        try:
            result = _quarto_run(command, work)
            output_text = (result.stderr or '') + (result.stdout or '')
            if result.returncode != 0 and _cache_problem(output_text):
                result = _quarto_run(command, work, home=Path(cache_dir) / 'quarto-home')
        except subprocess.TimeoutExpired:
            raise DocumentationError(
                f'Quarto took longer than {QUARTO_TIMEOUT_SECONDS} s to render.',
            )
        output = work / 'index.html'
        if result.returncode != 0 or not output.is_file():
            message = (result.stderr or result.stdout).strip()[-3000:]
            # Name the pipeline's files, not the temporary copies.
            message = message.replace(str(work.resolve()), str(folder)).replace(
                str(work), str(folder),
            ).replace('index.qmd', FILES['quarto'])
            raise DocumentationError(f'Quarto could not render the document:\n{message}')
        html = output.read_text(encoding='utf-8')
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_text(html, encoding='utf-8')
    return html
