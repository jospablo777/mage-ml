"""
Writes the documentation as plain Markdown that GitHub renders, in docs-md/, from the
Mintlify pages in docs/. Run it after changing docs/:

    uv run python scripts/docs/export_markdown.py

Mintlify components become Markdown or HTML GitHub shows: callouts become alerts
(> [!NOTE]), accordions <details>, tabs and steps bold headings, light and dark image
pairs <picture> elements. Links to pages point at the .md files, and images at docs/
itself, so nothing is copied. The index, docs-md/README.md, follows the site's navigation.
"""
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / 'docs'
OUT = ROOT / 'docs-md'
SKIPPED_DIRS = {'node_modules', 'snippets', 'images', 'media', 'logo', 'favicon'}

CALLOUTS = {
    'Note': 'NOTE', 'Info': 'NOTE', 'Tip': 'TIP', 'Check': 'TIP', 'Warning': 'WARNING',
    'Callout': 'NOTE',
}
# Components whose tags are dropped and whose content stays.
WRAPPERS = {
    'AccordionGroup', 'CardGroup', 'CodeGroup', 'Steps', 'Tabs', 'Expandable',
    'RequestExample', 'ResponseExample', 'Frame', 'ProBanner', 'ProButton',
}
COMPONENTS = set(CALLOUTS) | WRAPPERS | {
    'Accordion', 'Tab', 'Step', 'Card', 'ParamField', 'ResponseField', 'Update', 'Snippet',
    'ProOnly',
}
ATTRIBUTE = re.compile(r'''([\w:-]+)(?:=(?:"([^"]*)"|'([^']*)'|\{([^}]*)\}))?''')
FENCE = re.compile(r'^(\s*)(```|~~~)')


def attributes(text: str) -> Dict[str, str]:
    found = {}
    for name, double, single, expression in ATTRIBUTE.findall(text):
        value = double or single or expression
        found[name] = value.strip().strip('"\'') if value else 'true'
    return found


def front_matter(text: str) -> Tuple[Dict[str, str], str]:
    if not text.startswith('---\n'):
        return {}, text
    end = text.find('\n---', 4)
    if end == -1:
        return {}, text
    meta = {}
    for line in text[4:end].splitlines():
        match = re.match(r'^([\w"\':.-]+):\s*(.*)$', line)
        if match:
            meta[match.group(1).strip('"\'')] = match.group(2).strip().strip('"\'')
    return meta, text[end + 4:].lstrip('\n')


class Site:
    """The pages, redirects and navigation of the docs, for links and the index."""

    def __init__(self, docs: Path, out: Path = OUT):
        self.docs = docs
        self.out = out
        config = json.loads((docs / 'docs.json').read_text())
        self.config = config
        self.redirects = {
            r['source'].rstrip('/'): r['destination'] for r in config.get('redirects', [])
        }
        self.pages: Dict[str, Path] = {}
        for path in sorted(docs.rglob('*')):
            if path.suffix not in ('.md', '.mdx') or not path.is_file():
                continue
            parts = path.relative_to(docs).parts
            if any(part in SKIPPED_DIRS for part in parts[:-1]):
                continue
            slug = '/' + str(path.relative_to(docs).with_suffix(''))
            self.pages[slug] = path

    def target(self, slug: str) -> Optional[str]:
        """The page a link points at, after redirects."""
        slug = slug.rstrip('/') or '/'
        for _ in range(5):
            if slug in self.pages:
                return slug
            if slug in self.redirects:
                slug = self.redirects[slug].split('#')[0].rstrip('/')
                continue
            if slug + '/index' in self.pages:
                return slug + '/index'
            return None
        return None


def relative(from_file: Path, to_file: Path, site: 'Site') -> str:
    """
    The path from one file to another as GitHub resolves it, with docs/ and docs-md/ side
    by side in the repository wherever the export is written.
    """
    def logical(path: Path) -> Path:
        path = path.resolve()
        for base in (site.out.resolve(), site.docs.resolve()):
            if path.is_relative_to(base):
                return Path(base.name) / path.relative_to(base)
        return path
    start = logical(from_file).parent
    return os.path.relpath(logical(to_file), start).replace(os.sep, '/')


def rewrite_url(url: str, site: Site, out_file: Path, source: Optional[Path] = None) -> str:
    """A link or image path from the site, as a path GitHub resolves from out_file."""
    if re.match(r'^[a-z][a-z0-9+.-]*:', url, re.I) or url.startswith(('//', '#')) or not url:
        return url
    if not url.startswith('/'):
        # Relative to the source page: a file beside it, such as an image.
        if source is not None:
            path, _, anchor = url.partition('#')
            beside = (source.parent / path).resolve()
            if beside.is_file() and beside.suffix not in ('.md', '.mdx'):
                return relative(out_file, beside, site) + ('#' + anchor if anchor else '')
        return url
    path, _, anchor = url.partition('#')
    if (site.docs / path.lstrip('/')).is_file():
        # An image or another file served from docs/.
        rewritten = relative(out_file, site.docs / path.lstrip('/'), site)
    else:
        slug = site.target(path)
        if slug is None:
            return url
        rewritten = relative(out_file, site.out / (slug.lstrip('/') + '.md'), site)
    return rewritten + ('#' + anchor if anchor else '')


def rewrite_links(line: str, site: Site, out_file: Path, source: Optional[Path] = None) -> str:
    # Inline code shows paths as examples; leave it as it is.
    parts = re.split(r'(`[^`]*`)', line)
    for i, part in enumerate(parts):
        if part.startswith('`'):
            continue
        part = re.sub(
            r'(\]\()([^)\s]+)(\))',
            lambda m: m.group(1) + rewrite_url(m.group(2), site, out_file, source) + m.group(3),
            part,
        )
        parts[i] = re.sub(
            r'''((?:src|href|srcset)=["'])([^"']*)(["'])''',
            lambda m: m.group(1) + rewrite_url(m.group(2), site, out_file, source) + m.group(3),
            part,
        )
    return ''.join(parts)


def image_html(attrs: Dict[str, str]) -> str:
    keep = [f'{k}="{attrs[k]}"' for k in ('src', 'alt', 'width', 'height') if k in attrs]
    return '<img ' + ' '.join(keep) + ' />'


class Converter:
    def __init__(self, site: Site, source: Path, out_file: Path):
        self.site = site
        self.source = source
        self.out_file = out_file
        self.output: List[str] = []
        # Open components: (name, indentation of their content or None until known).
        self.stack: List[List] = []
        self.step = 0
        self.frame_images: List[Dict[str, str]] = []
        self.frame_caption: Optional[str] = None

    @property
    def quote(self) -> str:
        return '> ' * sum(1 for name, _ in self.stack if name in CALLOUTS)

    def emit(self, line: str = '') -> None:
        prefix = self.quote
        if not line.strip():
            self.output.append(prefix.rstrip() if prefix else '')
        else:
            self.output.append(prefix + line)

    def dedent(self, line: str) -> str:
        if not self.stack:
            return line
        content = self.stack[-1]
        leading = len(line) - len(line.lstrip(' '))
        if content[1] is None and line.strip():
            content[1] = leading
        return line[min(leading, content[1] or 0):]

    def convert(self, text: str) -> str:
        meta, body = front_matter(text)
        if meta.get('title'):
            self.output.append(f"# {meta['title']}")
            self.output.append('')
        if meta.get('description'):
            self.output.append(
                '*' + rewrite_links(meta['description'], self.site, self.out_file) + '*'
            )
            self.output.append('')
        lines = body.splitlines()
        index = 0
        fence: Optional[str] = None
        fence_indent = 0
        while index < len(lines):
            line = lines[index]
            index += 1
            if fence is not None:
                if line.strip().startswith(fence):
                    fence = None
                self.emit(line[min(fence_indent, len(line) - len(line.lstrip(' '))):])
                continue
            match = FENCE.match(line)
            if match:
                fence = match.group(2)
                stripped = self.dedent(line)
                fence_indent = len(line) - len(stripped)
                # The fence's language only: Mintlify titles follow it.
                info = stripped.strip()[3:].strip().split(' ')[0]
                self.emit(stripped.strip()[:3] + info)
                continue
            stripped = line.strip()
            if stripped.startswith(('import ', 'export ')) and not self.stack:
                continue
            if stripped.startswith('{/*') and stripped.endswith('*/}'):
                continue
            # A tag that spans lines: join until its closing bracket.
            if re.match(r'^<(/?)([A-Za-z][\w.]*)', stripped) and '>' not in stripped:
                while index < len(lines) and '>' not in lines[index - 1]:
                    line = line + ' ' + lines[index].strip()
                    index += 1
                stripped = line.strip()
            if self.tag(stripped):
                continue
            text = self.dedent(line).rstrip()
            self.emit(rewrite_links(text, self.site, self.out_file, self.source))
        text = '\n'.join(self.output)
        text = re.sub(r'\n{3,}', '\n\n', text).strip() + '\n'
        return text

    def tag(self, line: str) -> bool:
        """Handles a line holding a component's tag; False when it is ordinary text."""
        single = re.match(r'^<(\w+)([^>]*)>(.*)</\1>$', line)
        if single and single.group(1) in CALLOUTS:
            self.emit()
            self.emit(f'> [!{CALLOUTS[single.group(1)]}]')
            self.emit('> ' + rewrite_links(single.group(3).strip(), self.site, self.out_file))
            self.emit()
            return True
        if re.match(r'^<img\b', line):
            attrs = attributes(line[4:].rstrip('/>'))
            if 'src' in attrs:
                attrs['src'] = rewrite_url(attrs['src'], self.site, self.out_file, self.source)
            if any(name == 'Frame' for name, _ in self.stack):
                self.frame_images.append(attrs)
            else:
                self.emit(image_html(attrs))
            return True
        if re.match(r'^<(iframe|video)\b', line):
            attrs = attributes(re.sub(r'^<\w+|/?>.*$', '', line))
            source = attrs.get('src') or ''
            if not source:
                found = re.search(r'src="([^"]+)"', line)
                source = found.group(1) if found else ''
            if source:
                self.emit(f"[{attrs.get('title') or 'Video'}]({source})")
            return True
        if re.match(r'^</?(source)\b', line) or re.match(r'^</(iframe|video)>', line):
            return True
        closing = re.match(r'^</(\w+)>$', line)
        if closing and closing.group(1) in COMPONENTS:
            self.close(closing.group(1))
            return True
        opening = re.match(r'^<(\w+)\b(.*?)(/?)>(.*)$', line)
        if not opening or opening.group(1) not in COMPONENTS:
            return False
        name, raw, self_closing, rest = opening.groups()
        attrs = attributes(raw)
        self.open(name, attrs, bool(self_closing))
        if rest.strip() and not self_closing:
            self.emit(rewrite_links(rest.strip(), self.site, self.out_file))
        return True

    def open(self, name: str, attrs: Dict[str, str], self_closing: bool) -> None:
        if name == 'Snippet':
            snippet = self.site.docs / 'snippets' / attrs.get('file', '')
            if snippet.is_file():
                _, body = front_matter(snippet.read_text())
                inner = Converter(self.site, snippet, self.out_file).convert(body)
                for line in inner.splitlines():
                    self.emit(line)
            return
        if name == 'ProOnly':
            self.emit()
            self.emit('> [!NOTE]')
            self.emit('> Not included in MageML: this describes a feature of Mage Pro, '
                      "Mage's managed product.")
            self.emit()
            return
        if name in CALLOUTS:
            self.emit()
            self.stack.append([name, None])
            self.emit(f'[!{CALLOUTS[name]}]')
            return
        if name == 'Accordion':
            self.emit()
            self.emit(f"<details><summary>{attrs.get('title', 'Details')}</summary>")
            self.emit()
        elif name in ('Tab', 'Step'):
            if name == 'Step':
                self.step += 1
                title = f"{self.step}. {attrs.get('title', '')}".strip()
            else:
                title = attrs.get('title', '')
            self.emit()
            self.emit(f'**{title}**')
            self.emit()
        elif name == 'Steps':
            self.step = 0
        elif name == 'Card':
            href = rewrite_url(attrs.get('href', ''), self.site, self.out_file)
            title = attrs.get('title', '')
            self.emit()
            self.emit(f'**[{title}]({href})**' if href else f'**{title}**')
            self.emit()
        elif name in ('ParamField', 'ResponseField'):
            field = attrs.get('path') or attrs.get('name') or attrs.get('query') or \
                attrs.get('body') or attrs.get('header') or ''
            details = [attrs[k] for k in ('type',) if k in attrs]
            if attrs.get('required') == 'true':
                details.append('required')
            if 'default' in attrs:
                details.append(f"default `{attrs['default']}`")
            self.emit()
            self.emit(f'**`{field}`**' + (f" ({', '.join(details)})" if details else ''))
            self.emit()
        elif name == 'Update':
            self.emit()
            label = attrs.get('label', '')
            description = attrs.get('description', '')
            self.emit(f'### {label}' + (f' ({description})' if description else ''))
            self.emit()
        elif name == 'Frame':
            self.frame_images = []
            self.frame_caption = attrs.get('caption')
        elif name in ('RequestExample', 'ResponseExample'):
            self.emit()
            self.emit('**Request**' if name == 'RequestExample' else '**Response**')
            self.emit()
        if not self_closing:
            self.stack.append([name, None])
        elif name == 'Frame':
            self.close_frame()

    def close_frame(self) -> None:
        images = self.frame_images
        light = [i for i in images if 'dark:hidden' in i.get('className', '')]
        dark = [i for i in images if 'dark:block' in i.get('className', '')]
        self.emit()
        if light and dark:
            self.emit('<picture>')
            self.emit(f'  <source media="(prefers-color-scheme: dark)" srcset="{dark[0]["src"]}">')
            self.emit('  ' + image_html(light[0]))
            self.emit('</picture>')
        else:
            for image in images:
                self.emit(image_html(image))
        if self.frame_caption:
            self.emit()
            self.emit(f'*{self.frame_caption}*')
        self.emit()
        self.frame_images = []
        self.frame_caption = None

    def close(self, name: str) -> None:
        while self.stack:
            top, _ = self.stack.pop()
            if top == 'Frame':
                self.close_frame()
            elif top == 'Accordion':
                self.emit()
                self.emit('</details>')
                self.emit()
            elif top in CALLOUTS:
                self.emit()
            if top == name:
                break


def title_of(path: Path) -> str:
    meta, body = front_matter(path.read_text())
    if meta.get('title'):
        return meta['title']
    heading = re.search(r'^#\s+(.+)$', body, re.M)
    return heading.group(1).strip() if heading else path.stem.replace('-', ' ')


def index(site: Site) -> str:
    lines = [
        '# MageML documentation',
        '',
        'The MageML documentation as Markdown, for reading on GitHub. It is generated from '
        'the pages in [`docs/`](../docs) by `scripts/docs/export_markdown.py`; edit those '
        'pages, not these files. The same pages, with search, run as a site with '
        '`docs/docs.sh` (http://localhost:3333).',
        '',
    ]
    listed = set()

    def page_line(slug: str, depth: int) -> Optional[str]:
        slug = '/' + slug.lstrip('/')
        if slug not in site.pages:
            return None
        listed.add(slug)
        return f"{'  ' * depth}- [{title_of(site.pages[slug])}]({slug.lstrip('/')}.md)"

    def walk_pages(pages, depth: int) -> None:
        for page in pages:
            if isinstance(page, str):
                line = page_line(page, depth)
                if line:
                    lines.append(line)
            elif isinstance(page, dict) and 'pages' in page:
                lines.append(f"{'  ' * depth}- **{page.get('group', '')}**")
                walk_pages(page['pages'], depth + 1)

    for tab in site.config.get('navigation', {}).get('tabs', []):
        lines += [f"## {tab.get('tab', '')}", '']
        for group in tab.get('groups', []):
            lines += [f"### {group.get('group', '')}", '']
            walk_pages(group.get('pages', []), 0)
            lines.append('')
    others = [slug for slug in site.pages if slug not in listed]
    if others:
        lines += ['## Other pages', '']
        lines += [f"- [{title_of(site.pages[s])}]({s.lstrip('/')}.md)" for s in sorted(others)]
        lines.append('')
    return '\n'.join(lines)


def broken_links(out: Path, docs: Path = DOCS) -> List[str]:
    """Relative links and images in the export that point at nothing."""
    def exists(page: Path, target: str) -> bool:
        logical = Path(os.path.normpath(Path(out.name) / page.relative_to(out).parent / target))
        first, rest = logical.parts[0], Path(*logical.parts[1:])
        if first == out.name:
            return (out / rest).exists()
        if first == docs.name:
            return (docs / rest).exists()
        return (out.parent / logical).exists()

    problems = []
    pattern = re.compile(r'''\]\(([^)\s]+)\)|(?:src|href|srcset)=["']([^"']+)["']''')
    for path in sorted(out.rglob('*.md')):
        text = re.sub(r'(?ms)^\s*(```|~~~).*?^\s*\1', '', path.read_text())
        text = re.sub(r'`[^`\n]*`', '', text)
        for match in pattern.finditer(text):
            target = (match.group(1) or match.group(2)).split('#')[0]
            if not target or re.match(r'^[a-z]+:', target) or target.startswith('//'):
                continue
            if target.startswith('/'):
                problems.append(f'{path.relative_to(out)}: {target} (site path not found)')
            elif not exists(path, target):
                problems.append(f'{path.relative_to(out)}: {target}')
    return problems


def export(docs: Path = DOCS, out: Path = OUT) -> List[str]:
    site = Site(docs, out)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    for slug, source in site.pages.items():
        out_file = out / (slug.lstrip('/') + '.md')
        out_file.parent.mkdir(parents=True, exist_ok=True)
        out_file.write_text(Converter(site, source, out_file).convert(source.read_text()))
    (out / 'README.md').write_text(index(site) + '\n')
    return broken_links(out, docs)


def main() -> None:
    problems = export()
    print(f'Wrote {OUT.relative_to(ROOT)}/')
    for problem in problems:
        print(f'broken link: {problem}')
    if problems:
        sys.exit(1)


if __name__ == '__main__':
    main()
