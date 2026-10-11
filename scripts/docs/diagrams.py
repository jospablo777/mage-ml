"""
Draws the documentation's diagrams, with the logos of the languages and tools they show, as
SVG files in docs/images/diagrams: <name>.light.svg and <name>.dark.svg for each theme.

    uv run python scripts/docs/diagrams.py
"""
import html
from pathlib import Path
from typing import List, Optional, Tuple

from integration_tests.benchmark.pipelines.logos import COLORS, logo

OUT = Path(__file__).resolve().parents[2] / 'docs' / 'images' / 'diagrams'
THEMES = {
    'light': dict(text='#1F2328', muted='#59636E', line='#8C959F', card='#FFFFFF',
                  group='#F6F8FA', border='#D0D7DE'),
    'dark': dict(text='#E6EDF3', muted='#9198A1', line='#6E7681', card='#161B22',
                 group='#0D1117', border='#30363D'),
}
FONT = 'system-ui, -apple-system, Segoe UI, sans-serif'


class Diagram:
    def __init__(self, width: int, height: int, title: str):
        self.width, self.height, self.title = width, height, title
        self.parts: List[str] = []

    def group(self, x, y, w, h, label: str, color: Optional[str] = None):
        stroke = color or '{border}'
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{{group}}" '
            f'stroke="{stroke}" stroke-width="1.5" stroke-dasharray="{"" if color else "5 4"}"/>'
            f'<text x="{x + 14}" y="{y + 22}" font-size="12.5" font-weight="600" '
            f'fill="{color or "{muted}"}">{html.escape(label)}</text>'
        )

    def node(self, x, y, w, h, label: str, slug: Optional[str] = None, sub: str = '',
             color: Optional[str] = None):
        accent = color or (COLORS.get(slug) if slug else '{line}')
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="9" fill="{{card}}" '
            f'stroke="{accent}" stroke-width="1.6"/>'
        )
        tx = x + 14
        if slug:
            size = 22
            self.parts.append(logo(slug, x + 12, y + (h - size) / 2, size))
            tx = x + 12 + size + 10
        ty = y + h / 2 + (0 if sub else 5)
        self.parts.append(f'<text x="{tx}" y="{ty}" font-size="14" font-weight="600" '
                          f'fill="{{text}}">{html.escape(label)}</text>')
        if sub:
            self.parts.append(f'<text x="{tx}" y="{ty + 17}" font-size="11.5" '
                              f'fill="{{muted}}">{html.escape(sub)}</text>')

    def arrow(self, points: List[Tuple[float, float]], label: str = '', dashed=False,
              label_at: Optional[Tuple[float, float]] = None):
        path = ' '.join(f'{"M" if i == 0 else "L"}{x},{y}' for i, (x, y) in enumerate(points))
        dash = ' stroke-dasharray="5 4"' if dashed else ''
        self.parts.append(
            f'<path d="{path}" fill="none" stroke="{{line}}" stroke-width="1.6" '
            f'marker-end="url(#arrow)"{dash}/>'
        )
        if label:
            (x1, y1), (x2, y2) = points[-2], points[-1]
            lx, ly = label_at or ((x1 + x2) / 2, (y1 + y2) / 2 - 7)
            self.parts.append(f'<text x="{lx}" y="{ly}" font-size="11" text-anchor="middle" '
                              f'fill="{{muted}}">{html.escape(label)}</text>')

    def text(self, x, y, value: str, size=12, anchor='start', weight='400', muted=True):
        self.parts.append(f'<text x="{x}" y="{y}" font-size="{size}" text-anchor="{anchor}" '
                          f'font-weight="{weight}" fill="{{{"muted" if muted else "text"}}}">'
                          f'{html.escape(value)}</text>')

    def badge(self, x, y, value: str, color: str):
        w = 8 + 6.4 * len(value)
        self.parts.append(f'<rect x="{x - w / 2}" y="{y - 11}" width="{w}" height="18" rx="9" '
                          f'fill="{color}" fill-opacity="0.15" stroke="{color}"/>'
                          f'<text x="{x}" y="{y + 2}" font-size="10.5" text-anchor="middle" '
                          f'fill="{color}" font-weight="600">{html.escape(value)}</text>')

    def svg(self, theme: str) -> str:
        colors = THEMES[theme]
        body = '\n'.join(self.parts)
        for key, value in colors.items():
            body = body.replace('{' + key + '}', value)
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.width} {self.height}" '
            f'role="img" aria-label="{html.escape(self.title)}" font-family="{FONT}">\n'
            f'<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
            f'markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" '
            f'fill="{colors["line"]}"/></marker></defs>\n{body}\n</svg>\n'
        )


def compiled_chains() -> Diagram:
    """A polyglot pipeline and the chains its production run forms."""
    d = Diagram(1080, 330, 'A polyglot pipeline and its compiled chains')
    d.text(24, 32, 'One pipeline, three languages: each run of blocks in one language becomes '
           'one chain', size=14, weight='600', muted=False)
    d.group(24, 56, 210, 120, 'Python chain: one process', COLORS['python'])
    d.node(40, 90, 178, 60, 'load_orders', 'pandas', 'pandas DataFrame')
    d.group(264, 56, 452, 120, 'Compiled Rust chain: one program, one Polars plan',
            COLORS['rust'])
    d.node(280, 90, 130, 60, 'clean', 'rust', 'plan')
    d.node(424, 90, 130, 60, 'enrich', 'rust', 'plan')
    d.node(568, 90, 138, 60, 'aggregate', 'rust', 'computes')
    d.group(746, 56, 310, 120, 'R chain: one R session', COLORS['r'])
    d.node(762, 90, 130, 60, 'model', 'r', 'tibble')
    d.node(910, 90, 130, 60, 'report', 'r', 'computes')
    d.arrow([(218, 120), (280, 120)])
    d.arrow([(410, 120), (424, 120)])
    d.arrow([(554, 120), (568, 120)])
    d.arrow([(706, 120), (762, 120)])
    d.arrow([(892, 120), (910, 120)])
    d.badge(249, 196, 'Arrow', COLORS['apachearrow'])
    d.badge(731, 196, 'Arrow', COLORS['apachearrow'])
    d.text(490, 234, 'At each language switch the data is converted (Apache Arrow). Inside a '
           'chain it stays in memory;', anchor='middle')
    d.text(490, 252, 'in a Rust chain, Polars optimizes every block\'s query as one plan and '
           'computes it once.', anchor='middle')
    d.text(490, 290, 'Fewer switches, faster runs: keep the heavy steps of a pipeline in one '
           'language.', anchor='middle', size=13, weight='600', muted=False)
    return d


def pipeline_service() -> Diagram:
    """From the editor to a service image and its runtime."""
    d = Diagram(1080, 380, 'Exporting a pipeline as a service')
    d.group(24, 30, 290, 320, 'MageML: build and test', None)
    d.node(44, 66, 250, 54, 'Editor and runs', None, 'pipelines and blocks')
    d.node(44, 134, 250, 54, 'Blocks', 'python', 'Python, SQL')
    d.node(44, 202, 250, 54, 'Blocks', 'rust', 'Rust, compiled')
    d.node(44, 270, 250, 54, 'Blocks', 'r', 'R, with rv')
    d.node(360, 150, 180, 64, 'mage export service', None, 'build context')
    d.node(586, 150, 150, 64, 'Image', 'docker', 'docker build')
    d.arrow([(314, 182), (360, 182)])
    d.arrow([(540, 182), (586, 182)])
    d.group(780, 30, 276, 320, 'Service container', COLORS['docker'])
    d.node(800, 70, 236, 54, 'HTTP API, schedules', 'rust', 'mage-service')
    d.node(800, 140, 236, 50, 'Python workers', 'python')
    d.node(800, 204, 236, 50, 'Rust blocks binary', 'rust')
    d.node(800, 268, 236, 50, 'R sessions', 'r')
    d.arrow([(736, 182), (780, 182)])
    d.text(918, 344, 'run history, logs, metrics, secrets', anchor='middle', size=11)
    return d


def feast_flow() -> Diagram:
    """Blocks reading and writing features through Feast's REST API."""
    d = Diagram(1000, 290, 'Reading and writing features with Feast')
    d.group(24, 30, 300, 200, 'MageML pipeline', None)
    d.node(44, 70, 260, 56, 'Load features', 'pandas', 'get_online_features')
    d.node(44, 146, 260, 56, 'Push features', 'polars', 'push, write_to_online_store')
    d.node(400, 100, 220, 70, 'Feast feature server', None, 'REST API', color='#B57EDC')
    d.group(700, 30, 276, 200, 'Feature stores', None)
    d.node(720, 70, 236, 56, 'Online store', 'postgresql', 'latest values')
    d.node(720, 146, 236, 56, 'Offline store', 'postgresql', 'history, materialize')
    d.arrow([(304, 98), (400, 125)], 'HTTP', label_at=(352, 100))
    d.arrow([(304, 174), (400, 150)], 'HTTP', label_at=(352, 176))
    d.arrow([(620, 125), (720, 98)])
    d.arrow([(620, 150), (720, 174)])
    d.text(500, 260, 'FeastClient keeps values exact (large integers, event order, time '
           'zones) and retries only transient failures.', anchor='middle', size=11.5)
    return d


def documentation() -> Diagram:
    """Where a pipeline's README lives and how it shows."""
    d = Diagram(1000, 250, 'Pipeline documentation')
    d.group(24, 30, 330, 190, 'pipelines/sales/', None)
    d.node(44, 66, 290, 50, 'README.md or README.qmd', None, 'versions with the pipeline')
    d.node(44, 134, 290, 50, 'images/flow.png', None, 'shown with ![...](images/flow.png)')
    d.node(430, 66, 230, 50, 'Markdown', 'markdown', 'rendered in the browser')
    d.node(430, 134, 230, 50, 'Quarto', 'quarto', 'quarto render, no code run')
    d.node(740, 96, 236, 58, 'Documentation page', None, 'pipeline row button and nav')
    d.arrow([(354, 125), (430, 91)])
    d.arrow([(354, 125), (430, 159)])
    d.arrow([(660, 91), (740, 118)])
    d.arrow([(660, 159), (740, 134)])
    return d


DIAGRAMS = {
    'compiled-chains': compiled_chains,
    'pipeline-service': pipeline_service,
    'feast-flow': feast_flow,
    'documentation': documentation,
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, build in DIAGRAMS.items():
        diagram = build()
        for theme in THEMES:
            (OUT / f'{name}.{theme}.svg').write_text(diagram.svg(theme))
        print(OUT / f'{name}.*.svg')


if __name__ == '__main__':
    main()
