"""
Draws the documentation's diagrams, with the logos of the languages and tools they show, as
SVG files in docs/images/diagrams: <name>.light.svg and <name>.dark.svg for each theme.

    uv run python scripts/docs/diagrams.py
"""
import html
import sys
from pathlib import Path
from typing import List, Optional, Tuple

# The logos live in integration_tests, a package of the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from integration_tests.benchmark.pipelines.logos import COLORS, logo  # noqa: E402

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

    def curve(self, start: Tuple[float, float], end: Tuple[float, float]):
        """An arrow that leaves start to the right and enters end from the left."""
        (x1, y1), (x2, y2) = start, end
        bend = max(18.0, (x2 - x1) / 2)
        self.parts.append(
            f'<path d="M{x1},{y1} C{x1 + bend},{y1} {x2 - bend},{y2} {x2},{y2}" fill="none" '
            f'stroke="{{line}}" stroke-width="1.5" marker-end="url(#arrow)"/>'
        )

    def chip(self, x, y, slug: str, size: int = 40):
        """A block drawn as its language's logo in a rounded square."""
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{size}" height="{size}" rx="10" fill="{{card}}" '
            f'stroke="{COLORS[slug]}" stroke-width="1.8"/>'
        )
        inner = size * 0.55
        self.parts.append(logo(slug, x + (size - inner) / 2, y + (size - inner) / 2, inner))

    def stage(self, x, y, w, h, color: str = '#2DA44E', label: str = ''):
        """The blocks that run together, as one fused stage."""
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{color}" '
            f'fill-opacity="0.09" stroke="{color}" stroke-width="1.3" stroke-dasharray="5 4"/>'
        )
        if label:
            self.parts.append(f'<text x="{x + w / 2}" y="{y + h + 14}" font-size="10.5" '
                              f'text-anchor="middle" fill="{color}" font-weight="600">'
                              f'{html.escape(label)}</text>')

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


def tutorial_pipeline() -> Diagram:
    """The pipeline the polyglot tutorial builds."""
    d = Diagram(1080, 250, 'The tutorial pipeline')
    d.group(24, 40, 200, 110, 'Python', COLORS['python'])
    d.node(40, 72, 168, 58, 'load_orders', 'pandas', 'data loader')
    d.group(244, 40, 412, 110, 'Rust: one compiled chain', COLORS['rust'])
    d.node(260, 72, 170, 58, 'clean_orders', 'rust', 'transformer')
    d.node(446, 72, 194, 58, 'monthly_revenue', 'rust', 'transformer')
    d.group(676, 40, 380, 110, 'R: one session', COLORS['r'])
    d.node(692, 72, 172, 58, 'revenue_trend', 'r', 'transformer')
    d.node(880, 72, 160, 58, 'save_trend', 'r', 'data exporter')
    d.arrow([(208, 101), (260, 101)])
    d.arrow([(430, 101), (446, 101)])
    d.arrow([(640, 101), (692, 101)])
    d.arrow([(864, 101), (880, 101)])
    d.badge(234, 170, 'Arrow', COLORS['apachearrow'])
    d.badge(666, 170, 'Arrow', COLORS['apachearrow'])
    d.text(540, 214, '200,000 generated orders, cleaned and summed by region and month in '
           'Rust, then a month-over-month trend in R.', anchor='middle', size=12.5,
           muted=False)
    return d


def rust_block() -> Diagram:
    """How a Rust block builds and runs."""
    d = Diagram(1080, 270, 'How a Rust block runs')
    d.node(24, 60, 190, 60, 'transform.rs', 'rust', 'your block')
    d.node(260, 60, 220, 60, 'Crate in rust/', 'rust', 'workspace, Cargo.lock')
    d.node(526, 60, 230, 60, 'cargo build', 'rust', 'cached by code and deps')
    d.node(802, 60, 254, 60, 'Block binary', 'rust', 'Polars linked in')
    d.arrow([(214, 90), (260, 90)])
    d.arrow([(480, 90), (526, 90)])
    d.arrow([(756, 90), (802, 90)])
    d.node(24, 170, 250, 60, 'Upstream outputs', 'apachearrow', 'Parquet or Arrow IPC')
    d.node(420, 170, 240, 60, 'Run the binary', 'polars', 'LazyFrame in, plan, collect')
    d.node(806, 170, 250, 60, 'Output and tests', 'apachearrow', 'Arrow IPC back to MageML')
    d.arrow([(274, 200), (420, 200)])
    d.arrow([(660, 200), (806, 200)])
    d.arrow([(929, 120), (929, 150), (540, 150), (540, 170)], dashed=True)
    return d


def r_block() -> Diagram:
    """How an R block runs."""
    d = Diagram(1080, 230, 'How an R block runs')
    d.node(24, 40, 200, 60, 'block.R', 'r', '#* @transformer')
    d.node(270, 40, 270, 60, 'Rscript --vanilla', 'r', 'only the locked rv library')
    d.node(586, 40, 220, 60, 'mageml package', 'r', 'calls your function')
    d.node(852, 40, 204, 60, 'Data frame out', 'r', 'tibble')
    d.arrow([(224, 70), (270, 70)])
    d.arrow([(540, 70), (586, 70)])
    d.arrow([(806, 70), (852, 70)])
    d.node(270, 140, 270, 56, 'Inputs as Arrow', 'apachearrow', 'integer64 fits as integer')
    d.node(586, 140, 220, 56, 'Outputs as Arrow', 'apachearrow', 'read by any block')
    d.arrow([(405, 140), (405, 100)])
    d.arrow([(696, 100), (696, 140)])
    d.text(540, 222, 'In a production run, consecutive R blocks share one session: '
           'packages load once.', anchor='middle', size=12)
    return d


def r_packages() -> Diagram:
    """Where an R block's packages come from and where they go."""
    d = Diagram(1100, 320, 'R packages with rv')
    d.group(24, 30, 268, 262, 'Package sources', None)
    d.node(40, 62, 236, 48, 'Posit Package Manager', 'r', 'binaries, dated snapshots')
    d.node(40, 120, 236, 48, 'CRAN', 'r', 'the R package archive')
    d.node(40, 178, 236, 48, 'Bioconductor', 'r', 'added as a repository')
    d.node(40, 236, 236, 48, 'Git repositories', 'github', 'tag, branch or commit')
    d.group(332, 30, 462, 262, '<project>/r: the rv project', COLORS['r'])
    d.node(348, 62, 200, 58, 'rproject.toml', 'r', 'repos and packages')
    d.node(578, 62, 200, 58, 'rv.lock', 'r', 'exact versions')
    d.node(578, 196, 200, 58, 'rv/library', 'r', 'installed packages')
    d.arrow([(548, 91), (578, 91)])
    d.arrow([(678, 120), (678, 196)], 'rv sync', label_at=(716, 162))
    d.text(448, 150, 'rv add and rv remove', anchor='middle', size=11)
    d.text(448, 166, 'edit both files', anchor='middle', size=11)
    d.text(448, 214, 'Commit rproject.toml', anchor='middle', size=11)
    d.text(448, 230, 'and rv.lock. The library', anchor='middle', size=11)
    d.text(448, 246, 'is generated.', anchor='middle', size=11)
    d.arrow([(292, 161), (320, 161), (320, 91), (348, 91)])
    d.node(840, 62, 236, 58, 'Exported service', 'docker', 'image syncs rv.lock')
    d.node(840, 196, 236, 58, 'R blocks', 'r', 'Rscript, this library only')
    d.arrow([(778, 91), (840, 91)], dashed=True)
    d.arrow([(778, 225), (840, 225)])
    return d


def tutorial_pipeline_python() -> Diagram:
    """The tutorial pipeline in Python only: one chain in one process."""
    d = Diagram(1080, 230, 'The tutorial pipeline in Python only')
    d.group(24, 40, 1032, 110, 'Python: one compiled chain, one process, frames in memory',
            COLORS['python'])
    nodes = [('load_orders', 'data loader', 150), ('clean_orders_py', 'transformer', 190),
             ('monthly_revenue_py', 'transformer', 220), ('revenue_trend_py', 'transformer', 200),
             ('save_trend_py', 'data exporter', 160)]
    x = 40
    for position, (name, kind, width) in enumerate(nodes):
        d.node(x, 72, width, 58, name, 'pandas', kind)
        if position:
            d.arrow([(x - 20, 101), (x, 101)])
        x += width + 20
    d.text(540, 190, 'No language switches, so no conversions: each block hands its frame '
           'to the next in memory.', anchor='middle', size=12.5, muted=False)
    return d



def single_vs_polyglot() -> Diagram:
    """The same pipeline in one language and in three: chains, switches and conversions."""
    d = Diagram(1080, 420, 'The same pipeline in one language and in three')
    d.text(24, 30, 'Python only: one chain, no conversions', size=14, weight='600',
           muted=False)
    d.group(24, 44, 1032, 104, 'One Python process: each block hands its frame to the next '
            'in memory', COLORS['python'])
    x = 40
    for position, (name, kind) in enumerate((
        ('load', 'pandas'), ('clean', 'pandas'), ('aggregate', 'pandas'),
        ('trend', 'pandas'), ('export', 'pandas'),
    )):
        d.node(x, 74, 180, 56, name, kind, 'pandas DataFrame')
        if position:
            d.arrow([(x - 22, 102), (x, 102)])
        x += 202
    d.text(24, 196, 'Python, Rust and R: four chains, three conversions', size=14,
           weight='600', muted=False)
    d.group(24, 210, 210, 104, 'Python', COLORS['python'])
    d.node(40, 240, 178, 56, 'load', 'pandas', 'pandas DataFrame')
    d.group(264, 210, 414, 104, 'Rust: one program, one Polars plan', COLORS['rust'])
    d.node(280, 240, 180, 56, 'clean', 'rust', 'plan')
    d.node(482, 240, 180, 56, 'aggregate', 'rust', 'computes')
    d.group(708, 210, 196, 104, 'R session', COLORS['r'])
    d.node(724, 240, 164, 56, 'trend', 'r', 'tibble')
    d.group(934, 210, 122, 104, 'Python', COLORS['python'])
    d.node(946, 240, 98, 56, 'export', 'pandas')
    d.arrow([(218, 268), (280, 268)])
    d.arrow([(460, 268), (482, 268)])
    d.arrow([(662, 268), (724, 268)])
    d.arrow([(888, 268), (946, 268)])
    for x in (249, 693, 919):
        d.badge(x, 332, 'Arrow', COLORS['apachearrow'])
    d.text(540, 372, 'Each switch starts another process and converts the data. On small '
           'tables that costs more than it saves;', anchor='middle')
    d.text(540, 392, 'on large tables, Rust on the heavy steps can pay for it.',
           anchor='middle')
    return d

FUSED = '#2DA44E'
CHIP = 40


def dag(d: Diagram, ox: float, oy: float, nodes: dict, edges: List[Tuple[str, str]],
        stages: List[List[str]] = ()):
    """A small pipeline: nodes {id: (x, y, language)} relative to (ox, oy)."""
    pad = 9
    for members in stages:
        xs = [nodes[m][0] for m in members]
        ys = [nodes[m][1] for m in members]
        d.stage(ox + min(xs) - pad, oy + min(ys) - pad, max(xs) - min(xs) + CHIP + 2 * pad,
                max(ys) - min(ys) + CHIP + 2 * pad, FUSED)
    for source, target in edges:
        sx, sy, _ = nodes[source]
        tx, ty, _ = nodes[target]
        d.curve((ox + sx + CHIP, oy + sy + CHIP / 2), (ox + tx - 2, oy + ty + CHIP / 2))
    for x, y, slug in nodes.values():
        d.chip(ox + x, oy + y, slug, CHIP)


def panel(d: Diagram, x, y, w, h, name: str, lines: List[str]):
    d.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="{{group}}" '
                   f'stroke="{{border}}" stroke-width="1.2"/>')
    d.text(x + 16, y + 26, name, size=14.5, weight='700', muted=False)
    for i, line in enumerate(lines):
        d.text(x + 16, y + h - 34 + 16 * i, line, size=11.5)


def legend(d: Diagram, x, y, slugs=('pandas', 'polars', 'r', 'rust')):
    names = {'pandas': 'pandas', 'polars': 'Polars', 'r': 'R', 'rust': 'Rust',
             'python': 'Python'}
    for slug in slugs:
        d.chip(x, y - 15, slug, 22)
        d.text(x + 30, y + 1, names[slug], size=12, muted=False)
        x += 40 + 7.5 * len(names[slug])
    d.stage(x, y - 15, 30, 22, FUSED)
    d.text(x + 38, y + 1, 'runs as one fused stage', size=12, muted=False)


def pipeline_shapes() -> Diagram:
    """The seven shapes of the compilation study, with what fuses in each."""
    d = Diagram(1080, 752, 'The seven pipeline shapes and the stages a production run forms')
    d.text(24, 34, 'Only a Line fuses end to end; branches and joins split a pipeline into '
           'stages that run side by side', size=15, weight='600', muted=False)
    top, w4, h = 56, 252, 300
    gap = 12
    x = [24 + i * (w4 + gap) for i in range(4)]
    panel(d, x[0], top, w4, h, 'Line',
          ['One path. Fuses end to end; each run', 'of one language compiles.'])
    dag(d, x[0] + 22, top + 108, {
        'a': (0, 0, 'pandas'), 'b': (56, 0, 'pandas'), 'c': (112, 0, 'polars'),
        'd': (168, 0, 'polars'),
    }, [('a', 'b'), ('b', 'c'), ('c', 'd')], [['a', 'b', 'c', 'd']])
    panel(d, x[1], top, w4, h, 'Parallel lines',
          ['Lines that share nothing: each line', 'fuses, and the lines run side by side.'])
    dag(d, x[1] + 34, top + 66, {
        'a': (0, 0, 'r'), 'b': (64, 0, 'r'), 'c': (128, 0, 'r'),
        'd': (0, 96, 'rust'), 'e': (64, 96, 'rust'), 'f': (128, 96, 'rust'),
    }, [('a', 'b'), ('b', 'c'), ('d', 'e'), ('e', 'f')], [['a', 'b', 'c'], ['d', 'e', 'f']])
    panel(d, x[2], top, w4, h, 'Diamond',
          ['One block feeds two that meet again:', 'the branches run side by side.'])
    dag(d, x[2] + 22, top + 62, {
        'a': (0, 52, 'pandas'), 'b': (84, 0, 'polars'), 'c': (84, 104, 'r'),
        'd': (168, 52, 'rust'),
    }, [('a', 'b'), ('a', 'c'), ('b', 'd'), ('c', 'd')])
    panel(d, x[3], top, w4, h, 'Broadcast',
          ['One block feeds several: each consumer', 'reads the stored table, side by side.'])
    dag(d, x[3] + 40, top + 54, {
        'a': (0, 64, 'polars'), 'b': (130, 0, 'pandas'), 'c': (130, 64, 'r'),
        'd': (130, 128, 'rust'),
    }, [('a', 'b'), ('a', 'c'), ('a', 'd')])

    top2, w3 = top + h + gap, (1032 - 2 * gap) / 3
    x = [24 + i * (w3 + gap) for i in range(3)]
    panel(d, x[0], top2, w3, h, 'Merge',
          ['Several roots feed one block: they load', 'side by side, then one join.'])
    dag(d, x[0] + 70, top2 + 54, {
        'a': (0, 0, 'pandas'), 'b': (0, 64, 'polars'), 'c': (0, 128, 'r'),
        'd': (140, 64, 'rust'),
    }, [('a', 'd'), ('b', 'd'), ('c', 'd')])
    panel(d, x[1], top2, w3, h, 'Lattice',
          ['Every block reads the whole layer before:', 'almost nothing fuses.'])
    dag(d, x[1] + 46, top2 + 62, {
        'a': (0, 0, 'polars'), 'b': (0, 104, 'pandas'), 'c': (110, 0, 'r'),
        'd': (110, 104, 'rust'), 'e': (220, 0, 'rust'), 'f': (220, 104, 'polars'),
    }, [('a', 'c'), ('a', 'd'), ('b', 'c'), ('b', 'd'), ('c', 'e'), ('c', 'f'), ('d', 'e'),
        ('d', 'f')])
    panel(d, x[2], top2, w3, h, 'Thicket',
          ['Edges that skip around: only the', 'stretches that are lines fuse.'])
    dag(d, x[2] + 24, top2 + 56, {
        'a': (0, 0, 'pandas'), 'b': (0, 120, 'rust'), 'c': (72, 60, 'polars'),
        'd': (144, 0, 'r'), 'e': (144, 120, 'rust'), 'f': (216, 120, 'rust'),
        'g': (216, 0, 'pandas'),
    }, [('a', 'c'), ('b', 'c'), ('a', 'd'), ('c', 'd'), ('c', 'e'), ('e', 'f'), ('d', 'g')],
        [['d', 'g'], ['e', 'f']])
    legend(d, 24, 732)
    return d


def language_mixes() -> Diagram:
    """The same Line with its blocks' languages mixed four ways."""
    d = Diagram(1080, 492, 'One Line of six blocks with four mixes of languages')
    d.text(24, 34, 'Each language switch converts the data and starts a process: group the '
           'blocks of each language', size=15, weight='600', muted=False)
    rows = [
        ('One language', ['rust'] * 6, '1 compiled chain, no switch'),
        ('Islands of three', ['pandas'] * 3 + ['rust'] * 3, '2 chains, 1 switch'),
        ('Random', ['polars', 'polars', 'r', 'rust', 'rust', 'pandas'], '4 chains, 3 switches'),
        ('Alternate', ['pandas', 'r'] * 3, '6 chains, 5 switches'),
    ]
    step, x0 = 98, 230
    for index, (label, languages, summary) in enumerate(rows):
        y = 78 + index * 96
        d.text(24, y + 26, label, size=14, weight='600', muted=False)
        runs, start = [], 0
        for i in range(1, len(languages) + 1):
            if i == len(languages) or languages[i] != languages[start]:
                runs.append((start, i - 1))
                start = i
        for first, last in runs:
            d.stage(x0 + first * step - 9, y - 9, (last - first) * step + CHIP + 18,
                    CHIP + 18, COLORS[languages[first]])
        for i in range(len(languages) - 1):
            d.arrow([(x0 + i * step + CHIP + 4, y + CHIP / 2),
                     (x0 + (i + 1) * step - 4, y + CHIP / 2)])
            if languages[i] != languages[i + 1]:
                d.badge(x0 + i * step + CHIP + (step - CHIP) / 2, y + CHIP + 22, 'Arrow',
                        COLORS['apachearrow'])
        for i, slug in enumerate(languages):
            d.chip(x0 + i * step, y, slug, CHIP)
        d.text(x0 + 5 * step + CHIP + 34, y + 26, summary, size=13, weight='600',
               muted=False)
    d.text(24, 478, 'A colored frame is one chain: one process for pandas and Polars, one '
           'R session, one compiled Rust program.', size=12)
    return d


def playground_shapes() -> Diagram:
    """The playground's Diamond, Broadcast and Merge pipelines, block by block."""
    d = Diagram(1080, 820, "The playground's shape pipelines and the stages their runs form")
    d.text(24, 34, 'Three playground pipelines, one per branching shape', size=15,
           weight='600', muted=False)
    w, h = 230, 56

    def box(x, y, name, slug, sub):
        d.node(x, y, w, h, name, slug, sub)

    def link(a, b):
        d.curve((a[0] + w, a[1] + h / 2), (b[0] - 2, b[1] + h / 2))

    d.text(24, 72, 'shape_diamond_store_scorecard: a Diamond', size=13.5, weight='600',
           muted=False)
    load, rev, mar = (24, 150), (290, 92), (290, 208)
    join, out = (556, 150), (822, 150)
    d.stage(join[0] - 12, join[1] - 14, out[0] - join[0] + w + 24, h + 28, FUSED,
            'join and export fuse into one stage')
    box(*load, 'diamond_order_lines', 'pandas', 'loads order lines')
    box(*rev, 'diamond_store_revenue', 'polars', 'revenue per store')
    box(*mar, 'diamond_store_margin', 'r', 'margin per store')
    box(*join, 'diamond_store_scorecard', 'rust', 'joins the branches')
    box(*out, 'diamond_export_store_…', 'pandas', 'writes the scorecard')
    for a, b in ((load, rev), (load, mar), (rev, join), (mar, join), (join, out)):
        link(a, b)

    d.text(24, 318, 'shape_broadcast_web_events: a Broadcast', size=13.5, weight='600',
           muted=False)
    events = (24, 418)
    mix, dwell, hourly = (330, 342), (330, 418), (330, 494)
    box(*events, 'broadcast_web_events', 'pandas', 'one table, read once')
    box(*mix, 'broadcast_device_mix', 'polars', 'device mix')
    box(*dwell, 'broadcast_page_dwell', 'r', 'dwell time per page')
    box(*hourly, 'broadcast_hourly_load', 'rust', 'load per hour')
    for target in (mix, dwell, hourly):
        link(events, target)
    d.text(620, 438, 'Each summary is a stage of its own and', size=12)
    d.text(620, 456, 'reads the stored table; the three run side by side.', size=12)

    d.text(24, 588, 'shape_merge_customer_signals: a Merge', size=13.5, weight='600',
           muted=False)
    cust, orders, web = (24, 612), (24, 688), (24, 764 - 10)
    join2, out2 = (330, 688), (620, 688)
    d.stage(join2[0] - 12, join2[1] - 14, out2[0] - join2[0] + w + 24, h + 28, FUSED,
            'join and export fuse into one stage')
    box(*cust, 'merge_customers', 'pandas', 'customers')
    box(*orders, 'merge_order_stats', 'rust', 'order stats, binary COPY')
    box(*web, 'merge_web_stats', 'polars', 'web stats')
    box(*join2, 'merge_customer_signals', 'rust', 'joins the three')
    box(*out2, 'merge_export_customer_…', 'pandas', 'writes the signals')
    for source in (cust, orders, web):
        link(source, join2)
    link(join2, out2)
    return d


DIAGRAMS = {
    'pipeline-shapes': pipeline_shapes,
    'language-mixes': language_mixes,
    'playground-shapes': playground_shapes,
    'single-vs-polyglot': single_vs_polyglot,
    'tutorial-pipeline-python': tutorial_pipeline_python,
    'r-packages': r_packages,
    'rust-block': rust_block,
    'r-block': r_block,
    'tutorial-pipeline': tutorial_pipeline,
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
