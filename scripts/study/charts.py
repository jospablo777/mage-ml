"""
Small SVG charts for the docs, without plotting libraries: grouped bars with error bars,
lines with bands, and a labelled grid. Each chart is written twice, as NAME.light.svg and
NAME.dark.svg, which the docs show with the reader's theme.
"""
from __future__ import annotations

import math
from html import escape
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

THEMES = {
    'light': dict(background='#ffffff', text='#1f2328', muted='#57606a', grid='#d8dee4',
                  axis='#8c959f'),
    'dark': dict(background='#0d1117', text='#e6edf3', muted='#9198a1', grid='#30363d',
                 axis='#6e7681'),
}
PALETTE = ['#4c78a8', '#f58518', '#54a24b', '#e45756', '#72b7b2', '#b279a2', '#9d755d']
LEVELS = {
    'light': {'good': '#d1f0d7', 'warn': '#fff1c2', 'bad': '#ffd6d6', 'none': '#eef1f4'},
    'dark': {'good': '#1f4d2b', 'warn': '#5c4a12', 'bad': '#6b1f24', 'none': '#21262d'},
}
FONT = 'font-family="-apple-system,Segoe UI,Helvetica,Arial,sans-serif"'


def _nice_max(value: float) -> float:
    if value <= 0:
        return 1.0
    exponent = math.floor(math.log10(value))
    for step in (1, 2, 2.5, 5, 10):
        top = step * 10 ** exponent
        if top >= value:
            return top
    return 10 ** (exponent + 1)


def _ticks(top: float, count: int = 5) -> List[float]:
    return [top * i / count for i in range(count + 1)]


def _fmt(value: float) -> str:
    if value == 0:
        return '0'
    if abs(value) >= 100:
        return f'{value:,.0f}'
    if abs(value) >= 10:
        return f'{value:.0f}'
    if abs(value) >= 1:
        return f'{value:.1f}'.rstrip('0').rstrip('.')
    return f'{value:.2g}'


def _write(base: Path, render) -> List[Path]:
    paths = []
    for theme in THEMES:
        path = base.with_name(f'{base.name}.{theme}.svg')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render(theme))
        paths.append(path)
    return paths


def bars(
    base: Path,
    title: str,
    categories: Sequence[str],
    series: Dict[str, Sequence[Optional[float]]],
    errors: Optional[Dict[str, Sequence[Optional[Tuple[float, float]]]]] = None,
    ylabel: str = '',
    width: int = 760,
    height: int = 360,
) -> List[Path]:
    """Grouped bars; errors gives (low, high) for each bar, such as a 95% interval."""
    errors = errors or {}
    names = list(series)
    highs = [v for name in names for v in series[name] if v is not None]
    highs += [e[1] for name in errors for e in errors[name] if e]
    top = _nice_max(max(highs) if highs else 1)
    left, right, upper, lower = 70, 20, 50, 70
    plot_w, plot_h = width - left - right, height - upper - lower
    group_w = plot_w / max(len(categories), 1)
    bar_w = group_w * 0.8 / max(len(names), 1)

    def render(theme):
        t = THEMES[theme]
        y = lambda v: upper + plot_h - plot_h * v / top  # noqa: E731
        out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
               f'viewBox="0 0 {width} {height}" {FONT} font-size="12">',
               f'<rect width="100%" height="100%" fill="{t["background"]}"/>',
               f'<text x="{left}" y="22" fill="{t["text"]}" font-size="14" '
               f'font-weight="600">{escape(title)}</text>']
        for tick in _ticks(top):
            out.append(f'<line x1="{left}" x2="{width - right}" y1="{y(tick):.1f}" '
                       f'y2="{y(tick):.1f}" stroke="{t["grid"]}"/>')
            out.append(f'<text x="{left - 6}" y="{y(tick) + 4:.1f}" fill="{t["muted"]}" '
                       f'text-anchor="end">{_fmt(tick)}</text>')
        if ylabel:
            out.append(f'<text transform="translate(16,{upper + plot_h / 2}) rotate(-90)" '
                       f'fill="{t["muted"]}" text-anchor="middle">{escape(ylabel)}</text>')
        for gi, category in enumerate(categories):
            x0 = left + gi * group_w + group_w * 0.1
            for si, name in enumerate(names):
                value = series[name][gi]
                if value is None:
                    continue
                x = x0 + si * bar_w
                out.append(f'<rect x="{x:.1f}" y="{y(value):.1f}" width="{bar_w * 0.92:.1f}" '
                           f'height="{upper + plot_h - y(value):.1f}" '
                           f'fill="{PALETTE[si % len(PALETTE)]}"/>')
                error = (errors.get(name) or [None] * len(categories))[gi]
                if error:
                    cx = x + bar_w * 0.46
                    out.append(f'<line x1="{cx:.1f}" x2="{cx:.1f}" y1="{y(error[0]):.1f}" '
                               f'y2="{y(error[1]):.1f}" stroke="{t["text"]}"/>')
                    for end in error:
                        out.append(f'<line x1="{cx - 4:.1f}" x2="{cx + 4:.1f}" '
                                   f'y1="{y(end):.1f}" y2="{y(end):.1f}" '
                                   f'stroke="{t["text"]}"/>')
            out.append(f'<text x="{left + gi * group_w + group_w / 2:.1f}" '
                       f'y="{upper + plot_h + 18}" fill="{t["text"]}" '
                       f'text-anchor="middle">{escape(category)}</text>')
        out.append(f'<line x1="{left}" x2="{width - right}" y1="{upper + plot_h}" '
                   f'y2="{upper + plot_h}" stroke="{t["axis"]}"/>')
        lx = left
        for si, name in enumerate(names):
            out.append(f'<rect x="{lx}" y="{height - 26}" width="12" height="12" '
                       f'fill="{PALETTE[si % len(PALETTE)]}"/>')
            out.append(f'<text x="{lx + 18}" y="{height - 16}" fill="{t["text"]}">'
                       f'{escape(name)}</text>')
            lx += 30 + 7 * len(name)
        out.append('</svg>')
        return '\n'.join(out) + '\n'

    return _write(base, render)


def lines(
    base: Path,
    title: str,
    series: Dict[str, Sequence[Tuple[float, float, Optional[float], Optional[float]]]],
    xlabel: str = '',
    ylabel: str = '',
    width: int = 760,
    height: int = 360,
) -> List[Path]:
    """Lines of (x, y, low, high) points; low and high draw a band when given."""
    points = [p for s in series.values() for p in s]
    xs = [p[0] for p in points]
    x_min, x_max = (min(xs), max(xs)) if xs else (0, 1)
    if x_min == x_max:
        x_max = x_min + 1
    top = _nice_max(max([p[3] if p[3] is not None else p[1] for p in points] or [1]))
    left, right, upper, lower = 70, 20, 50, 80
    plot_w, plot_h = width - left - right, height - upper - lower

    def render(theme):
        t = THEMES[theme]
        x = lambda v: left + plot_w * (v - x_min) / (x_max - x_min)  # noqa: E731
        y = lambda v: upper + plot_h - plot_h * v / top  # noqa: E731
        out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
               f'viewBox="0 0 {width} {height}" {FONT} font-size="12">',
               f'<rect width="100%" height="100%" fill="{t["background"]}"/>',
               f'<text x="{left}" y="22" fill="{t["text"]}" font-size="14" '
               f'font-weight="600">{escape(title)}</text>']
        for tick in _ticks(top):
            out.append(f'<line x1="{left}" x2="{width - right}" y1="{y(tick):.1f}" '
                       f'y2="{y(tick):.1f}" stroke="{t["grid"]}"/>')
            out.append(f'<text x="{left - 6}" y="{y(tick) + 4:.1f}" fill="{t["muted"]}" '
                       f'text-anchor="end">{_fmt(tick)}</text>')
        for value in sorted(set(xs)):
            out.append(f'<text x="{x(value):.1f}" y="{upper + plot_h + 18}" '
                       f'fill="{t["muted"]}" text-anchor="middle">{_fmt(value)}</text>')
        if xlabel:
            out.append(f'<text x="{left + plot_w / 2}" y="{upper + plot_h + 36}" '
                       f'fill="{t["muted"]}" text-anchor="middle">{escape(xlabel)}</text>')
        if ylabel:
            out.append(f'<text transform="translate(16,{upper + plot_h / 2}) rotate(-90)" '
                       f'fill="{t["muted"]}" text-anchor="middle">{escape(ylabel)}</text>')
        for si, pts in enumerate(series.values()):
            color = PALETTE[si % len(PALETTE)]
            pts = sorted(pts)
            band = [p for p in pts if p[2] is not None and p[3] is not None]
            if len(band) >= 2:
                path = ' '.join(f'{x(p[0]):.1f},{y(p[3]):.1f}' for p in band)
                path += ' ' + ' '.join(f'{x(p[0]):.1f},{y(p[2]):.1f}' for p in reversed(band))
                out.append(f'<polygon points="{path}" fill="{color}" fill-opacity="0.18"/>')
            out.append('<polyline fill="none" stroke="{}" stroke-width="2" points="{}"/>'.format(
                color, ' '.join(f'{x(p[0]):.1f},{y(p[1]):.1f}' for p in pts)))
            for p in pts:
                out.append(f'<circle cx="{x(p[0]):.1f}" cy="{y(p[1]):.1f}" r="3" '
                           f'fill="{color}"/>')
        lx = left
        for si, name in enumerate(series):
            out.append(f'<rect x="{lx}" y="{height - 22}" width="12" height="12" '
                       f'fill="{PALETTE[si % len(PALETTE)]}"/>')
            out.append(f'<text x="{lx + 18}" y="{height - 12}" fill="{t["text"]}">'
                       f'{escape(name)}</text>')
            lx += 30 + 7 * len(name)
        out.append('</svg>')
        return '\n'.join(out) + '\n'

    return _write(base, render)


def grid(
    base: Path,
    title: str,
    rows: Sequence[str],
    columns: Sequence[str],
    cells: Dict[Tuple[str, str], Tuple[str, str]],
    cell_w: int = 120,
    row_h: int = 22,
    label_w: int = 130,
) -> List[Path]:
    """A table of short labels; each cell is (text, level) with level good, warn or bad."""
    width = label_w + cell_w * len(columns) + 20
    height = 70 + row_h * (len(rows) + 1)

    def render(theme):
        t, levels = THEMES[theme], LEVELS[theme]
        out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
               f'viewBox="0 0 {width} {height}" {FONT} font-size="11">',
               f'<rect width="100%" height="100%" fill="{t["background"]}"/>',
               f'<text x="10" y="22" fill="{t["text"]}" font-size="14" '
               f'font-weight="600">{escape(title)}</text>']
        y0 = 40
        for ci, column in enumerate(columns):
            out.append(f'<text x="{label_w + ci * cell_w + cell_w / 2}" y="{y0 + 14}" '
                       f'fill="{t["text"]}" font-weight="600" text-anchor="middle">'
                       f'{escape(column)}</text>')
        for ri, row in enumerate(rows):
            y = y0 + row_h * (ri + 1)
            out.append(f'<text x="10" y="{y + 15}" fill="{t["text"]}">{escape(row)}</text>')
            for ci, column in enumerate(columns):
                text, level = cells.get((row, column), ('', 'none'))
                x = label_w + ci * cell_w
                out.append(f'<rect x="{x + 1}" y="{y + 1}" width="{cell_w - 2}" '
                           f'height="{row_h - 2}" fill="{levels.get(level, levels["none"])}"/>')
                out.append(f'<text x="{x + cell_w / 2}" y="{y + 15}" fill="{t["text"]}" '
                           f'text-anchor="middle">{escape(text)}</text>')
        out.append('</svg>')
        return '\n'.join(out) + '\n'

    return _write(base, render)
