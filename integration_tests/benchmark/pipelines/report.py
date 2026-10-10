"""
The report of a pipeline benchmark (run.py): statistics over the measured rounds, a
Markdown report, a self-contained HTML report with SVG charts, and the charts as SVG
files for the documentation.

    uv run python -m integration_tests.benchmark.pipelines.report <out directory>
"""
import html
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

MODE_LABELS = {
    'cli': 'mage run',
    'scheduler': 'Scheduler, block by block',
    'fused': 'Scheduler, fused',
    'service': 'Exported service',
}
PIPELINE_LABELS = {
    'pandas': 'pandas',
    'polars': 'Polars',
    'r': 'R (dplyr)',
    'rust': 'Rust',
}
COLORS = ['#4C6EF5', '#12B886', '#F59F00', '#E8590C', '#AE3EC9', '#1098AD']


def pipeline_label(name: str) -> str:
    if name in PIPELINE_LABELS:
        return PIPELINE_LABELS[name]
    if name.startswith('rust_') and name.endswith('_of_5'):
        return f'{name.split("_")[1]} Rust + {5 - int(name.split("_")[1])} pandas'
    return name


def rust_blocks(name: str) -> Optional[int]:
    """Position in the staggered series: Rust blocks out of 5, pandas being 0."""
    if name == 'pandas':
        return 0
    if name == 'rust':
        return 5
    if name.startswith('rust_') and name.endswith('_of_5'):
        return int(name.split('_')[1])
    return None


def _quantile(values: List[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def summarize(results: Dict) -> Dict[Tuple[int, str, str], Dict]:
    """(rows, mode, pipeline) to medians and spreads over the measured rounds."""
    groups: Dict[Tuple[int, str, str], List[Dict]] = {}
    for run in results['runs']:
        if run.get('round', 0) < 1:
            continue
        groups.setdefault((run['rows'], run['mode'], run['pipeline']), []).append(run)
    summary = {}
    for key, runs in groups.items():
        ok = [r for r in runs if r['returncode'] == 0]
        entry = dict(runs=len(runs), failed=len(runs) - len(ok),
                     checks=sorted({r.get('check') or 'failed' for r in runs}))
        for metric in ('seconds', 'cpu_seconds', 'peak_rss_bytes'):
            values = [r[metric] for r in ok]
            if values:
                entry[metric] = dict(
                    median=statistics.median(values), min=min(values), max=max(values),
                    q1=_quantile(values, 0.25), q3=_quantile(values, 0.75),
                )
        summary[key] = entry
    return summary


def _value(summary, key, metric, field='median') -> Optional[float]:
    entry = summary.get(key) or {}
    return (entry.get(metric) or {}).get(field)


# ---- SVG charts -----------------------------------------------------------------------

def _nice_max(value: float) -> float:
    if value <= 0:
        return 1.0
    for exponent in range(-3, 7):
        for step in (1, 2, 2.5, 5):
            candidate = step * 10 ** exponent
            if candidate >= value:
                return candidate
    return value


def _log_ticks(low: float, high: float) -> List[float]:
    ticks = []
    exponent = math.floor(math.log10(low))
    while 10 ** exponent <= high * 1.0001:
        for step in (1, 2, 5):
            value = step * 10 ** exponent
            if low * 0.9999 <= value <= high * 1.0001:
                ticks.append(value)
        exponent += 1
    return ticks


def bar_chart(title: str, groups: List[str], series: List[Tuple[str, List[Optional[float]],
              List[Optional[Tuple[float, float]]]]], unit: str, log: bool = False) -> str:
    """
    Grouped bars (median) with whiskers (min to max); text uses currentColor. A log axis
    shows values that differ by orders of magnitude side by side.
    """
    width, height = 900, 380
    left, right, top, bottom = 64, 16, 40, 92
    plot_w, plot_h = width - left - right, height - top - bottom
    values_all = [hi for _, _, spreads in series for s in spreads if s for hi in s] \
        + [v for _, values, _ in series for v in values if v is not None]
    peak = max(values_all + [0])
    if log and values_all and min(values_all) > 0:
        floor = 10 ** math.floor(math.log10(min(values_all)))
        ceiling = 10 ** math.ceil(math.log10(peak * 1.0001))

        def scale(value):
            return plot_h * (math.log10(max(value, floor)) - math.log10(floor)) / (
                math.log10(ceiling) - math.log10(floor))

        ticks = _log_ticks(floor, ceiling)
    else:
        ceiling = _nice_max(peak * 1.05)

        def scale(value):
            return plot_h * value / ceiling

        ticks = [ceiling * tick / 5 for tick in range(6)]
    group_w = plot_w / max(len(groups), 1)
    bar_w = group_w * 0.8 / max(len(series), 1)
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
             f'role="img" aria-label="{html.escape(title)}" font-family="system-ui, sans-serif" '
             f'font-size="12" fill="currentColor">',
             f'<text x="{left}" y="22" font-size="15" font-weight="600">'
             f'{html.escape(title)}</text>']
    for value in ticks:
        y = top + plot_h - scale(value)
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" '
                     f'stroke="currentColor" stroke-opacity="0.15"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end">'
                     f'{value:g}</text>')
    axis = f'{unit}, log scale' if log else unit
    parts.append(f'<text x="14" y="{top + plot_h / 2}" transform="rotate(-90 14 '
                 f'{top + plot_h / 2})" text-anchor="middle">{html.escape(axis)}</text>')
    for g, group in enumerate(groups):
        x0 = left + g * group_w + group_w * 0.1
        for s, (_, values, spreads) in enumerate(series):
            value = values[g]
            if value is None:
                continue
            x = x0 + s * bar_w
            h = scale(value)
            parts.append(f'<rect x="{x:.1f}" y="{top + plot_h - h:.1f}" width="{bar_w * 0.9:.1f}" '
                         f'height="{h:.1f}" fill="{COLORS[s % len(COLORS)]}" rx="2">'
                         f'<title>{html.escape(series[s][0])}, {html.escape(group)}: '
                         f'{value:.3g} {html.escape(unit)}</title></rect>')
            spread = spreads[g]
            if spread:
                cx = x + bar_w * 0.45
                y_lo = top + plot_h - scale(spread[0])
                y_hi = top + plot_h - scale(spread[1])
                parts.append(f'<line x1="{cx:.1f}" x2="{cx:.1f}" y1="{y_lo:.1f}" '
                             f'y2="{y_hi:.1f}" stroke="currentColor" stroke-opacity="0.6"/>')
        label_x = left + g * group_w + group_w / 2
        if len(groups) <= 5:
            parts.append(f'<text x="{label_x:.1f}" y="{top + plot_h + 20}" '
                         f'text-anchor="middle">{html.escape(group)}</text>')
        else:
            parts.append(f'<text x="{label_x:.1f}" y="{top + plot_h + 18}" text-anchor="end" '
                         f'transform="rotate(-25 {label_x:.1f} {top + plot_h + 18})">'
                         f'{html.escape(group)}</text>')
    legend_x = left
    for s, (name, _, _) in enumerate(series):
        parts.append(f'<rect x="{legend_x}" y="{height - 18}" width="12" height="12" '
                     f'fill="{COLORS[s % len(COLORS)]}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 18}" y="{height - 8}">{html.escape(name)}</text>')
        legend_x += 30 + 7.2 * len(name)
    parts.append('</svg>')
    return '\n'.join(parts)


def line_chart(title: str, xs: List[int], series: List[Tuple[str, List[Optional[float]]]],
               unit: str, x_label: str) -> str:
    width, height = 900, 340
    left, right, top, bottom = 64, 16, 40, 72
    plot_w, plot_h = width - left - right, height - top - bottom
    peak = max([v for _, values in series for v in values if v is not None] + [0])
    ceiling = _nice_max(peak * 1.05)
    span = max(xs) - min(xs) or 1

    def point(x, v):
        return (left + plot_w * (x - min(xs)) / span, top + plot_h - plot_h * v / ceiling)

    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
             f'role="img" aria-label="{html.escape(title)}" font-family="system-ui, sans-serif" '
             f'font-size="12" fill="currentColor">',
             f'<text x="{left}" y="22" font-size="15" font-weight="600">'
             f'{html.escape(title)}</text>']
    for tick in range(6):
        value = ceiling * tick / 5
        y = top + plot_h - plot_h * tick / 5
        parts.append(f'<line x1="{left}" x2="{width - right}" y1="{y:.1f}" y2="{y:.1f}" '
                     f'stroke="currentColor" stroke-opacity="0.15"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end">{value:g}</text>')
    for x in xs:
        px, _ = point(x, 0)
        parts.append(f'<text x="{px:.1f}" y="{top + plot_h + 18}" text-anchor="middle">{x}</text>')
    parts.append(f'<text x="{left + plot_w / 2}" y="{top + plot_h + 38}" text-anchor="middle">'
                 f'{html.escape(x_label)}</text>')
    parts.append(f'<text x="14" y="{top + plot_h / 2}" transform="rotate(-90 14 '
                 f'{top + plot_h / 2})" text-anchor="middle">{html.escape(unit)}</text>')
    for s, (name, values) in enumerate(series):
        points = [point(x, v) for x, v in zip(xs, values) if v is not None]
        if not points:
            continue
        color = COLORS[s % len(COLORS)]
        path = ' '.join(f'{x:.1f},{y:.1f}' for x, y in points)
        parts.append(f'<polyline points="{path}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for (x, y), v in zip(points, [v for v in values if v is not None]):
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}">'
                         f'<title>{html.escape(name)}: {v:.3g} {html.escape(unit)}</title>'
                         '</circle>')
    legend_x = left
    for s, (name, _) in enumerate(series):
        parts.append(f'<rect x="{legend_x}" y="{height - 18}" width="12" height="12" '
                     f'fill="{COLORS[s % len(COLORS)]}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 18}" y="{height - 8}">{html.escape(name)}</text>')
        legend_x += 30 + 7.2 * len(name)
    parts.append('</svg>')
    return '\n'.join(parts)


# ---- the report -----------------------------------------------------------------------

METRICS = (
    ('seconds', 'Run time', 's', 1),
    ('cpu_seconds', 'CPU time', 'CPU s', 1),
    ('peak_rss_bytes', 'Peak memory', 'MB', 2**20),
)


def charts(results: Dict, summary: Dict) -> Dict[str, str]:
    settings = results['settings']
    pipelines = list(settings['pipelines'])
    language_pipelines = [p for p in pipelines if p in PIPELINE_LABELS]
    staggered = sorted([p for p in pipelines if rust_blocks(p) is not None], key=rust_blocks)
    modes = settings['modes']
    out = {}
    for rows in settings['rows']:
        for metric, label, unit, scale in METRICS:
            series = []
            for pipeline in language_pipelines:
                values, spreads = [], []
                for mode in modes:
                    key = (rows, mode, pipeline)
                    median = _value(summary, key, metric)
                    values.append(None if median is None else median / scale)
                    lo = _value(summary, key, metric, 'min')
                    hi = _value(summary, key, metric, 'max')
                    spreads.append(None if lo is None else (lo / scale, hi / scale))
                series.append((pipeline_label(pipeline), values, spreads))
            out[f'languages_{metric}_{rows}'] = bar_chart(
                f'{label}, {rows:,} rows (median; lines: min to max)',
                [MODE_LABELS[m] for m in modes], series, unit,
                log=metric != 'peak_rss_bytes',
            )
        if len(staggered) > 2 and 'pandas' in staggered:
            series = []
            for mode in modes:
                base = _value(summary, (rows, mode, 'pandas'), 'seconds')
                series.append((MODE_LABELS[mode], [
                    None if base is None or _value(summary, (rows, mode, p), 'seconds') is None
                    else _value(summary, (rows, mode, p), 'seconds') / base
                    for p in staggered
                ]))
            out[f'staggered_seconds_{rows}'] = line_chart(
                f'Replacing pandas blocks with Rust, {rows:,} rows '
                f'(median run time; 1 = the pandas pipeline)',
                [rust_blocks(p) for p in staggered], series, 'relative to pandas',
                'Rust blocks out of 5',
            )
    return out


def _cell(summary, key, metric, scale=1, digits=2) -> str:
    median = _value(summary, key, metric)
    if median is None:
        entry = summary.get(key)
        return 'failed' if entry and entry['failed'] else '-'
    q1, q3 = _value(summary, key, metric, 'q1'), _value(summary, key, metric, 'q3')
    return f'{median / scale:.{digits}f} ({q1 / scale:.{digits}f} to {q3 / scale:.{digits}f})'


def markdown(results: Dict, summary: Dict) -> str:
    settings, machine = results['settings'], results['machine']
    lines = [
        '# Pipeline benchmark',
        '',
        f'Run from {results["started_at"]} to {results.get("finished_at", "?")}, on '
        f'{machine["processor"]} ({machine["cpus"]} cores, {machine["memory_gb"]} GB), '
        f'{machine["platform"]}.',
        f'Python {machine["python"]}, '
        + ', '.join(f'{k} {v}' for k, v in machine['packages'].items() if v)
        + f'; {machine["r"]}; {machine["rustc"]}.',
        f'Commit {machine["git_commit"]}' + (' with local changes' if machine['git_dirty'] else '')
        + f'. {settings["repeats"]} measured rounds after a warm-up round; '
        f'seed {settings["seed"]}.',
        '',
        'Cells are the median, with the interquartile range in parentheses.',
    ]
    for rows in settings['rows']:
        for metric, label, unit, scale in METRICS:
            lines += ['', f'## {label}, {rows:,} rows ({unit})', '',
                      '| Pipeline | '
                      + ' | '.join(MODE_LABELS[m] for m in settings['modes']) + ' |',
                      '| --- |' + ' --- |' * len(settings['modes'])]
            for pipeline in settings['pipelines']:
                cells = [_cell(summary, (rows, mode, pipeline), metric, scale,
                               0 if metric == 'peak_rss_bytes' else 2)
                         for mode in settings['modes']]
                lines.append(f'| {pipeline_label(pipeline)} | ' + ' | '.join(cells) + ' |')
    problems = [
        f'- {pipeline_label(p)}, {MODE_LABELS[m]}, {rows:,} rows: {", ".join(e["checks"])}'
        + (f'; {e["failed"]} failed runs' if e['failed'] else '')
        for (rows, m, p), e in sorted(summary.items())
        if e['failed'] or set(e['checks']) - {'same', 'close', 'reference'}
    ]
    lines += ['', '## Output check', '',
              'Every run\'s summary was compared with the pandas pipeline\'s.', '']
    lines += problems or ['All runs wrote the same summary (equal, or within 1e-9 relative).']
    return '\n'.join(lines) + '\n'


PAGE = '''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline Benchmark</title>
<style>
:root {{ --bg: #ffffff; --fg: #1f2328; --muted: #59636e; --line: #d1d9e0; --card: #f6f8fa; }}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{ --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1;
    --line: #3d444d; --card: #151b23; }}
}}
:root[data-theme="dark"] {{ --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #3d444d;
  --card: #151b23; }}
body {{ background: var(--bg); color: var(--fg); margin: 0;
  font: 15px/1.55 system-ui, -apple-system, Segoe UI, sans-serif; }}
main {{ max-width: 980px; margin: 0 auto; padding: 24px 16px 64px; }}
h1 {{ font-size: 28px; margin: 8px 0 4px; }}
h2 {{ font-size: 20px; margin-top: 36px; border-bottom: 1px solid var(--line);
  padding-bottom: 6px; }}
p.meta {{ color: var(--muted); font-size: 13px; }}
figure {{ margin: 18px 0; background: var(--card); border: 1px solid var(--line);
  border-radius: 8px; padding: 12px; }}
figure svg {{ width: 100%; height: auto; display: block; }}
.table {{ overflow-x: auto; }}
table {{ border-collapse: collapse; font-size: 13px; min-width: 100%; }}
th, td {{ border-bottom: 1px solid var(--line); padding: 6px 10px; text-align: right;
  white-space: nowrap; }}
th:first-child, td:first-child {{ text-align: left; }}
code {{ font-size: 13px; }}
</style>
</head>
<body>
<main>
{body}
</main>
</body>
</html>
'''


def html_report(results: Dict, summary: Dict, svgs: Dict[str, str], notes: str = '') -> str:
    settings, machine = results['settings'], results['machine']
    body = [
        '<h1>Pipeline benchmark</h1>',
        f'<p class="meta">{html.escape(machine["processor"] or "")}, {machine["cpus"]} cores, '
        f'{machine["memory_gb"]} GB; {html.escape(machine["platform"])}; Python '
        f'{machine["python"]}, '
        + html.escape(', '.join(f'{k} {v}' for k, v in machine['packages'].items() if v))
        + f'; {html.escape(machine["r"] or "R ?")}; {html.escape(machine["rustc"] or "")}. '
        f'Commit <code>{(machine["git_commit"] or "")[:10]}</code>'
        + (' with local changes' if machine['git_dirty'] else '')
        + f'. {settings["repeats"]} measured rounds after a warm-up.</p>',
    ]
    if notes:
        body.append(notes)
    for rows in settings['rows']:
        body.append(f'<h2>{rows:,} rows</h2>')
        for metric, _, _, _ in METRICS:
            svg = svgs.get(f'languages_{metric}_{rows}')
            if svg:
                body.append(f'<figure>{svg}</figure>')
        if svgs.get(f'staggered_seconds_{rows}'):
            body.append(f'<figure>{svgs[f"staggered_seconds_{rows}"]}</figure>')
        for metric, label, unit, scale in METRICS:
            header = ''.join(f'<th>{html.escape(MODE_LABELS[m])}</th>' for m in settings['modes'])
            rows_html = ''.join(
                f'<tr><td>{html.escape(pipeline_label(p))}</td>' + ''.join(
                    '<td>' + html.escape(_cell(summary, (rows, m, p), metric, scale,
                                               0 if metric == 'peak_rss_bytes' else 2))
                    + '</td>'
                    for m in settings['modes']) + '</tr>'
                for p in settings['pipelines']
            )
            body.append(f'<h3>{label} ({unit}): median (interquartile range)</h3>'
                        f'<div class="table"><table><tr><th>Pipeline</th>{header}</tr>'
                        f'{rows_html}</table></div>')
    return PAGE.format(body='\n'.join(body))


def write(results: Dict, out: Path, notes: str = '') -> Path:
    summary = summarize(results)
    svgs = charts(results, summary)
    chart_dir = out / 'charts'
    chart_dir.mkdir(exist_ok=True)
    for name, svg in svgs.items():
        (chart_dir / f'{name}.svg').write_text(svg)
    (out / 'summary.json').write_text(json.dumps(
        [dict(rows=k[0], mode=k[1], pipeline=k[2], **v) for k, v in sorted(summary.items())],
        indent=2,
    ))
    (out / 'report.md').write_text(markdown(results, summary))
    path = out / 'report.html'
    path.write_text(html_report(results, summary, svgs, notes))
    return path


if __name__ == '__main__':
    directory = Path(sys.argv[1])
    print(write(json.loads((directory / 'results.json').read_text()), directory))
