#!/usr/bin/env python3
"""
Analysis of the compilation study's measurements (integration_tests/topologies/results).
Prints the tables of docs/development/compilation-study.md and writes its charts to
docs/images/study. Runs with numpy and scipy:

    uv run python scripts/study/analyze.py

Statistics:
- medians of warm runs (the first, cold run of each pipeline builds caches and is excluded)
  with a bootstrap 95% interval (5,000 resamples, seed 7);
- the fused speedup is the ratio of the block by block median to the fused median, with a
  bootstrap interval of that ratio; Mann-Whitney U tests whether the two modes differ;
- costs per block and per language boundary are least squares slopes with a 95% interval
  from the t distribution; Spearman's rho checks that the relation is monotonic.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
from scipy import stats

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts' / 'study'))
import charts  # noqa: E402

RESULTS = ROOT / 'integration_tests' / 'topologies' / 'results'
IMAGES = ROOT / 'docs' / 'images' / 'study'
RNG = np.random.default_rng(7)
BOOT = 5000


def load(experiment: str) -> List[Dict]:
    """Every run of an experiment, from all sessions (<experiment>-<session>.jsonl)."""
    rows = []
    for path in sorted(RESULTS.glob(f'{experiment}-*.jsonl')):
        rows += [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return rows


def flagged(row: Dict) -> bool:
    """A run that started on a busy or hot machine: excluded from timings, counted."""
    before = row.get('before') or {}
    return before.get('quiet') is False


def warm(rows: Iterable[Dict]) -> List[Dict]:
    rows = list(rows)
    kept = [r for r in rows if not r.get('cold') and not r.get('problems') and not flagged(r)]
    excluded = dict(cold=sum(1 for r in rows if r.get('cold')),
                    failed=sum(1 for r in rows if r.get('problems')),
                    busy=sum(1 for r in rows if flagged(r)))
    EXCLUDED[rows[0]['experiment'] if rows else '-'] = excluded
    return kept


EXCLUDED: Dict[str, Dict[str, int]] = {}


def median_ci(values: List[float]) -> Tuple[float, float, float]:
    values = np.asarray(values, dtype=float)
    if len(values) == 1:
        return float(values[0]), float(values[0]), float(values[0])
    samples = RNG.choice(values, size=(BOOT, len(values)), replace=True)
    medians = np.median(samples, axis=1)
    low, high = np.percentile(medians, [2.5, 97.5])
    return float(np.median(values)), float(low), float(high)


def ratio_ci(slow: List[float], fast: List[float]) -> Tuple[float, float, float]:
    slow, fast = np.asarray(slow, dtype=float), np.asarray(fast, dtype=float)
    ratio = float(np.median(slow) / np.median(fast))
    a = np.median(RNG.choice(slow, size=(BOOT, len(slow))), axis=1)
    b = np.median(RNG.choice(fast, size=(BOOT, len(fast))), axis=1)
    low, high = np.percentile(a / b, [2.5, 97.5])
    return ratio, float(low), float(high)


def slope_ci(x: List[float], y: List[float]) -> Dict[str, float]:
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    fit = stats.linregress(x, y)
    t = stats.t.ppf(0.975, len(x) - 2) if len(x) > 2 else float('nan')
    rho = stats.spearmanr(x, y)
    return dict(slope=fit.slope, slope_low=fit.slope - t * fit.stderr,
                slope_high=fit.slope + t * fit.stderr, intercept=fit.intercept,
                r2=fit.rvalue ** 2, spearman=float(rho.statistic), spearman_p=float(rho.pvalue),
                n=len(x))


def mann_whitney(a: List[float], b: List[float]) -> float:
    if len(a) < 2 or len(b) < 2:
        return float('nan')
    return float(stats.mannwhitneyu(a, b, alternative='two-sided').pvalue)


def table(header: List[str], rows: List[List]) -> str:
    out = ['| ' + ' | '.join(header) + ' |', '|' + '---|' * len(header)]
    for row in rows:
        out.append('| ' + ' | '.join(str(c) for c in row) + ' |')
    return '\n'.join(out)


def fmt(value: float, digits: int = 2) -> str:
    return f'{value:.{digits}f}'


# Experiments.

def correctness():
    rows = load('correctness')
    if not rows:
        return
    print('## Correctness\n')
    by = defaultdict(lambda: [0, 0])
    failures = []
    for r in rows:
        key = (r['topology'], r['pattern'], r['rows'])
        ok = not r['problems'] and r['status'] == 'completed'
        by[key][0 if ok else 1] += 1
        if not ok:
            failures.append(r)
    passed = sum(v[0] for v in by.values())
    total = sum(sum(v) for v in by.values())
    print(f'{passed} of {total} runs passed.\n')
    if failures:
        print(table(['topology', 'languages', 'rows', 'mode', 'problem'], [
            [f['topology'], f['pattern'], f['rows'], f['mode'],
             '; '.join(f'{k.split("_")[0]}: {v}' for k, v in f['problems'].items())[:160]
             or ', '.join(f'{k}={v}' for k, v in f['statuses'].items() if v != 'completed')]
            for f in failures]))
        print()


def chain_length():
    rows = warm(load('chain_length'))
    if not rows:
        return
    print('## Chain length\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['language'], r['mode'], r['length'])].append(r['seconds'])
    languages = sorted({k[0] for k in groups})
    lengths = sorted({k[2] for k in groups})
    lines, out = {}, []
    for language in languages:
        for mode in ('off', 'chains'):
            pts, xs, ys = [], [], []
            for length in lengths:
                values = groups.get((language, mode, length))
                if not values:
                    continue
                med, low, high = median_ci(values)
                pts.append((length, med, low, high))
                xs += [length] * len(values)
                ys += values
            if pts:
                lines[f'{language} {"fused" if mode == "chains" else "block by block"}'] = pts
                fit = slope_ci(xs, ys)
                out.append([language, mode, fmt(fit['intercept']),
                            f"{fmt(fit['slope'], 3)} [{fmt(fit['slope_low'], 3)}, "
                            f"{fmt(fit['slope_high'], 3)}]", fmt(fit['r2'], 3)])
    print(table(['language', 'mode', 'fixed s', 's per block [95% CI]', 'R²'], out))
    print()
    speed = []
    for language in languages:
        for length in lengths:
            off = groups.get((language, 'off', length))
            fused = groups.get((language, 'chains', length))
            if off and fused:
                ratio, low, high = ratio_ci(off, fused)
                speed.append([language, length, f'{fmt(ratio)} [{fmt(low)}, {fmt(high)}]',
                              f'{mann_whitney(off, fused):.3g}'])
    print(table(['language', 'blocks', 'fused speedup [95% CI]', 'Mann-Whitney p'], speed))
    print()
    charts.lines(IMAGES / 'chain-length', 'Run time by chain length (10,000 rows)', lines,
                 xlabel='blocks in the chain', ylabel='seconds per run (median, 95% CI)')


def switches():
    rows = warm(load('switches'))
    if not rows:
        return
    print('## Language boundaries\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['pair'], r['mode'])].append((r['switches'], r['seconds']))
    out, lines = [], {}
    for (pair, mode), values in sorted(groups.items()):
        fit = slope_ci([v[0] for v in values], [v[1] for v in values])
        out.append([pair, mode, f"{fmt(fit['slope'], 3)} [{fmt(fit['slope_low'], 3)}, "
                                f"{fmt(fit['slope_high'], 3)}]",
                    fmt(fit['spearman'], 2), f"{fit['spearman_p']:.3g}"])
        by_x = defaultdict(list)
        for x, y in values:
            by_x[x].append(y)
        lines[f'{pair} {"fused" if mode == "chains" else "block by block"}'] = [
            (x, *median_ci(ys)) for x, ys in sorted(by_x.items())]
    print(table(['languages', 'mode', 's per boundary [95% CI]', 'Spearman rho', 'p'], out))
    print()
    charts.lines(IMAGES / 'language-boundaries', 'Run time of a 6-block chain by language switches',
                 lines, xlabel='switches between languages', ylabel='seconds per run')


def data_size():
    rows = warm(load('data_size'))
    if not rows:
        return
    print('## Data size\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['language'], r['mode'], r['rows'])].append(r['seconds'])
    out, lines = [], {}
    for (language, mode, size), values in sorted(groups.items()):
        med, low, high = median_ci(values)
        out.append([language, mode, f'{size:,}', f'{fmt(med)} [{fmt(low)}, {fmt(high)}]',
                    f'{size / med:,.0f}'])
        lines.setdefault(f'{language} {"fused" if mode == "chains" else "block by block"}', []
                         ).append((float(np.log10(size)), med, low, high))
    print(table(['language', 'mode', 'rows', 'seconds [95% CI]', 'rows per second'], out))
    print()
    charts.lines(IMAGES / 'data-size', 'Run time of a 4-block chain by input rows', lines,
                 xlabel='log10(rows)', ylabel='seconds per run')


def width():
    rows = warm(load('width'))
    if not rows:
        return
    print('## Branching\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['language'], r['shape'], r['mode'], r['width'])].append(r['seconds'])
    out = []
    for (language, shape, mode, w), values in sorted(groups.items()):
        med, low, high = median_ci(values)
        out.append([language, shape, mode, w, f'{fmt(med)} [{fmt(low)}, {fmt(high)}]'])
    print(table(['language', 'shape', 'mode', 'width', 'seconds [95% CI]'], out))
    print()


def faults():
    rows = load('faults')
    if not rows:
        return
    print('## Failures\n')
    by = defaultdict(dict)
    for r in rows:
        by[(r['topology'], r['language'], r['fault'])][r['mode']] = r['statuses']
    out = []
    for (topology, language, fault), modes in sorted(by.items()):
        same = modes.get('off') == modes.get('chains')
        out.append([topology, language, fault,
                    ', '.join(f'{k}:{v}' for k, v in sorted(modes.get('off', {}).items())),
                    'same' if same else 'differs: ' + ', '.join(
                        f'{k}:{v}' for k, v in sorted(modes.get('chains', {}).items()))])
    print(table(['topology', 'language', 'fault', 'block by block (node:status)',
                 'fused'], out))
    print()


def types():
    rows = [r for r in load('types') if r.get('columns')]
    if not rows:
        return
    print('## Data types\n')
    data = {f"{r['language']}/{r['mode']}": r['columns'] for r in rows}
    columns = sorted(data)
    names = sorted({c for key in columns for c in data[key]})
    cells = {}
    for key in columns:
        for column, entry in data[key].items():
            if entry.get('result') == 'same':
                cells[(column, key)] = (entry['dtype'][1][:14], 'good')
            elif entry.get('result') == 'missing':
                cells[(column, key)] = ('missing', 'bad')
            else:
                cells[(column, key)] = (entry.get('dtype', ['', ''])[1][:14] or 'changed', 'warn')
    charts.grid(IMAGES / 'data-types', 'Column types after a block in another language',
                names, columns, cells)
    print(table(['column'] + columns, [[n] + [cells.get((n, c), ('', ''))[0] for c in columns]
                                       for n in names]))
    print()


def inference():
    rows = warm(load('inference'))
    if not rows:
        return
    print('## Inference\n')
    groups = defaultdict(list)
    for r in rows:
        score = [s for b, s in (r.get('block_seconds') or {}).items() if '_score_' in b or
                 b.startswith('score_')]
        groups[(r['scorer'], r['mode'], r['rows'])].append(
            (r['seconds'], score[0] if score else None))
    out = []
    for (scorer, mode, size), values in sorted(groups.items()):
        med, low, high = median_ci([v[0] for v in values])
        block = [v[1] for v in values if v[1] is not None]
        block_med = median_ci(block)[0] if block else float('nan')
        out.append([scorer, mode, f'{size:,}', f'{fmt(med)} [{fmt(low)}, {fmt(high)}]',
                    fmt(block_med, 3), f'{size / block_med:,.0f}' if block else ''])
    print(table(['scorer', 'mode', 'rows', 'run s [95% CI]', 'scoring block s',
                 'rows per second (block)'], out))
    print()


def excluded():
    if EXCLUDED:
        print('## Runs left out of the timings\n')
        print(table(['experiment', 'cold', 'failed', 'busy machine'],
                    [[k, v['cold'], v['failed'], v['busy']] for k, v in sorted(EXCLUDED.items())]))
        print()


if __name__ == '__main__':
    IMAGES.mkdir(parents=True, exist_ok=True)
    for step in (correctness, types, chain_length, switches, data_size, width, faults,
                 inference, excluded):
        step()
