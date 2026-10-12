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


def latest(rows: List[Dict]) -> List[Dict]:
    """The rows of the newest session, for experiments measured again after a change."""
    if not rows:
        return rows
    newest = max(r['session'] for r in rows)
    return [r for r in rows if r['session'] == newest]


INCIDENTS = json.loads((RESULTS / 'incidents.json').read_text()) \
    if (RESULTS / 'incidents.json').exists() else []


def condition(row: Dict) -> str:
    """The condition a run ran under when its session's conditions changed (incidents.json)."""
    from datetime import datetime

    for incident in INCIDENTS:
        if (incident['session'] == row.get('session')
                and incident['experiment'] == row.get('experiment')
                and incident['language'] == row.get('language')
                and datetime.fromisoformat((row.get('before') or {}).get('utc'))
                >= datetime.fromisoformat(incident['from_utc'])):
            return incident['condition']
    return 'as recorded'


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
    out, lines = [], {}
    for (language, shape, mode, w), values in sorted(groups.items()):
        med, low, high = median_ci(values)
        speedup = ''
        if mode == 'chains' and groups.get((language, shape, 'off', w)):
            ratio, r_low, r_high = ratio_ci(groups[(language, shape, 'off', w)], values)
            speedup = f'{fmt(ratio)} [{fmt(r_low)}, {fmt(r_high)}]'
        out.append([language, shape, mode, w, f'{fmt(med)} [{fmt(low)}, {fmt(high)}]',
                    speedup])
        name = {'fan_out': 'Broadcast', 'fan_in': 'Merge'}[shape]
        lines.setdefault(f'{name} {language} {"fused" if mode == "chains" else "block by block"}',
                         []).append((float(w), med, low, high))
    print(table(['language', 'shape', 'mode', 'width', 'seconds [95% CI]',
                 'block by block / fused [95% CI]'], out))
    print()
    pandas_lines = {k: v for k, v in lines.items() if ' pandas ' in k}
    if pandas_lines:
        charts.lines(IMAGES / 'branching', 'Run time of Broadcast and Merge pipelines by width '
                     '(pandas, 10,000 rows)', pandas_lines, xlabel='width (blocks)',
                     ylabel='seconds per run')


def string_dtype():
    rows = warm(load('string_dtype'))
    if not rows:
        return
    print('## pandas strings\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['strings'], r['mode'])].append(r['seconds'])
    out = []
    for (strings, mode), values in sorted(groups.items()):
        med, low, high = median_ci(values)
        out.append([strings, mode, f'{fmt(med)} [{fmt(low)}, {fmt(high)}]', len(values)])
    print(table(['strings', 'mode', 'seconds [95% CI]', 'runs'], out))
    print()
    fused = {k[0]: v for k, v in groups.items() if k[1] == 'chains'}
    if 'object' in fused and 'arrow' in fused:
        ratio, low, high = ratio_ci(fused['object'], fused['arrow'])
        print(f'Fused, object / string[pyarrow]: {fmt(ratio)} [{fmt(low)}, {fmt(high)}], '
              f'Mann-Whitney p = {mann_whitney(fused["object"], fused["arrow"]):.3g}\n')


def faults():
    rows = load('faults')
    if not rows:
        return
    # The latest session: the statuses are behaviour, not timings, and an earlier session
    # may predate a fix (1210d19d3 changed the status of a compiled chain's last block).
    latest = max(r['session'] for r in rows)
    earlier = sorted({r['session'] for r in rows} - {latest})
    rows = [r for r in rows if r['session'] == latest]
    print('## Failures\n')
    print(f'Session {latest}' + (f'; earlier sessions {", ".join(earlier)} not shown.'
                                 if earlier else '.') + '\n')
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
    largest = max(k[2] for k in groups)
    bars = {}
    for (scorer, mode, size), values in sorted(groups.items()):
        block = [v[1] for v in values if v[1] is not None]
        if size == largest and mode == 'chains' and block:
            bars[scorer] = median_ci(block)
    if bars:
        names = list(bars)
        charts.bars(IMAGES / 'inference', f'Scoring block time, {largest:,} rows, fused',
                    names, {'seconds': [bars[n][0] for n in names]},
                    {'seconds': [(bars[n][1], bars[n][2]) for n in names]},
                    ylabel='seconds', width=max(760, 60 * len(names)))


def _score_seconds(row: Dict) -> float:
    """The scoring block's own time: the run less the loader is the scheduler's share."""
    seconds = [v for b, v in (row.get('block_seconds') or {}).items()
               if b.startswith('score_') and v is not None]
    return seconds[0] if seconds else float('nan')


def _gpu_joules(row: Dict) -> float:
    """GPU energy of the run above the idle draw measured before it."""
    if row.get('gpu_j') is None or row.get('gpu_idle_w') is None:
        return float('nan')
    return row['gpu_j'] - row['gpu_idle_w'] * row['seconds']


def ml_scoring():
    all_sessions = load('ml_scoring')
    every = latest(all_sessions)
    rows = warm(every)
    if not rows:
        return
    print('## Model scoring\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['model'], r['engine'], r['language'], r['device'], r['rows'], r['mode'])
               ].append(r)
    hashes = defaultdict(set)
    for r in every:
        if r.get('sha256'):
            hashes[r['scorer'], r['rows']].add(r['sha256'])
    out = []
    largest = max(r['rows'] for r in rows)
    summary = {}
    for key, values in sorted(groups.items()):
        model, engine, language, device, size, mode = key
        block = [_score_seconds(v) for v in values]
        med, low, high = median_ci(block)
        energy = [_gpu_joules(v) for v in values]
        energy = [e for e in energy if not np.isnan(e)]
        scorer = values[0]['scorer']
        out.append([
            model, engine, language, device, f'{size:,}', mode,
            f'{fmt(med, 3)} [{fmt(low, 3)}, {fmt(high, 3)}]', f'{size / med:,.0f}',
            fmt(1000 * np.median(energy) / size, 4) if energy else '',
            f"{max(v.get('max_abs_diff', 0) for v in values):.1e}",
            'yes' if len(hashes[scorer, size]) == 1 else f'no ({len(hashes[scorer, size])})',
        ])
        if size == largest and mode == 'off':
            summary[(model, engine, language, device)] = (med, low, high)
    print(table(['model', 'engine', 'language', 'device', 'rows', 'mode',
                 'scoring s [95% CI]', 'rows per s', 'GPU J per 1k rows',
                 'max abs diff vs Python', 'same bytes every run'], out))
    print()
    # ONNX in each language, and each language's native runtime, on the CPU.
    models = sorted({k[0] for k in summary})
    for engine, name, title in (
        ('onnx', 'ml-onnx-languages', f'ONNX scoring on the CPU, {largest:,} rows'),
        ('native', 'ml-native-languages', f'Native scoring on the CPU, {largest:,} rows'),
    ):
        series, errors = {}, {}
        for language in ('python', 'r', 'rust'):
            points = [summary.get((m, engine, language, 'cpu')) for m in models]
            if not any(points):
                continue
            series[language] = [p[0] if p else None for p in points]
            errors[language] = [(p[1], p[2]) if p else None for p in points]
        if series:
            charts.bars(IMAGES / name, title, models, series, errors,
                        ylabel='scoring block seconds')
    # Fused against block by block for the two-block scoring pipelines.
    ratios = []
    for key in sorted({k[:5] for k in groups}):
        off = [r['seconds'] for r in groups.get(key + ('off',), [])]
        fused = [r['seconds'] for r in groups.get(key + ('chains',), [])]
        if len(off) > 1 and len(fused) > 1:
            ratio, low, high = ratio_ci(off, fused)
            ratios.append([*key[:4], f'{key[4]:,}', f'{fmt(ratio)} [{fmt(low)}, {fmt(high)}]',
                           f'{mann_whitney(off, fused):.3g}'])
    if ratios:
        print(table(['model', 'engine', 'language', 'device', 'rows',
                     'block by block / fused [95% CI]', 'Mann-Whitney p'], ratios))
        print()
    r_check(all_sessions, every)


def r_check(all_sessions: List[Dict], current: List[Dict]):
    """R scoring blocks before 0e7d5ac3a (rv plan in every block run) and in the latest
    session, warm runs."""
    before = [r for r in all_sessions if r['language'] == 'r' and r['engine'] == 'native'
              and r['session'] != (current[0]['session'] if current else None)
              and condition(r) == 'as recorded' and not r.get('cold') and not r['problems']]
    after = [r for r in current if r['language'] == 'r' and r['engine'] == 'native'
             and not r.get('cold') and not r['problems']]
    if not before or not after:
        return
    print('## R environment check, before and after 0e7d5ac3a\n')
    out = []
    for scorer in sorted({r['scorer'] for r in before}):
        for size in sorted({r['rows'] for r in before if r['scorer'] == scorer}):
            a = [_score_seconds(r) for r in before if r['scorer'] == scorer and r['rows'] == size]
            b = [_score_seconds(r) for r in after if r['scorer'] == scorer and r['rows'] == size]
            if len(a) > 1 and len(b) > 1:
                ma, la, ha = median_ci(a)
                mb, lb, hb = median_ci(b)
                out.append([scorer, f'{size:,}', f'{fmt(ma)} [{fmt(la)}, {fmt(ha)}]',
                            f'{fmt(mb)} [{fmt(lb)}, {fmt(hb)}]', f'{ma / mb:.1f}x',
                            f'{mann_whitney(a, b):.3g}'])
    print(table(['scorer', 'rows', 'before s [95% CI]', 'after s [95% CI]', 'faster',
                 'Mann-Whitney p'], out))
    print()


def ml_end_to_end():
    every = latest(load('ml_end_to_end'))
    rows = warm(every)
    if not rows:
        return
    print('## End to end in one language\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['model'], r['language'], r['rows'], r['mode'])].append(r['seconds'])
    out, lines = [], {}
    for (model, language, size, mode), values in sorted(groups.items()):
        if mode != 'chains':
            continue
        med, low, high = median_ci(values)
        off = groups.get((model, language, size, 'off'), [])
        ratio = ratio_ci(off, values) if len(off) > 1 and len(values) > 1 else None
        out.append([model, language, f'{size:,}', f'{fmt(med)} [{fmt(low)}, {fmt(high)}]',
                    fmt(float(np.median(off))) if off else '',
                    f'{fmt(ratio[0])} [{fmt(ratio[1])}, {fmt(ratio[2])}]' if ratio else ''])
        lines.setdefault(f'{model} {language}', []).append(
            (float(np.log10(size)), med, low, high))
    print(table(['model', 'language', 'rows', 'fused s [95% CI]', 'block by block s',
                 'block by block / fused [95% CI]'], out))
    print()
    charts.lines(IMAGES / 'ml-end-to-end', 'Scoring pipeline in one language, fused',
                 lines, xlabel='log10(rows)', ylabel='seconds per run')
    # The three languages compute the same summary: the mean score of each of 10 buckets.
    summaries = defaultdict(dict)
    for r in every:
        if r.get('summary'):
            summaries[(r['model'], r['rows'])][r['language']] = r['summary']
    agree = []
    for (model, size), by_language in sorted(summaries.items()):
        languages = sorted(by_language)
        reference = np.array(by_language[languages[0]]['mean_score'])
        worst = max(float(np.abs(np.array(by_language[lang]['mean_score']) - reference).max())
                    for lang in languages)
        counts = {lang: by_language[lang]['high'] for lang in languages}
        agree.append([model, f'{size:,}', ', '.join(languages), f'{worst:.1e}',
                      'yes' if len({tuple(v) for v in counts.values()}) == 1 else 'no'])
    print(table(['model', 'rows', 'languages', 'max difference of bucket means',
                 'same counts above 0.5'], agree))
    print()


def ml_training():
    every = latest(load('ml_training'))
    if not every:
        return
    print('## Training in Python and R\n')
    runs = warm(r for r in every if not r.get('summary'))
    out = []
    groups = defaultdict(list)
    for r in runs:
        fit = [v for b, v in (r.get('block_seconds') or {}).items()
               if b.startswith('fit_') and v is not None]
        if fit:
            groups[(r['model'], r['language'])].append(fit[0])
    for (model, language), values in sorted(groups.items()):
        med, low, high = median_ci(values)
        out.append([model, language, f'{fmt(med, 3)} [{fmt(low, 3)}, {fmt(high, 3)}]',
                    len(values)])
    print(table(['model', 'language', 'fit block s [95% CI]', 'runs'], out))
    print()
    summaries = [r for r in every if r.get('summary')]
    if summaries:
        print(table(['model', 'max abs diff', 'max rel diff', 'Python same bytes',
                     'R same bytes'], [
            [r['model'], f"{r['max_abs_diff']:.2e}", f"{r['max_rel_diff']:.2e}",
             r['python_deterministic'], r['r_deterministic']] for r in summaries]))
        print()


def journal():
    rows = warm(load('journal'))
    if not rows:
        return
    print('## Metadata database journal\n')
    groups = defaultdict(list)
    for r in rows:
        groups[(r['mode'], r['journal'])].append(r['seconds'])
    out = []
    for (mode, arm), values in sorted(groups.items()):
        values = sorted(values)
        med, low, high = median_ci(values)
        p90 = float(np.percentile(values, 90))
        boot = np.percentile(RNG.choice(values, size=(BOOT, len(values))), 90, axis=1)
        p90_low, p90_high = np.percentile(boot, [2.5, 97.5])
        out.append([mode, arm, len(values), f'{fmt(med)} [{fmt(low)}, {fmt(high)}]',
                    f'{fmt(p90)} [{fmt(p90_low)}, {fmt(p90_high)}]', fmt(max(values)),
                    sum(v > 2 * med for v in values)])
    print(table(['mode', 'journal', 'runs', 'median s [95% CI]', '90th percentile [95% CI]',
                 'max s', 'runs over 2x median'], out))
    print()
    tests = []
    for mode in sorted({k[0] for k in groups}):
        a, b = groups.get((mode, 'delete'), []), groups.get((mode, 'wal'), [])
        if len(a) > 1 and len(b) > 1:
            levene = stats.levene(a, b).pvalue
            tests.append([mode, f'{mann_whitney(a, b):.3g}', f'{levene:.3g}'])
    if tests:
        print(table(['mode', 'Mann-Whitney p (medians)', 'Levene p (spread)'], tests))
        print()


def excluded():
    if EXCLUDED:
        print('## Runs left out of the timings\n')
        print(table(['experiment', 'cold', 'failed', 'busy machine'],
                    [[k, v['cold'], v['failed'], v['busy']] for k, v in sorted(EXCLUDED.items())]))
        print()


if __name__ == '__main__':
    IMAGES.mkdir(parents=True, exist_ok=True)
    for step in (correctness, types, chain_length, switches, data_size, width,
                 string_dtype, faults,
                 inference, ml_scoring, ml_end_to_end, ml_training, journal, excluded):
        step()
