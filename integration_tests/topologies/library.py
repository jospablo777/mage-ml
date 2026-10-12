"""
Generated pipelines for the compilation study (docs/development/compilation-study.md).

A pipeline is a DAG of nodes. A root is a data loader that makes rows with id, x, g and s.
Every other node is a transformer that joins all its inputs on id (inner), sets
x = (sum of the inputs' x) * a + b, keeps g and s from its first input, and on every third
node drops the rows whose id % m == r. Each node runs in one of four languages (pandas,
Polars, R, Rust) with the same meaning, so a pandas oracle gives the expected output of
every node of any topology and language mix.
"""
from __future__ import annotations

import random
import textwrap
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

LANGUAGES = ('pandas', 'polars', 'r', 'rust')
MAGE_LANGUAGE = {'pandas': 'python', 'polars': 'python', 'r': 'r', 'rust': 'rust'}


FAULTS = ('raise', 'empty', 'missing_column')


@dataclass
class Node:
    index: int
    language: str
    upstream: List[int] = field(default_factory=list)
    # None, or one of FAULTS: raise an error, return no rows, or return no x column.
    fault: Optional[str] = None

    @property
    def a(self) -> float:
        return 1.0 + (self.index % 3) * 0.5

    @property
    def b(self) -> float:
        return self.index * 0.25

    @property
    def filters(self) -> bool:
        return self.index % 3 == 2

    @property
    def m(self) -> int:
        return 5 + self.index % 4

    @property
    def r(self) -> int:
        return self.index % self.m

    @property
    def seed(self) -> float:
        return float(self.index)


@dataclass
class Topology:
    name: str
    nodes: List[Node]
    # The pandas dtype of the s column the pandas loaders make: 'object' (pandas' default
    # for strings) or 'arrow' (string[pyarrow]).
    strings: str = 'object'

    @property
    def roots(self) -> List[Node]:
        return [n for n in self.nodes if not n.upstream]

    @property
    def leaves(self) -> List[Node]:
        used = {u for n in self.nodes for u in n.upstream}
        return [n for n in self.nodes if n.index not in used]

    def languages(self) -> str:
        return ','.join(n.language for n in self.nodes)


# Topologies: each returns the upstream lists of nodes 0..n-1, in topological order.

def chain(length: int) -> List[List[int]]:
    return [[]] + [[i - 1] for i in range(1, length)]


def chains(count: int, length: int) -> List[List[int]]:
    out = []
    for c in range(count):
        start = c * length
        out += [[]] + [[start + i - 1] for i in range(1, length)]
    return out


def diamond() -> List[List[int]]:
    return [[], [0], [0], [1, 2]]


def fan_out(width: int) -> List[List[int]]:
    return [[]] + [[0] for _ in range(width)]


def fan_in(width: int) -> List[List[int]]:
    return [[] for _ in range(width)] + [list(range(width))]


def lattice(layers: int, width: int) -> List[List[int]]:
    out = [[] for _ in range(width)]
    for layer in range(1, layers):
        previous = list(range((layer - 1) * width, layer * width))
        out += [list(previous) for _ in range(width)]
    return out


def random_dag(size: int, probability: float, seed: int) -> List[List[int]]:
    rng = random.Random(seed)
    out = [[]]
    for j in range(1, size):
        parents = [i for i in range(j) if rng.random() < probability]
        out.append(parents)
    return out


def assign(upstream: List[List[int]], pattern: str, seed: int = 0) -> List[str]:
    """Languages of the nodes: 'pandas', 'alternate:pandas,rust', 'islands:2:pandas,rust'."""
    size = len(upstream)
    if pattern in LANGUAGES:
        return [pattern] * size
    kind, _, rest = pattern.partition(':')
    if kind == 'alternate':
        langs = rest.split(',')
        return [langs[i % len(langs)] for i in range(size)]
    if kind == 'islands':
        run, _, langs = rest.partition(':')
        langs = langs.split(',')
        return [langs[(i // int(run)) % len(langs)] for i in range(size)]
    if kind == 'random':
        rng = random.Random(seed)
        langs = rest.split(',') if rest else list(LANGUAGES)
        return [rng.choice(langs) for _ in range(size)]
    raise ValueError(pattern)


def topology(name: str, upstream: List[List[int]], pattern: str, seed: int = 0,
             faults: Optional[Dict[int, str]] = None) -> Topology:
    languages = assign(upstream, pattern, seed)
    faults = faults or {}
    return Topology(name, [Node(i, lang, list(up), faults.get(i)) for i, (up, lang)
                           in enumerate(zip(upstream, languages))])


def descendants(top: Topology, index: int) -> List[int]:
    found, frontier = set(), [index]
    while frontier:
        current = frontier.pop()
        for node in top.nodes:
            if current in node.upstream and node.index not in found:
                found.add(node.index)
                frontier.append(node.index)
    return sorted(found)


# Block code.

def _py_loader(node: Node, rows: int, polars: bool, strings: str = 'object') -> str:
    frame = 'pl.from_pandas(frame)' if polars else 'frame'
    if strings == 'arrow' and not polars:
        frame = "frame.astype({'s': 'string[pyarrow]'})"
    imports = 'import pandas as pd\nimport polars as pl\n' if polars else 'import pandas as pd\n'
    return f'''
import numpy as np
{imports}
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load(*args, **kwargs):
    ids = np.arange({rows}, dtype='int64')
    frame = pd.DataFrame({{
        'id': ids,
        'x': ids * 0.5 + {node.seed!r},
        'g': ids % 7,
        's': ['k' + str(i % 5) for i in ids],
    }})
    return {frame}
'''


def _py_fault_start(node: Node) -> str:
    if node.fault == 'raise':
        return f"\n    raise ValueError('injected fault in node {node.index}')"
    return ''


def _py_fault_end(node: Node, polars: bool) -> str:
    if node.fault == 'empty':
        return '\n    out = out.head(0)'
    if node.fault == 'missing_column':
        return "\n    out = out.drop('x')" if polars else "\n    out = out.drop(columns=['x'])"
    return ''


def _py_node(node: Node, polars: bool) -> str:
    keep = (f"\n    out = out.filter(pl.col('id') % {node.m} != {node.r})" if polars
            else f"\n    out = out[out['id'] % {node.m} != {node.r}].reset_index(drop=True)"
            ) if node.filters else ''
    end = _py_fault_end(node, polars)
    if polars:
        body = f'''
    frames = [f if isinstance(f, pl.DataFrame) else pl.from_pandas(f) for f in frames]
    joined = frames[0].select('id', pl.col('x').alias('x_0'), 'g', 's')
    for i, other in enumerate(frames[1:], 1):
        joined = joined.join(other.select('id', pl.col('x').alias(f'x_{{i}}')), on='id')
    total = pl.sum_horizontal([f'x_{{i}}' for i in range(len(frames))])
    out = joined.select('id', (total * {node.a!r} + {node.b!r}).alias('x'), 'g', 's'){keep}{end}
    return out
'''
        imports = 'import polars as pl\n'
    else:
        body = f'''
    frames = [f.to_pandas() if hasattr(f, 'to_pandas') else f for f in frames]
    joined = frames[0][['id', 'x', 'g', 's']].rename(columns={{'x': 'x_0'}})
    for i, other in enumerate(frames[1:], 1):
        right = other[['id', 'x']].rename(columns={{'x': f'x_{{i}}'}})
        joined = joined.merge(right, on='id', how='inner')
    total = sum(joined[f'x_{{i}}'] for i in range(len(frames)))
    out = joined[['id', 'g', 's']].assign(x=total * {node.a!r} + {node.b!r})
    out = out[['id', 'x', 'g', 's']]{keep}{end}
    return out
'''
        imports = ''
    return f'''{imports}
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def node(*frames, **kwargs):{_py_fault_start(node)}{body}'''


def _r_loader(node: Node, rows: int) -> str:
    return f'''
#* @data_loader
load <- function(...) {{
  ids <- bit64::as.integer64(seq_len({rows})) - 1L
  tibble::tibble(
    id = ids,
    x = as.numeric(ids) * 0.5 + {node.seed!r},
    g = ids %% 7L,
    s = paste0("k", as.integer(ids %% 5L))
  )
}}
'''


def _r_node(node: Node) -> str:
    args = ', '.join(f'df_{i + 1}' for i in range(len(node.upstream)))
    keep = (f'\n  out <- out[as.numeric(out$id) %% {node.m} != {node.r}, ]'
            if node.filters else '')
    start = (f'\n  stop("injected fault in node {node.index}")'
             if node.fault == 'raise' else '')
    end = {'empty': '\n  out <- out[0, ]',
           'missing_column': '\n  out <- dplyr::select(out, -x)'}.get(node.fault, '')
    return f'''
#* @transformer
node <- function({args}, ...) {{{start}
  frames <- list({args})
  joined <- dplyr::select(frames[[1]], id, x_0 = x, g, s)
  if (length(frames) > 1) {{
    for (i in 2:length(frames)) {{
      other <- dplyr::select(frames[[i]], id, x)
      names(other)[2] <- paste0("x_", i - 1)
      joined <- dplyr::inner_join(joined, other, by = "id")
    }}
  }}
  total <- Reduce(`+`, lapply(paste0("x_", seq_along(frames) - 1), function(n) joined[[n]]))
  out <- tibble::tibble(id = joined$id, x = total * {node.a!r} + {node.b!r},
                        g = joined$g, s = joined$s){keep}{end}
  out
}}
'''


def _rust_loader(node: Node, rows: int) -> str:
    return f'''
use mage::prelude::*;

fn load_data() -> Result<DataFrame> {{
    let ids: Vec<i64> = (0..{rows}i64).collect();
    Ok(df!(
        "id" => ids.clone(),
        "x" => ids.iter().map(|i| *i as f64 * 0.5 + {node.seed!r}).collect::<Vec<f64>>(),
        "g" => ids.iter().map(|i| i % 7).collect::<Vec<i64>>(),
        "s" => ids.iter().map(|i| format!("k{{}}", i % 5)).collect::<Vec<String>>(),
    )?)
}}
'''


def _rust_node(node: Node) -> str:
    keep = (f'\n        .filter((col("id") % lit({node.m}i64)).neq(lit({node.r}i64)))'
            if node.filters else '')
    start = (f'\n    bail!("injected fault in node {node.index}");\n    #[allow(unreachable_code)]'
             if node.fault == 'raise' else '')
    columns = ('col("id"), col("g"), col("s")' if node.fault == 'missing_column'
               else 'col("id"), col("x"), col("g"), col("s")')
    end = {'empty': '\n        .filter(lit(false))',
           'missing_column': f'\n        .select([{columns}])'}.get(node.fault, '')
    return f'''
use mage::prelude::*;

fn transform(inputs: Vec<LazyFrame>) -> Result<LazyFrame> {{{start}
    let mut inputs = inputs.into_iter();
    let first = inputs.next().ok_or_else(|| anyhow!("no input"))?;
    let mut joined = first.select([col("id"), col("x").alias("x_0"), col("g"), col("s")]);
    let mut names = vec!["x_0".to_string()];
    for (i, other) in inputs.enumerate() {{
        let name = format!("x_{{}}", i + 1);
        joined = joined.join(
            other.select([col("id"), col("x").alias(name.as_str())]),
            [col("id")],
            [col("id")],
            JoinArgs::new(JoinType::Inner),
        );
        names.push(name);
    }}
    let total = names
        .iter()
        .map(|n| col(n.as_str()))
        .reduce(|a, b| a + b)
        .ok_or_else(|| anyhow!("no columns"))?;
    Ok(joined
        .select([
            col("id"),
            (total * lit({node.a!r}) + lit({node.b!r})).alias("x"),
            col("g"),
            col("s"),
        ]){keep}{end})
}}
'''


def code(node: Node, rows: int, strings: str = 'object') -> str:
    root = not node.upstream
    if node.language == 'pandas':
        text = _py_loader(node, rows, False, strings) if root else _py_node(node, False)
    elif node.language == 'polars':
        text = _py_loader(node, rows, True) if root else _py_node(node, True)
    elif node.language == 'r':
        text = _r_loader(node, rows) if root else _r_node(node)
    else:
        text = _rust_loader(node, rows) if root else _rust_node(node)
    return textwrap.dedent(text).strip() + '\n'


# The pandas oracle.

def oracle(top: Topology, rows: int) -> Dict[int, 'object']:
    import numpy as np
    import pandas as pd

    out = {}
    for node in top.nodes:
        if not node.upstream:
            ids = np.arange(rows, dtype='int64')
            out[node.index] = pd.DataFrame({
                'id': ids, 'x': ids * 0.5 + node.seed, 'g': ids % 7,
                's': ['k' + str(i % 5) for i in ids],
            })
            continue
        frames = [out[u] for u in node.upstream]
        joined = frames[0][['id', 'x', 'g', 's']].rename(columns={'x': 'x_0'})
        for i, other in enumerate(frames[1:], 1):
            joined = joined.merge(other[['id', 'x']].rename(columns={'x': f'x_{i}'}), on='id')
        total = sum(joined[f'x_{i}'] for i in range(len(frames)))
        result = joined[['id', 'g', 's']].assign(x=total * node.a + node.b)[['id', 'x', 'g', 's']]
        if node.filters:
            result = result[result['id'] % node.m != node.r]
        if node.fault == 'empty':
            result = result.head(0)
        out[node.index] = result.reset_index(drop=True)
    return out


def normalize(frame) -> 'object':
    """A pandas frame sorted by id with plain dtypes, from pandas, Polars or anything else."""
    import pandas as pd

    if hasattr(frame, 'to_pandas') and not isinstance(frame, pd.DataFrame):
        frame = frame.to_pandas()
    frame = frame[['id', 'x', 'g', 's']].copy()
    frame['id'] = frame['id'].astype('int64')
    frame['g'] = frame['g'].astype('int64')
    frame['x'] = frame['x'].astype('float64')
    frame['s'] = frame['s'].astype(str)
    return frame.sort_values('id').reset_index(drop=True)


def differences(expected, actual) -> Optional[str]:
    import numpy as np

    e, a = normalize(expected), normalize(actual)
    if len(e) != len(a):
        return f'{len(a)} rows, expected {len(e)}'
    if not (e['id'].values == a['id'].values).all():
        return 'different ids'
    if not np.allclose(e['x'].values, a['x'].values, rtol=1e-12, atol=1e-9, equal_nan=True):
        i = int(np.argmax(~np.isclose(e['x'].values, a['x'].values, rtol=1e-12, atol=1e-9)))
        return f'x differs at id {e["id"].iloc[i]}: {a["x"].iloc[i]!r} != {e["x"].iloc[i]!r}'
    if not (e['g'].values == a['g'].values).all():
        return 'different g'
    if not (e['s'].values == a['s'].values).all():
        return 'different s'
    return None


# Mage pipelines.

def build(project: str, top: Topology, rows: int, prefix: str = 'study'):
    from mage_ai.data_preparation.models.block import Block
    from mage_ai.data_preparation.models.pipeline import Pipeline

    suffix = uuid.uuid4().hex[:8]
    pipeline = Pipeline.create(f'{prefix}_{suffix}', repo_path=project)
    uuids = {}
    for node in top.nodes:
        block_type = 'transformer' if node.upstream else 'data_loader'
        block = Block.create(f'n{node.index}_{node.language}_{suffix}', block_type, project,
                             language=MAGE_LANGUAGE[node.language])
        Path(block.file_path).write_text(code(node, rows, top.strings))
        pipeline.add_block(block, upstream_block_uuids=[uuids[u] for u in node.upstream])
        uuids[node.index] = block.uuid
    return Pipeline.get(pipeline.uuid, repo_path=project), uuids


def run(pipeline, mode: str):
    """Runs through the scheduler with each job in its own process; returns (run, seconds)."""
    from mage_ai.orchestration import fusion, fusion_verify
    from mage_ai.shared.dates import utc_now

    fusion_mode = {'off': fusion.BLOCK_FUSION_OFF, 'chains': fusion.BLOCK_FUSION_CHAINS}[mode]
    started = time.perf_counter()
    pipeline_run = fusion_verify.run_in_process(
        fusion_verify._create_run(pipeline, fusion_mode, {}, utc_now()), log=lambda text: None,
    )
    return pipeline_run, time.perf_counter() - started


def statuses(pipeline_run) -> Dict[str, str]:
    return {b.block_uuid: getattr(b.status, 'value', b.status) for b in pipeline_run.block_runs}


def block_seconds(pipeline_run) -> Dict[str, Optional[float]]:
    out = {}
    for b in pipeline_run.block_runs:
        if b.started_at and b.completed_at:
            out[b.block_uuid] = (b.completed_at - b.started_at).total_seconds()
        else:
            out[b.block_uuid] = None
    return out


def stored(pipeline, block_uuid: str, pipeline_run):
    return pipeline.variable_manager.get_variable(
        pipeline.uuid, block_uuid, 'output_0', partition=pipeline_run.execution_partition,
    )
