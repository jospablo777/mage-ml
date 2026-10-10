"""
Releases: a model version that a block registers in MLflow is evaluated against the
project's release policy for that model, and promoted by moving an MLflow alias.

    # releases/churn_model.yaml
    alias: champion          # the alias that marks the released version
    approval: manual         # or automatic: promote as soon as every rule passes
    fail_block: false        # true: a failed evaluation fails the block run
    rules:
      - metric: auc
        min: 0.75
        max_regression: 0.01 # at most 0.01 worse than the current champion
      - metric: log_loss
        max: 0.5
        higher_is_better: false
      - metric: eval_rows
        min: 1000

When a block run registers a version of a model with a policy (experiments.py records
the versions), its MLflow run's metrics are checked against each rule, and against the
metrics of the champion's run for regressions. The decision is pass (every rule holds),
fail (a rule does not), or hold (a metric is missing or not a finite number, so the rule
cannot be checked). It is recorded in block_run.metrics['releases'].

A passing version is promoted at once with `approval: automatic`; otherwise someone
approves it from the pipeline run's page or `mage release promote`. A promotion first
checks that the champion is still the one the version was compared with. Every promotion
and rollback is appended to the model's release log, which a rollback reads to restore
the previous champion.
"""
import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import yaml

RELEASES_FOLDER = 'releases'
LOG_FOLDER = '.releases'
_NAME = re.compile(r'^[\w.\-]{1,200}$')
_RULE_KEYS = {'metric', 'min', 'max', 'max_regression', 'higher_is_better'}
_POLICY_KEYS = {'model', 'alias', 'approval', 'fail_block', 'rules', 'description'}


class ReleaseError(Exception):
    pass


@dataclass
class Rule:
    metric: str
    min: Optional[float] = None
    max: Optional[float] = None
    max_regression: Optional[float] = None
    higher_is_better: bool = True


@dataclass
class Policy:
    model: str
    alias: str = 'champion'
    approval: str = 'manual'
    fail_block: bool = False
    rules: List[Rule] = field(default_factory=list)


def _number(value: Any, what: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ReleaseError(f'{what} must be a finite number.')
    return float(value)


def load_policy(model: str, repo_path: str) -> Optional[Policy]:
    """The release policy of a registered model, None when the project has none."""
    if not _NAME.match(model or ''):
        return None
    folder = os.path.join(repo_path, RELEASES_FOLDER)
    path = next(
        (os.path.join(folder, f'{model}{s}') for s in ('.yaml', '.yml')
         if os.path.isfile(os.path.join(folder, f'{model}{s}'))),
        None,
    )
    if path is None:
        return None
    try:
        document = yaml.safe_load(open(path, encoding='utf-8').read()) or {}
    except yaml.YAMLError as error:
        raise ReleaseError(f'The release policy of {model} is not valid YAML: {error}')
    if not isinstance(document, dict):
        raise ReleaseError(f'The release policy of {model} must be a mapping.')
    unknown = set(document) - _POLICY_KEYS
    if unknown:
        raise ReleaseError(
            f'The release policy of {model} has unknown settings: {", ".join(sorted(unknown))}.'
        )
    approval = document.get('approval', 'manual')
    if approval not in ('manual', 'automatic'):
        raise ReleaseError(f'The approval of {model} must be manual or automatic.')
    rules = []
    for index, raw in enumerate(document.get('rules') or [], start=1):
        if not isinstance(raw, dict) or not raw.get('metric'):
            raise ReleaseError(f'Rule {index} of {model} needs a metric.')
        unknown = set(raw) - _RULE_KEYS
        if unknown:
            raise ReleaseError(
                f'Rule {index} of {model} has unknown settings: {", ".join(sorted(unknown))}.'
            )
        rule = Rule(
            metric=str(raw['metric']),
            min=_number(raw.get('min'), f'The min of rule {index} of {model}'),
            max=_number(raw.get('max'), f'The max of rule {index} of {model}'),
            max_regression=_number(
                raw.get('max_regression'), f'The max_regression of rule {index} of {model}',
            ),
            higher_is_better=bool(raw.get('higher_is_better', True)),
        )
        if rule.min is None and rule.max is None and rule.max_regression is None:
            raise ReleaseError(f'Rule {index} of {model} needs min, max or max_regression.')
        if rule.max_regression is not None and rule.max_regression < 0:
            raise ReleaseError(f'The max_regression of rule {index} of {model} is negative.')
        rules.append(rule)
    if not rules:
        raise ReleaseError(f'The release policy of {model} has no rules.')
    alias = str(document.get('alias') or 'champion')
    if not _NAME.match(alias):
        raise ReleaseError(f'The alias of {model} is not valid.')
    return Policy(
        model=model,
        alias=alias,
        approval=approval,
        fail_block=bool(document.get('fail_block', False)),
        rules=rules,
    )


def _finite(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def evaluate(policy: Policy, candidate: Dict[str, Any], champion: Optional[Dict[str, Any]]) -> Dict:
    """
    The decision for a candidate's metrics against the policy and, for regressions, the
    champion's metrics. A failed rule fails the evaluation even when another rule holds.
    """
    rows = []
    for rule in policy.rules:
        value = _finite(candidate.get(rule.metric))
        row = dict(metric=rule.metric, value=value, result='pass', reasons=[])
        if value is None:
            row.update(result='hold', reasons=['the candidate has no finite value'])
            rows.append(row)
            continue
        if rule.min is not None and value < rule.min:
            row['result'] = 'fail'
            row['reasons'].append(f'below the min {rule.min}')
        if rule.max is not None and value > rule.max:
            row['result'] = 'fail'
            row['reasons'].append(f'above the max {rule.max}')
        if rule.max_regression is not None:
            reference = _finite((champion or {}).get(rule.metric)) if champion else None
            row['champion'] = reference
            if champion is None:
                row['reasons'].append('no champion to compare with')
            elif reference is None:
                if row['result'] == 'pass':
                    row['result'] = 'hold'
                row['reasons'].append('the champion has no finite value')
            else:
                regression = (reference - value) if rule.higher_is_better else (value - reference)
                row['regression'] = regression
                if regression > rule.max_regression:
                    row['result'] = 'fail'
                    row['reasons'].append(
                        f'{regression:g} worse than the champion (at most {rule.max_regression:g})'
                    )
        rows.append(row)
    results = {row['result'] for row in rows}
    decision = 'fail' if 'fail' in results else 'hold' if 'hold' in results else 'pass'
    return dict(decision=decision, rules=rows)


# ---- MLflow ------------------------------------------------------------------------


def _client():
    from mlflow.tracking import MlflowClient

    return MlflowClient()


def champion_of(client, policy: Policy):
    """The model version that holds the policy's alias, None when no version does."""
    try:
        return client.get_model_version_by_alias(policy.model, policy.alias)
    except Exception:
        return None


def _run_metrics(client, run_id: Optional[str]) -> Dict[str, float]:
    if not run_id:
        return {}
    return dict(client.get_run(run_id).data.metrics)


def evaluate_version(policy: Policy, version: str, client=None) -> Dict[str, Any]:
    """Evaluates a registered version of the policy's model against its champion."""
    client = client or _client()
    candidate = client.get_model_version(policy.model, str(version))
    champion = champion_of(client, policy)
    if champion is not None and str(champion.version) == str(version):
        champion = None
    result = evaluate(
        policy,
        _run_metrics(client, candidate.run_id),
        _run_metrics(client, champion.run_id) if champion else None,
    )
    result.update(
        model=policy.model,
        version=str(version),
        alias=policy.alias,
        approval=policy.approval,
        champion=str(champion.version) if champion else None,
        evaluated_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
        status='awaiting approval' if result['decision'] == 'pass' else 'not promoted',
    )
    return result


def _log_path(repo_path: str, model: str) -> str:
    """The model's release log, with the project's variables (not its code)."""
    from mage_ai.settings.repo import get_variables_dir

    return os.path.join(get_variables_dir(repo_path=repo_path), LOG_FOLDER, f'{model}.jsonl')


def release_log(repo_path: str, model: str) -> List[Dict[str, Any]]:
    path = _log_path(repo_path, model)
    if not os.path.isfile(path):
        return []
    with open(path, encoding='utf-8') as file:
        return [json.loads(line) for line in file if line.strip()]


def _append_log(repo_path: str, model: str, entry: Dict[str, Any]) -> None:
    path = _log_path(repo_path, model)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'a', encoding='utf-8') as file:
        file.write(json.dumps(entry) + '\n')


def promote(
    repo_path: str,
    model: str,
    version: str,
    expected_champion: Optional[str],
    actor: str,
    client=None,
    reason: str = 'promotion',
) -> Dict[str, Any]:
    """
    Moves the policy's alias to the version, if the champion is still expected_champion
    (the version it was evaluated against); returns the release log entry.
    """
    policy = load_policy(model, repo_path)
    if policy is None:
        raise ReleaseError(f'{model} has no release policy in {RELEASES_FOLDER}/.')
    client = client or _client()
    current = champion_of(client, policy)
    current_version = str(current.version) if current else None
    if current_version != (str(expected_champion) if expected_champion else None):
        raise ReleaseError(
            f'The {policy.alias} of {model} is now version {current_version or "none"}, not '
            f'{expected_champion or "none"} as when version {version} was evaluated; '
            'evaluate it again.'
        )
    client.set_registered_model_alias(model, policy.alias, str(version))
    entry = dict(
        action=reason,
        model=model,
        alias=policy.alias,
        version=str(version),
        previous=current_version,
        actor=actor,
        at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
    )
    _append_log(repo_path, model, entry)
    return entry


def rollback(repo_path: str, model: str, actor: str, client=None) -> Dict[str, Any]:
    """Gives the alias back to the version that held it before the latest promotion."""
    policy = load_policy(model, repo_path)
    if policy is None:
        raise ReleaseError(f'{model} has no release policy in {RELEASES_FOLDER}/.')
    client = client or _client()
    current = champion_of(client, policy)
    current_version = str(current.version) if current else None
    latest = next(
        (e for e in reversed(release_log(repo_path, model)) if e.get('version') == current_version),
        None,
    )
    if latest is None or not latest.get('previous'):
        raise ReleaseError(
            f'The release log of {model} has no version before {current_version or "none"} '
            'to roll back to.'
        )
    return promote(
        repo_path, model, latest['previous'], current_version, actor, client=client,
        reason='rollback',
    )


def evaluate_block_run(repo_path: str, mlflow_record: Optional[Dict], actor: str) -> List[Dict]:
    """
    Evaluates every model version a block run registered that has a release policy, and
    promotes the passing ones of policies with automatic approval.
    """
    evaluations = []
    for run in (mlflow_record or {}).get('runs') or []:
        for registered in run.get('model_versions') or []:
            policy = load_policy(registered['name'], repo_path)
            if policy is None:
                continue
            client = _client()
            result = evaluate_version(policy, registered['version'], client=client)
            if result['decision'] == 'pass' and policy.approval == 'automatic':
                promote(
                    repo_path, policy.model, result['version'], result['champion'], actor,
                    client=client,
                )
                result['status'] = 'promoted'
            result['fail_block'] = policy.fail_block
            evaluations.append(result)
    return evaluations


def summary(evaluation: Dict[str, Any]) -> str:
    lines = [
        f'Release check of {evaluation["model"]} version {evaluation["version"]}: '
        f'{evaluation["decision"]} ({evaluation["status"]}; {evaluation["alias"]} is '
        f'{evaluation["champion"] or "none"}).'
    ]
    for row in evaluation['rules']:
        detail = f'; {", ".join(row["reasons"])}' if row['reasons'] else ''
        lines.append(f'- {row["metric"]} = {row["value"]}: {row["result"]}{detail}')
    return '\n'.join(lines)
