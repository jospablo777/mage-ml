"""
Evidently and NannyML for monitors (monitors.py), run in their own environment.

This file is a script: the environment's Python runs it with a request file, so it
imports nothing of Mage, only pandas, pyarrow and the engines.

    python monitor_engines.py request.json

The request names the current and baseline Parquet files, the output folder and the
settings of each engine. The script writes result.json there, with each engine's checks
(check, column, value, result, detail) and the HTML reports it saved.
"""
import json
import math
import sys
import traceback
import warnings
from pathlib import Path


def _frame(paths):
    import pandas as pd
    import pyarrow.parquet as pq

    frames = [pq.read_table(path).to_pandas() for path in paths]
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


# ---- Evidently ------------------------------------------------------------------------

EVIDENTLY_STATUS = {'SUCCESS': 'pass', 'WARNING': 'warn', 'FAIL': 'fail', 'ERROR': 'hold'}


def _evidently_definition(settings):
    from evidently import BinaryClassification, DataDefinition, Regression

    target = settings.get('target')
    if not target:
        return None
    if settings.get('task') == 'regression':
        return DataDefinition(regression=[Regression(
            target=target, prediction=settings['prediction'],
        )])
    return DataDefinition(classification=[BinaryClassification(
        target=target,
        prediction_labels=settings.get('prediction'),
        prediction_probas=settings.get('score'),
        pos_label=settings.get('positive', 1),
    )])


def run_evidently(settings, current, baseline, output):
    from evidently import Dataset, Report
    from evidently import presets as available

    names = settings.get('presets') or ['DataDriftPreset', 'DataSummaryPreset']
    presets = []
    for name in names:
        preset = getattr(available, name, None)
        if preset is None:
            raise ValueError(
                f'Evidently has no preset {name}; the presets are '
                f'{", ".join(n for n in dir(available) if n.endswith("Preset"))}.'
            )
        options = {}
        if name == 'DataDriftPreset':
            if settings.get('drift_share') is not None:
                options['drift_share'] = float(settings['drift_share'])
            if settings.get('columns'):
                options['columns'] = settings['columns']
        presets.append(preset(**options))
    definition = _evidently_definition(settings)
    if definition is not None:
        current = Dataset.from_pandas(current, data_definition=definition)
        baseline = Dataset.from_pandas(baseline, data_definition=definition)
    snapshot = Report(presets, include_tests=True).run(current, baseline)
    html = output / 'evidently.html'
    snapshot.save_html(str(html))
    checks = []
    for test in snapshot.dict().get('tests') or []:
        status = getattr(test.get('status'), 'value', test.get('status'))
        checks.append(dict(
            check='evidently',
            column=None,
            value=None,
            result=EVIDENTLY_STATUS.get(str(status), 'hold'),
            detail=f"{test.get('name')}: {test.get('description')}",
        ))
    return dict(checks=checks, reports=[html.name])


# ---- NannyML ------------------------------------------------------------------------


def _chunking(settings):
    if settings.get('chunk_number'):
        return dict(chunk_number=int(settings['chunk_number']))
    return dict(chunk_size=int(settings.get('chunk_size') or 1000))


def _alerts(frame, prefix):
    """(alert in any chunk, value of the last chunk) for a metric's columns."""
    alert_column = prefix + ('alert',)
    value_column = prefix + ('value',)
    alerts = frame[alert_column].fillna(False).astype(bool)
    values = frame[value_column]
    return bool(alerts.any()), _finite(values.iloc[-1]) if len(values) else None, int(alerts.sum())


def run_nannyml(settings, current, baseline, output):
    import nannyml as nml

    problem = settings.get('problem_type', 'classification_binary')
    timestamp = settings.get('timestamp')
    common = dict(**_chunking(settings))
    if timestamp:
        common['timestamp_column_name'] = timestamp
    checks, reports = [], []

    if settings.get('y_pred') and settings.get('y_true'):
        metrics = settings.get('metrics') or (
            ['mae', 'rmse'] if problem == 'regression' else ['roc_auc', 'f1']
        )
        if problem == 'regression':
            features = settings.get('features') or []
            if not features:
                raise ValueError('NannyML estimates regression performance (DLE) from the '
                                 'features: set nannyml.features.')
            estimator = nml.DLE(
                feature_column_names=features, y_pred=settings['y_pred'],
                y_true=settings['y_true'], metrics=metrics, **common,
            )
        else:
            estimator = nml.CBPE(
                y_pred_proba=settings.get('y_pred_proba'), y_pred=settings['y_pred'],
                y_true=settings['y_true'], metrics=metrics, problem_type=problem, **common,
            )
        estimator.fit(baseline)
        analysis = current.drop(columns=[settings['y_true']], errors='ignore')
        estimated = estimator.estimate(analysis)
        frame = estimated.filter(period='analysis').to_df()
        for metric in metrics:
            alert, value, count = _alerts(frame, (metric,))
            checks.append(dict(
                check=f'estimated_{metric}', column=None, value=value,
                result='fail' if alert else 'pass',
                detail=(
                    f'NannyML estimates {metric} {value:.4g} in the last chunk without labels'
                    if value is not None else f'NannyML estimated no {metric}'
                ) + (f'; outside its thresholds in {count} chunk(s)' if alert else ''),
            ))
        estimated.plot().write_html(str(output / 'nannyml_performance.html'))
        reports.append('nannyml_performance.html')

    features = settings.get('features') or []
    if features:
        drift = nml.UnivariateDriftCalculator(
            column_names=features,
            continuous_methods=settings.get('continuous_methods') or ['jensen_shannon'],
            categorical_methods=settings.get('categorical_methods') or ['jensen_shannon'],
            **common,
        )
        drift.fit(baseline)
        calculated = drift.calculate(current)
        frame = calculated.filter(period='analysis').to_df()
        for column, method, field in frame.columns:
            if column == 'chunk' or field != 'alert':
                continue
            alert, value, count = _alerts(frame, (column, method))
            checks.append(dict(
                check='nannyml_drift', column=column, value=value,
                result='fail' if alert else 'pass',
                detail=f'NannyML {method} {value if value is None else round(value, 4)}'
                       + (f'; alert in {count} chunk(s)' if alert else ''),
            ))
        calculated.plot().write_html(str(output / 'nannyml_drift.html'))
        reports.append('nannyml_drift.html')
        if len(features) >= 2:
            reconstruction = nml.DataReconstructionDriftCalculator(
                column_names=features, **common,
            )
            reconstruction.fit(baseline)
            result = reconstruction.calculate(current)
            frame = result.filter(period='analysis').to_df()
            alert, value, count = _alerts(frame, ('reconstruction_error',))
            checks.append(dict(
                check='nannyml_multivariate_drift', column=None, value=value,
                result='fail' if alert else 'pass',
                detail=f'reconstruction error {value if value is None else round(value, 4)}'
                       + (f'; alert in {count} chunk(s)' if alert else ''),
            ))
            result.plot().write_html(str(output / 'nannyml_multivariate.html'))
            reports.append('nannyml_multivariate.html')
    return dict(checks=checks, reports=reports)


ENGINES = {'evidently': run_evidently, 'nannyml': run_nannyml}


def main(request_path):
    warnings.filterwarnings('ignore')
    request = json.loads(Path(request_path).read_text())
    output = Path(request['output_dir'])
    output.mkdir(parents=True, exist_ok=True)
    current = _frame(request['current'])
    baseline = _frame(request['baseline'])
    result = {}
    for name, function in ENGINES.items():
        settings = request.get(name)
        if not settings:
            continue
        try:
            result[name] = function(settings, current.copy(), baseline.copy(), output)
        except Exception as error:
            result[name] = dict(
                error=f'{type(error).__name__}: {error}',
                details=traceback.format_exc()[-4000:],
                checks=[], reports=[],
            )
    (output / 'result.json').write_text(json.dumps(result, default=str))


if __name__ == '__main__':
    main(sys.argv[1])
