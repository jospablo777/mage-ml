"""
Checks what a ClickHouse SQL block returns for a frame from a Python block.
"""
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def check(frame: pd.DataFrame, **kwargs) -> None:
    rows = int(kwargs.get('rows', 1000))
    assert len(frame) == rows, len(frame)
    frame = frame.sort_values('id').reset_index(drop=True)
    for i in range(rows):
        row = frame.iloc[i]
        expected_big = 2**53 + i if i % 7 else None
        if expected_big is None:
            assert pd.isna(row['big']) and pd.isna(row['big_plus_one']), row
        else:
            assert row['big'] == expected_big and row['big_plus_one'] == expected_big + 1, row
        if i % 5:
            assert row['text_length'] == len(f'ñ {i}'), row
        assert row['day_year'] == (pd.Timestamp('2024-01-01') + pd.Timedelta(days=i)).year
        # The microseconds survived the upstream table.
        assert row['at_micros'] % 1_000_000 == 1, row
