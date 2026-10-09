"""
Checks what the R transformer returned against the source frame, and returns it for the
SQL exporter.
"""
from integration_tests.data import r_dataset

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def check(frame, **kwargs):
    source = r_dataset.source_frame(int(kwargs.get('rows', 200)))
    mismatches = r_dataset.round_trip_mismatches(source, frame)
    assert not mismatches, mismatches
    derived = r_dataset.derived_mismatches(
        source, frame, multiplier=kwargs['multiplier'], prefix=kwargs['prefix'],
    )
    assert not derived, derived
    return frame


@test
def test_rows(frame, **kwargs) -> None:
    import pointblank as pb

    (
        pb.Validate(data=frame)
        .col_vals_not_null(columns='id')
        .rows_distinct(columns_subset='id')
        .col_vals_ge(columns='r_list_length', value=0)
        .interrogate()
        .assert_passing()
    )
