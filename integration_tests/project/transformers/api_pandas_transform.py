if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(frame, *args, **kwargs):
    frame = frame.copy(deep=False)
    frame['name_length'] = frame['name'].str.len()
    frame['doubled'] = frame['amount'] * 2
    frame['tag_count'] = frame['tags'].list.len()
    frame['year'] = frame['day'].dt.year
    # pandas 3 does not implement % for pyarrow-backed integers.
    frame['is_even'] = frame['id'].astype('Int64') % 2 == 0
    frame['upper_name'] = frame['name'].str.upper()
    return frame
