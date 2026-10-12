if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(window, *args, **kwargs):
    print(f"No new drivers after {window['last_id']}.")
