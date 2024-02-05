if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f"Rows with zero passenger count: {data['passenger_count'].isin([0]).sum()}")
    print(f"Rows with zero trip distance: {data['trip_distance'].isin([.00]).sum()}")
    print(f"Existing values of VendorID column: {data['VendorID'].unique()}")
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.columns = (
        data.columns
        .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
        .str.lower()
    )
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] != .00)]


@test
def test_output(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'There is no "vendor_id" column in data'
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([[.00]]).sum() == 0, 'There are rides with zero trip distances'
