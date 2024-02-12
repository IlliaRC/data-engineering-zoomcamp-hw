import io
import math
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    result = None
    for m in range(1, 13):
        tmp = m if math.floor(math.log10(m) + 1) > 1 else '0' + str(m)
        data = pd.read_parquet(
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{tmp}.parquet"
        )
        if result is None:
            result = data
        else:
            result = pd.concat([result, data])
    return result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
