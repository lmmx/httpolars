import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl

from conftest import jsonpath

@mark.parametrize("url", ["http://localhost:8000/factorial"])
def test_reuse_client(url):
    df = pl.DataFrame({"number": [1, 2, 3]})
    client = httpl.create_api_client()
    assert isinstance(client, httpl.ApiClient)
    response = httpl.api_call("number", endpoint=url, client=client).alias("response")
    parsed = jsonpath(response, text=True)
    result_pre = df.with_columns(parsed)
    result = result_pre.select("response").unnest("response")
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "factorial": 1},
            {"number": 2, "factorial": 2},
            {"number": 3, "factorial": 6},
        ]
    )
    print(result)
