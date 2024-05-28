import polars as pl
from pytest import mark

import httpolars as httpl


def test_api_call_noop():
    df = pl.DataFrame({"number": [1, 2, 3]})
    result = df.with_columns(
        api_result=httpl.api_call("number", endpoint="http://localhost:80/noop")
    )
    assert result.to_dicts() == df.to_dicts()


@mark.skip
def test_api_call_factorial():
    df = pl.DataFrame({"number": [1, 2, 3]})
    result = df.with_columns(
        api_result=httpl.api_call(
            "number", endpoint="http://localhost:80/permutations"
        ).alias("factorial")
    )
    expected_df = pl.DataFrame(
        {
            "number": [1, 2, 3],
            "factorial": [1, 2, 6],
        }
    )
    assert result.to_dicts() == expected_df.to_dicts()
