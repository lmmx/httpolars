import polars as pl
from httpolars import api_call


def test_api_call():
    df = pl.DataFrame({"number": 123})
    result = df.with_columns(api_result=api_call("number").alias("is_even"))
    expected_df = pl.DataFrame(
        {
            "number": 123,
            "is_even": [False],
        }
    )
    assert result.equals(expected_df)
