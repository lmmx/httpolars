import polars as pl

import httpolars as httpl


def test_api_call():
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
    assert result.equals(expected_df)
