import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def test_api_call_noop():
    df = pl.DataFrame({"value": ["x", "y", "z"]})
    response = httpl.api_call("value", endpoint="http://localhost:8000/noop")
    result = df.with_columns(response.str.json_path_match("$.value"))
    assert result.to_dicts() == snapshot(
        [
            {"value": "x"},
            {"value": "y"},
            {"value": "z"},
        ]
    )


def test_api_call_factorial():
    df = pl.DataFrame({"number": [1, 2, 3]})
    response = httpl.api_call("number", endpoint="http://localhost:8000/factorial")
    factorial = response.str.json_path_match("$.factorial").str.to_integer()
    result = df.with_columns(permutations=factorial)
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "permutations": 1},
            {"number": 2, "permutations": 2},
            {"number": 3, "permutations": 6},
        ]
    )
