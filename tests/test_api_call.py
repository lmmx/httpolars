import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def test_api_call_noop():
    df = pl.DataFrame({"value": ["x", "y", "z"]})
    result = df.with_columns(
        response=httpl.api_call("value", endpoint="http://localhost:8000/noop")
    )
    assert result.to_dicts() == snapshot(
        [
            {"value": "x", "response": '{"value":"x"}'},
            {"value": "y", "response": '{"value":"y"}'},
            {"value": "z", "response": '{"value":"z"}'},
        ]
    )


def test_api_call_factorial():
    df = pl.DataFrame({"number": [1, 2, 3]})
    result = df.with_columns(
        permutations=httpl.api_call(
            "number", endpoint="http://localhost:8000/factorial"
        )
    )
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "permutations": "http://localhost:8000/factorial?number=1"},
            {"number": 2, "permutations": "http://localhost:8000/factorial?number=2"},
            {"number": 3, "permutations": "http://localhost:8000/factorial?number=3"},
        ]
    )
