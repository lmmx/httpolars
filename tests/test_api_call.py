import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def test_api_call_noop():
    df = pl.DataFrame({"number": [1, 2, 3]})
    result = df.with_columns(
        response=httpl.api_call("number", endpoint="http://localhost:80/noop")
    )
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "response": "http://localhost:80/noop?number=1"},
            {"number": 2, "response": "http://localhost:80/noop?number=2"},
            {"number": 3, "response": "http://localhost:80/noop?number=3"},
        ]
    )


def test_api_call_factorial():
    df = pl.DataFrame({"number": [1, 2, 3]})
    result = df.with_columns(
        response=httpl.api_call(
            "number", endpoint="http://localhost:80/factorial"
        ).alias("permutations")
    )
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "response": "http://localhost:80/factorial?number=1"},
            {"number": 2, "response": "http://localhost:80/factorial?number=2"},
            {"number": 3, "response": "http://localhost:80/factorial?number=3"},
        ]
    )
