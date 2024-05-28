import polars as pl
from inline_snapshot import snapshot

import httpolars as httpl


def test_abs_i64():
    df = df = pl.DataFrame(
        {"a": [1, -1, None], "b": [4.1, 5.2, -6.3], "c": ["hello", "everybody!", "!"]}
    )
    result = df.with_columns(httpl.abs_i64("a").name.suffix("_abs"))
    assert result.to_dicts() == snapshot(
        [
            {"a": 1, "b": 4.1, "c": "hello", "a_abs": 1},
            {"a": -1, "b": 5.2, "c": "everybody!", "a_abs": 1},
            {"a": None, "b": -6.3, "c": "!", "a_abs": None},
        ]
    )


def test_sum_i64():
    df = pl.DataFrame({"a": [1, 5, 2], "b": [3, None, -1]})
    result = df.with_columns(a_plus_b=httpl.sum_i64("a", "b"))
    assert result.to_dicts() == snapshot(
        [
            {"a": 1, "b": 3, "a_plus_b": 4},
            {"a": 5, "b": None, "a_plus_b": None},
            {"a": 2, "b": -1, "a_plus_b": 1},
        ]
    )


def test_add_suffix():
    df = pl.DataFrame({"a": ["bob", "billy"]})
    result = df.with_columns(httpl.add_suffix("a", suffix="-billy"))
    assert result.to_dicts() == snapshot([{"a": "bob-billy"}, {"a": "billy-billy"}])
