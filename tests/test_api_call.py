import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def jsonpath(response: str | pl.Expr, *, text: bool = False, status_code: bool = False):
    """Accept either the response `Expr` or reference by its column name."""
    response = pl.col(response) if isinstance(response, str) else response
    if (text and status_code) or not (text or status_code):
        raise NotImplementedError
    if text:
        return response.str.json_path_match(f"$.text").str.json_decode()
    if status_code:
        return response.str.json_path_match(f"$.status_code")


@mark.parametrize("url", ["http://localhost:8000/noop"])
def test_api_call_noop(client, url):
    """the response gives back the input, and the column is overwritten unchanged."""
    df = pl.DataFrame({"value": ["x", "y", "z"]})
    response = httpl.api_call("value", endpoint=url)
    value = jsonpath("value", text=True)
    result_pre = df.with_columns(response)
    result = result_pre.with_columns(value).select(pl.col("value").struct.field("value"))
    assert result.to_dicts() == snapshot(
        [{"value": "x"}, {"value": "y"}, {"value": "z"}]
    )
    print(result)


@mark.parametrize("url", ["http://localhost:8000/permafailure"])
def test_api_call_permafailure_keep_status(client, url):
    """Response is never obtained, always fails (429)."""
    df = pl.DataFrame({"futile": [0, 10, 20]})
    response = httpl.api_call("futile", endpoint=url).alias("response")
    status = jsonpath(response, status_code=True).str.to_integer().alias("status")
    result = df.with_columns(status)
    assert result.to_dicts() == snapshot(
        [
            {
                "futile": 0,
                "status": 429,
            },
            {
                "futile": 10,
                "status": 429,
            },
            {
                "futile": 20,
                "status": 429,
            },
        ]
    )
    print(result)


@mark.parametrize("url", ["http://localhost:8000/factorial"])
def test_api_call_factorial(client, url):
    """Response includes a `number` key and a `factorial` value key."""
    df = pl.DataFrame({"number": [1, 2, 3]})
    response = httpl.api_call("number", endpoint=url).alias("response")
    parsed = jsonpath(response, text=True)
    result_pre = df.with_columns(parsed)
    result = result_pre.with_columns([
        pl.col("response").struct.field("number").alias("supplied"),
        pl.col("response").struct.field("factorial").alias("permutations"),
    ]).drop("response")
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "supplied": 1, "permutations": 1},
            {"number": 2, "supplied": 2, "permutations": 2},
            {"number": 3, "supplied": 3, "permutations": 6},
        ]
    )
    print(result)
