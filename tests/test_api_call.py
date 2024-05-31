import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def jsonpath(response: str | pl.Expr, *, text: bool = False, status_code: bool = False):
    """Accept either the response `Expr` or reference by its column name."""
    response = pl.col(response) if isinstance(response, str) else response
    if (text and status_code) or not (text or status_code):
        raise NotImplementedError
    subpath = response.str.json_path_match
    if text:
        return subpath("$.text").str.json_decode()
    if status_code:
        return subpath("$.status_code").str.json_decode().alias("status")


@mark.parametrize("url", ["http://localhost:8000/noop"])
def test_api_call_noop(url):
    """the response gives back the input, and the column is overwritten unchanged."""
    df = pl.DataFrame({"value": ["x", "y", "z"]})
    response = httpl.api_call("value", endpoint=url)
    value = jsonpath("value", text=True)
    result_pre = df.with_columns(response)
    result = result_pre.select(value).select(pl.col("value").struct.field("value"))
    assert result.to_dicts() == snapshot(
        [{"value": "x"}, {"value": "y"}, {"value": "z"}]
    )
    print(result)


@mark.parametrize("url", ["http://localhost:8000/permafailure"])
def test_api_call_permafailure_keep_status(url):
    """Response is never obtained, always fails (429)."""
    df = pl.DataFrame({"futile": [0, 10, 20]})
    response = httpl.api_call("futile", endpoint=url).alias("response")
    status = jsonpath(response, status_code=True)
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
def test_api_call_factorial(url):
    """Response includes a `number` key and a `factorial` value key."""
    df = pl.DataFrame({"number": [1, 2, 3]})
    response = httpl.api_call("number", endpoint=url).alias("response")
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
