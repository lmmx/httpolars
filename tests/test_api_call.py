import polars as pl
from inline_snapshot import snapshot
from pytest import mark

import httpolars as httpl


def jsonpath(response: str | pl.Expr, *, path: str = "", status_code: bool = False):
    """Accept either the response `Expr` or reference by its column name."""
    response = pl.col(response) if isinstance(response, str) else response
    if path:
        return response.str.json_path_match(f"$.text.{path}")
    elif status_code:
        return response.str.json_path_match(f"$.status_code")
    else:
        return response


@mark.parametrize("url", ["http://localhost:8000/noop"])
def test_api_call_noop(client, url):
    """the response gives back the input, and the column is overwritten unchanged."""
    df = pl.DataFrame({"value": ["x", "y", "z"]})
    response = httpl.api_call("value", endpoint=url)
    # value = jsonpath("response", path="value").alias("response")
    result = df.with_columns(response) #.with_columns(value)
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
    in_ = jsonpath("response", "number").str.to_integer().alias("supplied")
    out = jsonpath("response", "factorial").str.to_integer().alias("permutations")
    result = df.with_columns(response).with_columns([in_, out]).drop("response")
    assert result.to_dicts() == snapshot(
        [
            {"number": 1, "supplied": 1, "permutations": 1},
            {"number": 2, "supplied": 2, "permutations": 2},
            {"number": 3, "supplied": 3, "permutations": 6},
        ]
    )
    print(result)
