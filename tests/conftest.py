from pytest import fixture
from time import sleep
import subprocess

from fastapi.testclient import TestClient
from httpolars.test_utils.rate_limit_server import app
import polars as pl


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


@fixture
def client():
    return TestClient(app)


# @fixture(scope="module")
def test_server():
    # Start the server in a separate process
    server_path = "httpolars.test_utils.rate_limit_server"
    process = subprocess.Popen(["uvicorn", f"{server_path}:app", "--host", "127.0.0.1", "--port", "8000"])
    sleep(1)  # Give the server some time to start

    yield

    # Terminate the server after tests
    process.terminate()
    process.wait()
