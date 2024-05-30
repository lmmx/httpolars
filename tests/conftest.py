from pytest import fixture
from time import sleep
import subprocess

from fastapi.testclient import TestClient
from httpolars.test_utils.rate_limit_server import app


@fixture
def client():
    return TestClient(app)


@fixture(scope="module")
def test_server():
    # Start the server in a separate process
    server_path = "httpolars.test_utils.rate_limit_server"
    process = subprocess.Popen(["uvicorn", f"{server_path}:app", "--host", "127.0.0.1", "--port", "8000"])
    sleep(1)  # Give the server some time to start

    yield

    # Terminate the server after tests
    process.terminate()
    process.wait()
