import httpx
from pytest import mark


def test_read_root_test_client(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}


@mark.skip(reason="Server subprocess doesn't work correctly")
def test_read_root_subprocess_client(test_server):
    """Manually run uvicorn instead. The command is:

    ```
    uvicorn httpolars.test_utils.rate_limit_server:app --host 127.0.0.1 --port 8000
    ```
    """
    response = httpx.get("http://127.0.0.1:8000/")
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}
