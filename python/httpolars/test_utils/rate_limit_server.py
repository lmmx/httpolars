from math import factorial

from fastapi import FastAPI, Request
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.get("/noop")
@limiter.limit("400/2 seconds")
async def read_noop(request: Request, value: str | None = None):
    return {"value": value}


@app.get("/factorial")
@limiter.limit("50/minute")
async def read_factorial(request: Request, number: int | None = None):
    return {"number": number, "factorial": factorial(number)}


@app.get("/permafailure")
@limiter.limit("0/minute")
async def read_permafailure(request: Request, futile: int | None = None):
    return {}


def run_app():
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
