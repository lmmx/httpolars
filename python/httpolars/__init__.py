from __future__ import annotations

import inspect
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .utils import parse_into_expr, parse_version
from ._lib import ApiClient, create_api_client

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

__all__ = ["api_call", "ApiClient", "create_api_client"]


def plug(expr, **kwargs) -> pl.Expr:
    """Wrap the `register_expr_plugin` helper to always pass `lib` (invariant)."""
    func_name = inspect.stack()[1].function
    return parse_into_expr(expr).register_plugin(symbol=func_name, lib=lib, **kwargs)


def api_call(expr: IntoExpr, *, endpoint: str) -> pl.Expr:
    return plug(expr, is_elementwise=True, kwargs={"endpoint": endpoint})
