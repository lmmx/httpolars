from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from httpolars.utils import parse_into_expr, parse_version, register_plugin

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent


def pig_latinnify(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pig_latinnify",
        is_elementwise=True,
        lib=lib,
    )


def noop(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)

    return expr.register_plugin(
        lib=lib,
        symbol="noop",
        is_elementwise=True,
    )
