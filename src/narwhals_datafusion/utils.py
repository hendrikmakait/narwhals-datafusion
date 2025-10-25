from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datafusion
    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    from narwhals_datafusion.expr import DataFusionExpr

def evaluate_exprs(
    df: DataFusionLazyFrame, /, *exprs: DataFusionExpr
) -> list[tuple[str, datafusion.Expr]]:
    native_results: list[tuple[str, datafusion.Expr]] = []
    for expr in exprs:
        native_series_list = expr._call(df)
        output_names = expr._evaluate_output_names(df)
        if expr._alias_output_names is not None:
            output_names = expr._alias_output_names(output_names)
        if len(output_names) != len(native_series_list):  # pragma: no cover
            msg = f"Internal error: got output names {output_names}, but only got {len(native_series_list)} results"
            raise AssertionError(msg)
        native_results.extend(zip(output_names, native_series_list))
    return native_results
