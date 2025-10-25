from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import LazyExpr
from narwhals._utils import Implementation, not_implemented
import datafusion

if TYPE_CHECKING:
    from collections.abc import Sequence, Callable
    import datafusion
    from narwhals._compliant.typing import AliasNames, EvalNames, EvalSeries, WindowFunction
    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    from narwhals._expression_parsing import ExprMetadata
    from narwhals._utils import Version
    from narwhals_datafusion.namespace import DataFusionNamespace
    from typing_extensions import Self, TypeIs
    from typing import Any
    from narwhals._utils import Version, _LimitedContext

class DataFusionExpr(LazyExpr["DataFusionLazyFrame", "datafusion.Expr"]):
    _implementation = Implementation.UNKNOWN

    def __init__(
        self,
        call: Callable[[DataFusionLazyFrame], Sequence[datafusion.Expr]],
        *,
        evaluate_output_names: EvalNames[DataFusionLazyFrame],
        alias_output_names: AliasNames | None,
        version: Version,
    ) -> None:
        self._call = call
        self._evaluate_output_names = evaluate_output_names
        self._alias_output_names = alias_output_names
        self._version = version
        self._metadata: ExprMetadata | None = None
    
    def __call__(self, df: DataFusionLazyFrame) -> Sequence[datafusion.Expr]:
        return self._call(df)

    def __narwhals_namespace__(self) -> DataFusionNamespace:
        # Unused, just for compatibility with PandasLikeExpr
        from narwhals_datafusion.namespace import DataFusionNamespace

        return DataFusionNamespace(version=self._version)

    @classmethod
    def _alias_native(cls, expr: datafusion.Expr, name: str) -> datafusion.Expr:
        return expr.alias(name)

    @classmethod
    def from_column_names(
        cls: type[Self],
        evaluate_column_names: EvalNames[DataFusionLazyFrame],
        /,
        *,
        context: _LimitedContext,
    ) -> Self:
        def func(df: DataFusionLazyFrame) -> list[datafusion.Expr]:
            return [datafusion.col(col_name) for col_name in evaluate_column_names(df)]

        return cls(
            func,
            evaluate_output_names=evaluate_column_names,
            alias_output_names=None,
            version=context._version,
        )

    @classmethod
    def from_column_indices(
        cls: type[Self], *column_indices: int, context: _LimitedContext
    ) -> Self:
        def func(df: DataFusionLazyFrame) -> list[datafusion.Expr]:
            columns = df.columns
            return [datafusion.col(columns[i]) for i in column_indices]

        return cls(
            func,
            evaluate_output_names=lambda df: [df.columns[i] for i in column_indices],
            alias_output_names=None,
            version=context._version,
        )




    @classmethod
    def _is_expr(cls, obj: Self | Any) -> TypeIs[Self]:
        return hasattr(obj, "__narwhals_expr__")

    def _callable_to_eval_series(
        self, call: Callable[..., datafusion.Expr], /, **expressifiable_args: Self | Any
    ) -> EvalSeries[DataFusionLazyFrame, datafusion.Expr]:
        def func(df: DataFusionLazyFrame) -> list[datafusion.Expr]:
            native_series_list = self(df)
            other_native_series = {
                key: df._evaluate_expr(value) if self._is_expr(value) else datafusion.lit(value)
                for key, value in expressifiable_args.items()
            }
            return [
                call(native_series, **other_native_series)
                for native_series in native_series_list
            ]

        return func

    def _with_elementwise(
        self, call: Callable[..., datafusion.Expr], /, **expressifiable_args: Self | Any
    ) -> Self:
        return self.__class__(
            self._callable_to_eval_series(call, **expressifiable_args),
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def _with_binary(self, op: Callable[..., datafusion.Expr], other: Self | Any) -> Self:
        return self.__class__(
            self._callable_to_eval_series(op, other=other),
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def __and__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr & other), other=other)

    def __or__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr | other), other=other)
