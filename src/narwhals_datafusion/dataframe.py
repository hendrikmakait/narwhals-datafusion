from __future__ import annotations

from sys import implementation
from typing import TYPE_CHECKING

from narwhals._compliant import CompliantLazyFrame
from narwhals._utils import Implementation, ValidateBackendVersion, not_implemented, parse_columns_to_drop
from narwhals._arrow.utils import native_to_narwhals_dtype
from narwhals_datafusion.utils import evaluate_exprs

if TYPE_CHECKING:
    import datafusion
    from collections.abc import Mapping
    from narwhals.dataframe import LazyFrame
    from narwhals_datafusion.expr import DataFusionExpr
    from collections.abc import Sequence
    from narwhals_datafusion.namespace import DataFusionNamespace
    from narwhals._utils import Version
    from narwhals.dtypes import DType
    from narwhals._utils import _LimitedContext
    from typing_extensions import Self, TypeIs
    from narwhals._compliant.typing import CompliantDataFrameAny
    from io import BytesIO
    from pathlib import Path
    from typing import Any
    from types import ModuleType


class DataFusionLazyFrame(
    CompliantLazyFrame["DataFusionExpr", "datafusion.DataFrame", "LazyFrame[datafusion.DataFrame]"],
    ValidateBackendVersion,
):
    _implementation = Implementation.UNKNOWN

    def __init__(
        self,
        native_dataframe: datafusion.DataFrame,
        *,
        version: Version,
        validate_backend_version: bool = False,
    ) -> None:
        self._native_frame: datafusion.DataFrame = native_dataframe
        self._version = version
        self._cached_schema: dict[str, DType] | None = None
        self._cached_columns: list[str] | None = None
        if validate_backend_version:
            self._validate_backend_version()

    @staticmethod
    def _is_native(obj: datafusion.DataFrame | Any) -> TypeIs[datafusion.DataFrame]:
        return isinstance(obj, datafusion.DataFrame)

    @classmethod
    def from_native(cls, data: datafusion.DataFrame, /, *, context: _LimitedContext) -> Self:
        return cls(data, version=context._version)
    
    def to_narwhals(self) -> LazyFrame[datafusion.DataFrame]:
        return self._version.lazyframe(self, level="lazy")

    def __native_namespace__(self) -> ModuleType:
        import datafusion
    
        return datafusion

    def __narwhals_namespace__(self) -> DataFusionNamespace:
        from narwhals_datafusion.namespace import DataFusionNamespace

        return DataFusionNamespace(version=self._version)

    def __narwhals_lazyframe__(self) -> Self:
        return self

    def _with_native(self, df: datafusion.DataFrame) -> Self:
        return self.__class__(df, version=self._version)

    def _with_version(self, version: Version) -> Self:
        return self.__class__(self.native, version=version)
    
    def _evaluate_expr(self, expr: DataFusionExpr) -> datafusion.Expr:
        result = expr(self)
        assert len(result) == 1
        return result[0]

    @property
    def columns(self) -> list[str]:
        if self._cached_columns is None:
            self._cached_columns = list(self.schema)
        return self._cached_columns

    @property
    def schema(self) -> dict[str, DType]:
        if self._cached_schema is None:
            # Note: prefer `self._cached_schema` over `functools.cached_property`
            # due to Python3.13 failures.
            self._cached_schema = self.collect_schema()
        return self._cached_schema

    def collect_schema(self) -> dict[str, DType]:
        return {
            field.name: native_to_narwhals_dtype(field.type, self._version)
            for field in self.native.schema()
        }

    def drop(self, columns: Sequence[str], *, strict: bool) -> Self:
        columns_to_drop = parse_columns_to_drop(self, columns, strict=strict)
        return self._with_native(self.native.drop(columns_to_drop))

    drop_nulls: not_implemented = not_implemented()
    explode: not_implemented = not_implemented()
    filter: not_implemented = not_implemented()
    group_by: not_implemented = not_implemented()
    
    def head(self, n: int) -> Self:
        return self._with_native(self.native.head(n))

    join: not_implemented = not_implemented()
    join_asof: not_implemented = not_implemented()

    def rename(self, mapping: Mapping[str, str]) -> Self:
        selection = [
            datafusion.col(col).alias(mapping[col]) if col in mapping else col
            for col in self.columns
        ]
        return self._with_native(self.native.select(*selection))

    select: not_implemented = not_implemented()
    def select(self, *exprs: DataFusionExpr) -> Self:
        new_columns_map = evaluate_exprs(self, *exprs)
        if not new_columns_map:
            msg = "At least one expression must be passed to LazyFrame.select"
            raise ValueError(msg)
        try:
            return self._with_native(self.native.select(*(val.alias(col) for col, val in new_columns_map)))
        except Exception as e:
            # TODO: Improve error handling
            raise e
    
    def simple_select(self, *column_names: str) -> Self:
        return self._with_native(self.native.select_columns(*column_names))

    sort: not_implemented = not_implemented()

    def tail(self, n: int) -> Self:
        return self._with_native(self.native.tail(n))

    unique: not_implemented = not_implemented()
    unpivot: not_implemented = not_implemented()

    def with_columns(self, *exprs: DataFusionExpr) -> Self:
        new_columns_map = dict(evaluate_exprs(self, *exprs))
        return self._with_native(self.native.with_columns(**new_columns_map))

    with_row_index: not_implemented = not_implemented()
    _iter_columns: not_implemented = not_implemented()
    aggregate: not_implemented = not_implemented()

    def collect(self, backend: ModuleType | Implementation | str | None, **kwargs: Any) -> CompliantDataFrameAny:
        if backend is None or backend is Implementation.PYARROW:
            from narwhals._arrow.dataframe import ArrowDataFrame

            return ArrowDataFrame(
                native_dataframe=self.native.to_arrow_table(),
                validate_backend_version=True,
                version=self._version,
                validate_column_names=True,
            )
        if backend is Implementation.PANDAS:
            from narwhals._pandas_like.dataframe import PandasLikeDataFrame

            return PandasLikeDataFrame(
                native_dataframe=self.native.to_pandas(),
                implementation=Implementation.PANDAS,
                validate_backend_version=True,
                version=self._version,
                validate_column_names=True,
            )
        
        if backend is Implementation.POLARS:
            from narwhals._polars.dataframe import PolarsDataFrame

            return PolarsDataFrame(
                df=self.native.to_polars(),
                validate_backend_version=True,
                version=self._version,
            )

        msg = f"Unsupported `backend` value: {backend}"
        raise ValueError(msg)

    def sink_parquet(self, file: str | Path | BytesIO) -> None:
        return self.native.write_parquet(file)
