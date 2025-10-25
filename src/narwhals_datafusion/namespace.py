from __future__ import annotations

from typing import TYPE_CHECKING, Any
from narwhals._arrow.utils import narwhals_to_native_dtype
from narwhals._compliant.namespace import LazyNamespace
from narwhals._utils import Implementation, not_implemented

import datafusion
from narwhals_datafusion.dataframe import DataFusionLazyFrame
from narwhals_datafusion.expr import DataFusionExpr

if TYPE_CHECKING:
    from narwhals._utils import Version
    from narwhals.dtypes import DType

class DataFusionNamespace(
    LazyNamespace[DataFusionLazyFrame, DataFusionExpr, "datafusion.DataFrame"]
):
    _implementation = Implementation.UNKNOWN

    def __init__(self, *, version: Version) -> None:
        self._version = version
    
    def from_native(self, native_object: datafusion.DataFrame) -> DataFusionLazyFrame:
        return DataFusionLazyFrame(native_object, version=self._version)
    
    @property
    def _expr(self) -> type[DataFusionExpr]:
        return DataFusionExpr
    
    @property
    def _lazyframe(self) -> type[DataFusionLazyFrame]:
        return DataFusionLazyFrame
    
    len: not_implemented = not_implemented()

    def lit(self, value: Any, dtype: DType | type[DType] | None) -> DataFusionExpr:
        def func(_df: DataFusionLazyFrame) -> list[datafusion.Expr]:
            if dtype is not None:
                return [datafusion.lit(value).cast(narwhals_to_native_dtype(dtype, self._version))]
            return [datafusion.lit(value)]
        
        return DataFusionExpr(
            func,
            evaluate_output_names=lambda _df: ["literal"],
            alias_output_names=None,
            version=self._version,
        )

    all_horizontal: not_implemented = not_implemented()
    any_horizontal: not_implemented = not_implemented()
    sum_horizontal: not_implemented = not_implemented()
    mean_horizontal: not_implemented = not_implemented()
    min_horizontal: not_implemented = not_implemented()
    max_horizontal: not_implemented = not_implemented()
    concat: not_implemented = not_implemented()
    when: not_implemented = not_implemented()
    concat_str: not_implemented = not_implemented()
    selectors: not_implemented = not_implemented()
    coalesce: not_implemented = not_implemented()
    is_native: not_implemented = not_implemented() # TODO: Do we need this?
