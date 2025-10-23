from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import CompliantLazyFrame
from narwhals._utils import Implementation, ValidateBackendVersion, not_implemented
if TYPE_CHECKING:
    import datafusion
    from narwhals.dataframe import LazyFrame
    from narwhals_datafusion.expr import DataFusionExpr
    from narwhals._utils import Version
    from narwhals.dtypes import DType


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

    from_native: not_implemented = not_implemented()
    to_narwhals: not_implemented = not_implemented()
    __native_namespace__: not_implemented = not_implemented()
    __narwhals_namespace__: not_implemented = not_implemented()
    _with_native: not_implemented = not_implemented()
    _with_version: not_implemented = not_implemented()
    _is_native: not_implemented = not_implemented()
    columns: not_implemented = not_implemented()
    schema: not_implemented = not_implemented()
    collect_schema: not_implemented = not_implemented()
    drop: not_implemented = not_implemented()
    drop_nulls: not_implemented = not_implemented()
    explode: not_implemented = not_implemented()
    filter: not_implemented = not_implemented()
    group_by: not_implemented = not_implemented()
    head: not_implemented = not_implemented()
    join: not_implemented = not_implemented()
    join_asof: not_implemented = not_implemented()
    rename: not_implemented = not_implemented()
    select: not_implemented = not_implemented()
    simple_select: not_implemented = not_implemented()
    sort: not_implemented = not_implemented()
    tail: not_implemented = not_implemented()
    unique: not_implemented = not_implemented()
    unpivot: not_implemented = not_implemented()
    with_columns: not_implemented = not_implemented()
    with_row_index: not_implemented = not_implemented()
    __narwhals_lazyframe__: not_implemented = not_implemented()
    _iter_columns: not_implemented = not_implemented()
    aggregate: not_implemented = not_implemented()
    collect: not_implemented = not_implemented()
    sink_parquet: not_implemented = not_implemented()