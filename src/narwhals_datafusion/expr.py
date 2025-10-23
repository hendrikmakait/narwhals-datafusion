from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import LazyExpr
from narwhals._utils import Implementation

if TYPE_CHECKING:
    import datafusion
    from narwhals_datafusion.dataframe import DataFusionLazyFrame


class DataFusionExpr(LazyExpr["DataFusionLazyFrame", "datafusion.Expr"]):
    _implementation = Implementation.UNKNOWN