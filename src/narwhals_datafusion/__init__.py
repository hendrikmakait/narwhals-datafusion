from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from narwhals_datafusion.namespace import DataFusionNamespace
    from narwhals_datafusion.dataframe import DataFusionLazyFrame
    
    from narwhals.utils import Version
    from typing_extensions import TypeIs


def __narwhals_namespace__(version: Version) -> DataFusionNamespace:
    from narwhals_datafusion.namespace import DataFusionNamespace

    return DataFusionNamespace(version=version)

def is_native(native_object:object) -> TypeIs[DataFusionLazyFrame]:
    import datafusion

    return isinstance(native_object, datafusion.DataFrame)


NATIVE_PACKAGE = "datafusion"