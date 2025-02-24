from __future__ import annotations

import typing as t

from sqlmesh import CustomMaterialization, CustomKind, Model
from sqlmesh.utils.pydantic import validate_string

if t.TYPE_CHECKING:
    from sqlmesh import QueryOrDF


class ExtendedCustomKind(CustomKind):
    @property
    def custom_property(self) -> str:
        return validate_string(self.materialization_properties.get("custom_property"))


class CustomFullWithCustomKindMaterialization(CustomMaterialization[ExtendedCustomKind]):
    NAME = "custom_full_with_custom_kind"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        **kwargs: t.Any,
    ) -> None:
        assert type(model.kind).__name__ == "ExtendedCustomKind"

        self._replace_query_for_model(model, table_name, query_or_df)
