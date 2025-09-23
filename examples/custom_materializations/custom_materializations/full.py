from __future__ import annotations

import typing as t

from sqlmesh import CustomMaterialization, Model

if t.TYPE_CHECKING:
    from sqlmesh import QueryOrDF


class CustomFullMaterialization(CustomMaterialization):
    NAME = "custom_full"

    def insert(
        self,
        table_name: str,
        query_or_df: QueryOrDF,
        model: Model,
        is_first_insert: bool,
        render_kwargs: t.Dict[str, t.Any],
        **kwargs: t.Any,
    ) -> None:
        self._replace_query_for_model(model, table_name, query_or_df, render_kwargs)
