from __future__ import annotations

import typing as t

from sqlmesh.core.model import Model, SeedKind, create_seed_model
from sqlmesh.dbt.basemodel import BaseModelConfig

if t.TYPE_CHECKING:
    from sqlmesh.dbt.context import DbtContext


class SeedConfig(BaseModelConfig):
    """
    seedConfig contains all config parameters available to DBT seeds

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For seeds sections.
    """

    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Converts the dbt seed into a SQLMesh model."""
        return create_seed_model(
            self.canonical_name(context),
            SeedKind(path=self.path.absolute().as_posix()),
            dialect=context.dialect,
            **self.sqlmesh_model_kwargs(context),
        )
