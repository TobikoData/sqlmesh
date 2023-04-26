from __future__ import annotations

from sqlmesh.core.model import Model, SeedKind, create_seed_model
from sqlmesh.dbt.basemodel import BaseModelConfig
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
        model_context = self._context_for_dependencies(context, self.dependencies)

        return create_seed_model(
            self.model_name,
            SeedKind(path=self.path.absolute()),
            **self.sqlmesh_model_kwargs(
                model_context,
            ),
        )
