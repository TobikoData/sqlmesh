from __future__ import annotations

import typing as t

import agate

try:
    from dbt_common.clients import agate_helper  # type: ignore

    SUPPORTS_DELIMITER = True
except ImportError:
    from dbt.clients import agate_helper  # type: ignore

    SUPPORTS_DELIMITER = False
from sqlglot import exp

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

    delimiter: str = ","

    def to_sqlmesh(self, context: DbtContext) -> Model:
        """Converts the dbt seed into a SQLMesh model."""
        seed_path = self.path.absolute().as_posix()
        kwargs = self.sqlmesh_model_kwargs(context)
        if kwargs.get("columns") is None:
            agate_table = (
                agate_helper.from_csv(seed_path, [], delimiter=self.delimiter)
                if SUPPORTS_DELIMITER
                else agate_helper.from_csv(seed_path, [])
            )
            kwargs["columns"] = {
                name: AGATE_TYPE_MAPPING[tpe.__class__]
                for name, tpe in zip(agate_table.column_names, agate_table.column_types)
            }

        return create_seed_model(
            self.canonical_name(context),
            SeedKind(path=seed_path),
            dialect=context.dialect,
            **kwargs,
        )


class Integer(agate.data_types.DataType):
    def cast(self, d: str) -> t.Optional[int]:
        if d is None:
            return d
        try:
            return int(d)
        except ValueError:
            raise agate.exceptions.CastError('Can not parse value "%s" as Integer.' % d)

    def jsonify(self, d: str) -> str:
        return d


# The dbt version has a bug in which they check whether the type of the input value
# is int, while the input value is actually always a string.
agate_helper.Integer = Integer  # type: ignore


AGATE_TYPE_MAPPING = {
    agate_helper.Integer: exp.DataType.build("int"),
    agate_helper.Number: exp.DataType.build("double"),
    agate_helper.ISODateTime: exp.DataType.build("datetime"),
    agate.Date: exp.DataType.build("date"),
    agate.DateTime: exp.DataType.build("datetime"),
    agate.Boolean: exp.DataType.build("boolean"),
    agate.Text: exp.DataType.build("text"),
}
