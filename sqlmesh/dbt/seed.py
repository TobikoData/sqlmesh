from __future__ import annotations

import copy
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
from sqlmesh.dbt.column import ColumnConfig

if t.TYPE_CHECKING:
    from sqlmesh.core.audit.definition import ModelAudit
    from sqlmesh.dbt.context import DbtContext


class SeedConfig(BaseModelConfig):
    """
    seedConfig contains all config parameters available to DBT seeds

    See https://docs.getdbt.com/reference/configs-and-properties for
    a more detailed description of each config parameter under the
    General propreties, General configs, and For seeds sections.
    """

    delimiter: str = ","
    column_types: t.Optional[t.Dict[str, str]] = None
    quote_columns: t.Optional[bool] = False

    def to_sqlmesh(
        self, context: DbtContext, audit_definitions: t.Optional[t.Dict[str, ModelAudit]] = None
    ) -> Model:
        """Converts the dbt seed into a SQLMesh model."""
        seed_path = self.path.absolute().as_posix()

        if column_types := self.column_types:
            column_types_override = copy.deepcopy(self.columns)
            for name, data_type in column_types.items():
                column = column_types_override.setdefault(name, ColumnConfig(name=name))
                column.data_type = data_type
                column.quote = self.quote_columns or column.quote
                kwargs = self.sqlmesh_model_kwargs(context, column_types_override)
        else:
            kwargs = self.sqlmesh_model_kwargs(context)

        columns = kwargs.get("columns") or {}
        descriptions = kwargs.get("column_descriptions") or {}
        missing_types = (set(descriptions) | set(self.columns)) - set(columns)
        if not columns or missing_types:
            agate_table = (
                agate_helper.from_csv(seed_path, [], delimiter=self.delimiter)
                if SUPPORTS_DELIMITER
                else agate_helper.from_csv(seed_path, [])
            )
            inferred_types = {
                name: AGATE_TYPE_MAPPING[tpe.__class__]
                for name, tpe in zip(agate_table.column_names, agate_table.column_types)
            }

            # The columns list built from the mixture of supplied and inferred types needs to
            # be in the same order as the data for assumptions elsewhere in the codebase to hold true
            new_columns = {}
            for column_name in agate_table.column_names:
                if (column_name in missing_types) or (column_name not in columns):
                    new_columns[column_name] = inferred_types[column_name]
                else:
                    new_columns[column_name] = columns[column_name]

            kwargs["columns"] = new_columns

        return create_seed_model(
            self.canonical_name(context),
            SeedKind(path=seed_path),
            dialect=self.dialect(context),
            audit_definitions=audit_definitions,
            **kwargs,
        )


class Integer(agate_helper.Integer):
    def cast(self, d: t.Any) -> t.Optional[int]:
        if isinstance(d, str):
            # The dbt's implementation doesn't support coercion of strings to integers.
            if d.strip().lower() in self.null_values:
                return None
            try:
                return int(d)
            except ValueError:
                raise agate.exceptions.CastError('Can not parse value "%s" as Integer.' % d)
        return super().cast(d)

    def jsonify(self, d: t.Any) -> str:
        return d


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
