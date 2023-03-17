from __future__ import annotations

import typing as t
from enum import Enum, auto

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter import EngineAdapter


class SchemaDeltaOp(Enum):
    ADD = auto()
    DROP = auto()
    ALTER_TYPE = auto()


class SchemaDelta(PydanticModel):
    column_name: str
    column_type: str
    op: SchemaDeltaOp

    @classmethod
    def add(cls, column_name: str, column_type: str) -> SchemaDelta:
        return cls(column_name=column_name, column_type=column_type, op=SchemaDeltaOp.ADD)

    @classmethod
    def drop(cls, column_name: str, column_type: str) -> SchemaDelta:
        return cls(column_name=column_name, column_type=column_type, op=SchemaDeltaOp.DROP)

    @classmethod
    def alter_type(cls, column_name: str, column_type: str) -> SchemaDelta:
        return cls(
            column_name=column_name,
            column_type=column_type,
            op=SchemaDeltaOp.ALTER_TYPE,
        )


class SchemaDiffCalculator:
    """Calculates the difference between table schemas.

    Args:
        engine_adapter: The engine adapter.
        is_type_transition_allowed: The predicate which accepts the source type and the target type
            and returns True if the transition from source to target is allowed and False otherwise.
            Default: no type transitions are allowed.
    """

    def __init__(
        self,
        engine_adapter: EngineAdapter,
        is_type_transition_allowed: t.Optional[t.Callable[[str, str], bool]] = None,
    ):
        self.engine_adapter = engine_adapter
        self.is_type_transition_allowed = is_type_transition_allowed or (lambda src, tgt: False)

    def calculate(self, apply_to_table: str, schema_from_table: str) -> t.List[SchemaDelta]:
        """Calculates a list of schema deltas between the two tables, applying which in order to the first table
        brings its schema in correspondence with the schema of the second table.

        Changes in positions of otherwise unchanged columns are currently ignored and are not reflected in the output.

        Additionally the implementation currently doesn't differentiate between regular columns and partition ones.
        It's a responsibility of a caller to determine whether a returned operation is allowed on partition columns or not.

        Args:
            apply_to_table: The name of the table to which deltas will be applied.
            schema_from_table: The schema of this table will be used for comparison.

        Returns:
            The list of deltas.
        """
        to_schema = self.engine_adapter.columns(apply_to_table)
        from_schema = self.engine_adapter.columns(schema_from_table)

        result = []

        for to_column_name, to_column_type in to_schema.items():
            from_column_type = from_schema.get(to_column_name)
            from_column_type = from_column_type.upper() if from_column_type else None
            if from_column_type != to_column_type.upper():
                if from_column_type and self.is_type_transition_allowed(
                    to_column_type, from_column_type
                ):
                    result.append(SchemaDelta.alter_type(to_column_name, from_column_type))
                else:
                    result.append(SchemaDelta.drop(to_column_name, to_column_type))
                    if from_column_type:
                        result.append(SchemaDelta.add(to_column_name, from_column_type))

        for from_column_name, from_column_type in from_schema.items():
            if from_column_name not in to_schema:
                result.append(SchemaDelta.add(from_column_name, from_column_type))

        return result
