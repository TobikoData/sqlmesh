from __future__ import annotations

import itertools
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
        return cls(
            column_name=column_name, column_type=column_type, op=SchemaDeltaOp.ADD
        )

    @classmethod
    def drop(cls, column_name: str, column_type: str) -> SchemaDelta:
        return cls(
            column_name=column_name, column_type=column_type, op=SchemaDeltaOp.DROP
        )

    @classmethod
    def alter_type(cls, column_name: str, column_type: str) -> SchemaDelta:
        return cls(
            column_name=column_name,
            column_type=column_type,
            op=SchemaDeltaOp.ALTER_TYPE,
        )


class SchemaDiffCalculator:
    """Claculates the difference between table schema.s

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
        self.is_type_transition_allowed = is_type_transition_allowed or (
            lambda src, tgt: False
        )

    def calculate(self, source_table: str, target_table: str) -> t.List[SchemaDelta]:
        """Calculates a list of schema deltas between the source and the target tables, applying which
        in order to the source table brings its schema in correspondence with the schema of the target
        table.

        Changes in positions of otherwise unchanged columns are currently ignored and are not reflected in the output.

        Additionally the implementation currently doesn't differentiate between regular columns and partition ones.
        It's a responsibility of a caller to determine whether a returned operation is allowed on partition columns or not.

        Args:
            source_table: The name of the source table.
            target_table: The name of the target table.

        Returns:
            The list of deltas.
        """
        source_schema = self._fetch_table_schema(source_table)
        target_schema = self._fetch_table_schema(target_table)

        result = []

        for source_column_name, source_column_type in source_schema.items():
            target_column_type = target_schema.get(source_column_name)

            if target_column_type != source_column_type:
                if target_column_type and self.is_type_transition_allowed(
                    source_column_type, target_column_type
                ):
                    result.append(
                        SchemaDelta.alter_type(source_column_name, target_column_type)
                    )
                else:
                    result.append(
                        SchemaDelta.drop(source_column_name, source_column_type)
                    )
                    if target_column_type:
                        result.append(
                            SchemaDelta.add(source_column_name, target_column_type)
                        )

        for target_column_name, target_column_type in target_schema.items():
            if target_column_name not in source_schema:
                result.append(SchemaDelta.add(target_column_name, target_column_type))

        return result

    def _fetch_table_schema(self, table_name: str) -> t.Dict[str, str]:
        describe_output = self.engine_adapter.describe_table(table_name)
        return {
            t[0]: t[1].upper()
            for t in itertools.takewhile(
                lambda t: not t[0].startswith("#"),
                describe_output,
            )
        }
