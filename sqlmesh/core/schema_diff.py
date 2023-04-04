from __future__ import annotations

import typing as t
from enum import Enum, auto

from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter import EngineAdapter


class SchemaDeltaOp(Enum):
    ADD = auto()
    DROP = auto()
    ALTER_TYPE = auto()


class Columns(PydanticModel):
    columns: t.List[t.Tuple[str, exp.DataType]]

    @classmethod
    def create(cls, column_name: str, column_type: t.Union[str, exp.DataType]) -> Columns:
        return cls(columns=[(column_name, exp.DataType.build(column_type))])

    @classmethod
    def empty(cls) -> Columns:
        return cls(columns=[])

    def add(self, name: str, type: exp.DataType) -> Columns:
        # We don't need the full struct kwargs, just the fact it was a struct
        if type.this == exp.DataType.Type.STRUCT:
            type = exp.DataType.build("STRUCT")
        if type.this == exp.DataType.Type.ARRAY:
            type = exp.DataType.build("ARRAY")
        return Columns(columns=self.columns + [(name, type)])

    @property
    def contains_struct(self) -> bool:
        return any(c[1] == exp.DataType.Type.STRUCT for c in self.columns)

    @property
    def contains_array(self) -> bool:
        return any(c[1] == exp.DataType.Type.ARRAY for c in self.columns)

    @property
    def has_columns(self) -> bool:
        return len(self.columns) > 0

    def sql(self, array_suffix: t.Optional[str] = None) -> str:
        return ".".join(
            [
                c[0]
                if c[1] != exp.DataType.Type.ARRAY
                else f"{c[0]}{array_suffix if array_suffix else ''}"
                for c in self.columns
            ]
        )


class ColumnPosition(PydanticModel):
    is_first: bool
    is_last: bool
    after: t.Optional[Columns] = None

    @classmethod
    def create_first(cls) -> ColumnPosition:
        return cls(is_first=True, is_last=False, after=None)

    @classmethod
    def create_last(cls, after: t.Optional[Columns] = None) -> ColumnPosition:
        return cls(is_first=False, is_last=True, after=after)

    @classmethod
    def create_middle(cls, after: Columns) -> ColumnPosition:
        return cls(is_first=False, is_last=False, after=after)

    @classmethod
    def create(
        cls, pos: int, current_kwargs: t.List[exp.StructKwarg], parent_cols: Columns
    ) -> ColumnPosition:
        is_first = pos == 0
        is_last = pos == len(current_kwargs)
        after = None
        if not is_first:
            prior_kwarg = current_kwargs[pos - 1]
            after = parent_cols.add(*_get_name_and_type(prior_kwarg))
        return cls(is_first=is_first, is_last=is_last, after=after)


class SchemaDelta(PydanticModel):
    column_name: str
    column_type: exp.DataType
    op: SchemaDeltaOp
    add_position: t.Optional[t.Union[str, ColumnPosition]] = None

    @classmethod
    def add(
        cls,
        column_name: str,
        column_type: t.Union[str, exp.DataType],
        position: ColumnPosition = ColumnPosition.create_last(),
    ) -> SchemaDelta:
        return cls(
            column_name=column_name,
            column_type=exp.DataType.build(column_type),
            op=SchemaDeltaOp.ADD,
            add_position=position,
        )

    @classmethod
    def drop(
        cls, column_name: str, column_type: t.Optional[t.Union[str, exp.DataType]] = None
    ) -> SchemaDelta:
        column_type = exp.DataType.build(column_type) if column_type else exp.DataType.build("INT")
        return cls(column_name=column_name, column_type=column_type, op=SchemaDeltaOp.DROP)

    @classmethod
    def alter_type(cls, column_name: str, column_type: t.Union[str, exp.DataType]) -> SchemaDelta:
        return cls(
            column_name=column_name,
            column_type=exp.DataType.build(column_type),
            op=SchemaDeltaOp.ALTER_TYPE,
        )

    @property
    def column_def(self) -> exp.ColumnDef:
        return exp.ColumnDef(
            this=exp.to_identifier(self.column_name),
            kind=self.column_type,
        )


def _get_name_and_type(struct: exp.StructKwarg) -> t.Tuple[str, exp.DataType]:
    return struct.alias_or_name, struct.expression


def struct_diff(
    current_struct: exp.DataType,
    new_struct: exp.DataType,
    parent_columns: t.Optional[Columns] = None,
) -> t.List[SchemaDelta]:
    def get_matching_kwarg(
        current_kwarg: exp.StructKwarg, new_struct: exp.DataType, current_pos: int
    ) -> t.Tuple[t.Optional[int], t.Optional[exp.StructKwarg]]:
        current_name, current_type = _get_name_and_type(current_kwarg)
        # First check if we have the same column in the same position to get O(1) complexity
        new_kwarg = seq_get(new_struct.expressions, current_pos)
        if new_kwarg:
            new_name, new_type = _get_name_and_type(new_kwarg)
            if current_name == new_name:
                return current_pos, new_kwarg
        # If not, check if we have the same column in a all positions to get O(n) complexity
        for i, new_kwarg in enumerate(new_struct.expressions):
            new_name, new_type = _get_name_and_type(new_kwarg)
            if current_name == new_name:
                return i, new_kwarg
        return None, None

    def get_column_name(name: str, parents: Columns) -> str:
        if parents.has_columns:
            return ".".join([parents.sql(), name])
        return name

    parent_columns = parent_columns or Columns(columns=[])
    operations = []
    # Resolve all drop columns
    pop_offset = 0
    for current_pos, current_kwarg in enumerate(current_struct.expressions[:]):
        new_pos, _ = get_matching_kwarg(current_kwarg, new_struct, current_pos)
        if new_pos is None:
            operations.append(
                SchemaDelta.drop(
                    get_column_name(current_kwarg.alias_or_name, parent_columns),
                    current_kwarg.expression,
                )
            )
            current_struct.expressions.pop(current_pos - pop_offset)
            pop_offset += 1
    # Resolve all add columns
    for new_pos, new_kwarg in enumerate(new_struct.expressions):
        possible_current_pos, _ = get_matching_kwarg(new_kwarg, current_struct, new_pos)
        if possible_current_pos is None:
            col_pos = ColumnPosition.create(new_pos, current_struct.expressions, parent_columns)
            operations.append(
                SchemaDelta.add(
                    get_column_name(new_kwarg.alias_or_name, parent_columns),
                    new_kwarg.expression,
                    col_pos,
                )
            )
            current_struct.expressions.insert(new_pos, new_kwarg)
    # Resolve all column type changes
    for current_pos, current_kwarg in enumerate(current_struct.expressions):
        new_pos, new_kwarg = get_matching_kwarg(current_kwarg, new_struct, current_pos)
        new_name, new_type = _get_name_and_type(new_kwarg)
        current_name, current_type = _get_name_and_type(current_kwarg)
        if new_type == current_type:
            continue
        elif (
            new_type.this == exp.DataType.Type.STRUCT
            and current_type.this == exp.DataType.Type.STRUCT
        ):
            operations.extend(
                struct_diff(
                    current_type,
                    new_type,
                    parent_columns.add(current_name, current_type),
                )
            )
        elif (
            new_type.this == exp.DataType.Type.ARRAY
            and current_type.this == exp.DataType.Type.ARRAY
        ):
            new_array_type = new_type.expressions[0]
            current_array_type = current_type.expressions[0]
            if (
                new_array_type.this == exp.DataType.Type.STRUCT
                and current_array_type.this == exp.DataType.Type.STRUCT
            ):
                operations.extend(
                    struct_diff(
                        current_array_type,
                        new_array_type,
                        parent_columns.add(current_name, current_type),
                    )
                )
            else:
                operations.append(
                    SchemaDelta.alter_type(
                        get_column_name(current_name, parent_columns), new_array_type
                    )
                )
        else:
            operations.append(
                SchemaDelta.alter_type(get_column_name(current_name, parent_columns), new_type)
            )
        current_struct.expressions.pop(current_pos)
        current_struct.expressions.insert(current_pos, new_kwarg)
    return operations


def table_diff(
    current_table: str, new_table: str, engine_adapter: EngineAdapter
) -> t.List[SchemaDelta]:
    def dict_to_struct(value: t.Dict[str, exp.DataType]) -> exp.DataType:
        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[
                exp.StructKwarg(this=k, expression=exp.DataType.build(v)) for k, v in value.items()
            ],
        )

    current_struct = dict_to_struct(engine_adapter.columns(current_table))
    new_struct = dict_to_struct(engine_adapter.columns(new_table))
    return struct_diff(current_struct, new_struct, Columns.empty())
