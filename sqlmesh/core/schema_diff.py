from __future__ import annotations

import typing as t
from enum import Enum, auto

from sqlglot import exp
from sqlglot.helper import ensure_list, seq_get

from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.engine_adapter import EngineAdapter


class TableAlterOperationType(Enum):
    ADD = auto()
    DROP = auto()
    ALTER_TYPE = auto()

    @property
    def is_add(self) -> bool:
        return self == TableAlterOperationType.ADD

    @property
    def is_drop(self) -> bool:
        return self == TableAlterOperationType.DROP

    @property
    def is_alter_type(self) -> bool:
        return self == TableAlterOperationType.ALTER_TYPE


class TableAlterColumn(PydanticModel):
    name: str
    is_struct: bool
    is_array_of_struct: bool
    is_array_of_primitive: bool

    @classmethod
    def primitive(self, name: str) -> TableAlterColumn:
        return self(
            name=name, is_struct=False, is_array_of_struct=False, is_array_of_primitive=False
        )

    @classmethod
    def struct(self, name: str) -> TableAlterColumn:
        return self(
            name=name, is_struct=True, is_array_of_struct=False, is_array_of_primitive=False
        )

    @classmethod
    def array_of_struct(self, name: str) -> TableAlterColumn:
        return self(
            name=name, is_struct=False, is_array_of_struct=True, is_array_of_primitive=False
        )

    @classmethod
    def array_of_primitive(self, name: str) -> TableAlterColumn:
        return self(
            name=name, is_struct=False, is_array_of_struct=False, is_array_of_primitive=True
        )

    @classmethod
    def from_struct_kwarg(self, struct: exp.StructKwarg) -> TableAlterColumn:
        name = struct.alias_or_name
        if struct.expression.is_type(exp.DataType.Type.STRUCT):
            return self.struct(name)
        elif struct.expression.is_type(exp.DataType.Type.ARRAY):
            if struct.expression.expressions[0].is_type(exp.DataType.Type.STRUCT):
                return self.array_of_struct(name)
            else:
                return self.array_of_primitive(name)
        else:
            return self.primitive(name)

    @property
    def is_array(self) -> bool:
        return self.is_array_of_struct or self.is_array_of_primitive

    @property
    def is_primitive(self) -> bool:
        return not self.is_struct and not self.is_array

    @property
    def is_nested(self) -> bool:
        return not self.is_primitive


class TableAlterColumnPosition(PydanticModel):
    is_first: bool
    is_last: bool
    after: t.Optional[str] = None

    @classmethod
    def first(self) -> TableAlterColumnPosition:
        return self(is_first=True, is_last=False, after=None)

    @classmethod
    def last(self, after: t.Optional[str] = None) -> TableAlterColumnPosition:
        return self(is_first=False, is_last=True, after=after)

    @classmethod
    def middle(self, after: str) -> TableAlterColumnPosition:
        return self(is_first=False, is_last=False, after=after)

    @classmethod
    def create(
        self,
        pos: int,
        current_kwargs: t.List[exp.StructKwarg],
        replacing_col: bool = False,
    ) -> TableAlterColumnPosition:
        is_first = pos == 0
        is_last = pos == len(current_kwargs) - int(replacing_col)
        after = None
        if not is_first:
            prior_kwarg = current_kwargs[pos - 1]
            after, _ = _get_name_and_type(prior_kwarg)
        return self(is_first=is_first, is_last=is_last, after=after)

    @property
    def column_position_node(self) -> t.Optional[exp.ColumnPosition]:
        column = exp.column(self.after) if self.after and not self.is_last else None
        position = None
        if self.is_first:
            position = "FIRST"
        elif column and not self.is_last:
            position = "AFTER"
        return exp.ColumnPosition(this=column, position=position)


class TableAlterOperation(PydanticModel):
    op: TableAlterOperationType
    columns: t.List[TableAlterColumn]
    column_type: exp.DataType
    expected_table_struct: exp.DataType
    add_position: t.Optional[TableAlterColumnPosition] = None
    current_type: t.Optional[exp.DataType] = None

    @classmethod
    def add(
        self,
        columns: t.Union[TableAlterColumn, t.List[TableAlterColumn]],
        column_type: t.Union[str, exp.DataType],
        expected_table_struct: t.Union[str, exp.DataType],
        position: t.Optional[TableAlterColumnPosition] = None,
    ) -> TableAlterOperation:
        return self(
            op=TableAlterOperationType.ADD,
            columns=ensure_list(columns),
            column_type=exp.DataType.build(column_type),
            add_position=position,
            expected_table_struct=exp.DataType.build(expected_table_struct),
        )

    @classmethod
    def drop(
        self,
        columns: t.Union[TableAlterColumn, t.List[TableAlterColumn]],
        expected_table_struct: t.Union[str, exp.DataType],
        column_type: t.Optional[t.Union[str, exp.DataType]] = None,
    ) -> TableAlterOperation:
        column_type = exp.DataType.build(column_type) if column_type else exp.DataType.build("INT")
        return self(
            op=TableAlterOperationType.DROP,
            columns=ensure_list(columns),
            column_type=column_type,
            expected_table_struct=exp.DataType.build(expected_table_struct),
        )

    @classmethod
    def alter_type(
        self,
        columns: t.Union[TableAlterColumn, t.List[TableAlterColumn]],
        column_type: t.Union[str, exp.DataType],
        current_type: t.Union[str, exp.DataType],
        expected_table_struct: t.Union[str, exp.DataType],
        position: t.Optional[TableAlterColumnPosition] = None,
    ) -> TableAlterOperation:
        return self(
            op=TableAlterOperationType.ALTER_TYPE,
            columns=ensure_list(columns),
            column_type=exp.DataType.build(column_type),
            add_position=position,
            current_type=exp.DataType.build(current_type),
            expected_table_struct=exp.DataType.build(expected_table_struct),
        )

    @property
    def is_add(self) -> bool:
        return self.op.is_add

    @property
    def is_drop(self) -> bool:
        return self.op.is_drop

    @property
    def is_alter_type(self) -> bool:
        return self.op.is_alter_type

    def full_column_path(self, array_suffix: str) -> str:
        results = []
        for column in self.columns:
            if column.is_array_of_struct and len(self.columns) > 1:
                results.append(column.name + array_suffix)
            else:
                results.append(column.name)
        return ".".join(results)

    def column(self, array_suffix: str) -> exp.Column:
        return exp.column(self.full_column_path(array_suffix))

    def column_def(self, array_suffix: str) -> exp.ColumnDef:
        return exp.ColumnDef(
            this=exp.to_identifier(self.full_column_path(array_suffix)),
            kind=self.column_type,
        )

    def expression(self, table_name: t.Union[str, exp.Table], array_suffix: str) -> exp.Expression:
        if self.is_alter_type:
            return exp.AlterTable(
                this=exp.to_table(table_name),
                actions=[
                    exp.AlterColumn(
                        this=self.column(array_suffix),
                        dtype=self.column_type,
                    )
                ],
            )
        elif self.is_add:
            alter_table = exp.AlterTable(this=exp.to_table(table_name))
            column = self.column_def(array_suffix)
            alter_table.set("actions", [column])
            if self.add_position:
                column.set("position", self.add_position.column_position_node)
            return alter_table
        elif self.is_drop:
            alter_table = exp.AlterTable(this=exp.to_table(table_name))
            drop_column = exp.Drop(this=self.column(array_suffix), kind="COLUMN")
            alter_table.set("actions", [drop_column])
            return alter_table
        else:
            raise ValueError(f"Unknown operation {self.op}")


class TableStructureResolver(PydanticModel):
    support_positional_add: bool = False
    support_struct_add_drop: bool = False
    array_suffix: str = ""
    compatible_types: t.Dict[exp.DataType, t.Set[exp.DataType]] = {}

    def _get_matching_kwarg(
        self,
        current_kwarg: t.Union[str, exp.StructKwarg],
        new_struct: exp.DataType,
        current_pos: int,
    ) -> t.Tuple[t.Optional[int], t.Optional[exp.StructKwarg]]:
        current_name = (
            current_kwarg
            if isinstance(current_kwarg, str)
            else _get_name_and_type(current_kwarg)[0]
        )
        # First check if we have the same column in the same position to get O(1) complexity
        new_kwarg = seq_get(new_struct.expressions, current_pos)
        if new_kwarg:
            new_name, new_type = _get_name_and_type(new_kwarg)
            if current_name == new_name:
                return current_pos, new_kwarg
        # If not, check if we have the same column in all positions with O(n) complexity
        for i, new_kwarg in enumerate(new_struct.expressions):
            new_name, new_type = _get_name_and_type(new_kwarg)
            if current_name == new_name:
                return i, new_kwarg
        return None, None

    def _drop_operation(
        self,
        columns: t.Union[TableAlterColumn, t.List[TableAlterColumn]],
        struct: exp.DataType,
        pos: int,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        columns = ensure_list(columns)
        operations = []
        root_column = columns[0]
        is_nested = len(columns) > 1
        is_supported_drop = (
            root_column.is_primitive
            or root_column.is_array_of_primitive
            or not is_nested
            or self.support_struct_add_drop
        )
        if not is_supported_drop:
            columns = [root_column]
            struct = root_struct
            column_pos, column_kwarg = self._get_matching_kwarg(root_column.name, root_struct, pos)
        else:
            column_pos, column_kwarg = self._get_matching_kwarg(columns[-1].name, struct, pos)
        assert column_pos
        assert column_kwarg
        struct.expressions.pop(column_pos)
        operations.append(
            TableAlterOperation.drop(columns, root_struct.copy(), column_kwarg.expression)
        )
        if not is_supported_drop:
            operations.extend(
                self._add_operation([root_column], column_pos, column_kwarg, struct, root_struct)
            )
        return operations

    def _resolve_drop_operation(
        self,
        parent_columns: t.List[TableAlterColumn],
        current_struct: exp.DataType,
        new_struct: exp.DataType,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        operations = []
        for current_pos, current_kwarg in enumerate(current_struct.expressions.copy()):
            new_pos, _ = self._get_matching_kwarg(current_kwarg, new_struct, current_pos)
            columns = parent_columns + [TableAlterColumn.from_struct_kwarg(current_kwarg)]
            if new_pos is None:
                operations.extend(
                    self._drop_operation(columns, current_struct, current_pos, root_struct)
                )
        return operations

    def _add_operation(
        self,
        columns: t.List[TableAlterColumn],
        new_pos: int,
        new_kwarg: exp.StructKwarg,
        current_struct: exp.DataType,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        if self.support_positional_add:
            col_pos = TableAlterColumnPosition.create(new_pos, current_struct.expressions)
            current_struct.expressions.insert(new_pos, new_kwarg)
        else:
            col_pos = None
            current_struct.expressions.append(new_kwarg)
        return [
            TableAlterOperation.add(
                columns,
                new_kwarg.expression,
                root_struct.copy(),
                col_pos,
            )
        ]

    def _resolve_add_operations(
        self,
        parent_columns: t.List[TableAlterColumn],
        current_struct: exp.DataType,
        new_struct: exp.DataType,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        operations = []
        for new_pos, new_kwarg in enumerate(new_struct.expressions):
            possible_current_pos, _ = self._get_matching_kwarg(new_kwarg, current_struct, new_pos)
            if possible_current_pos is None:
                columns = parent_columns + [TableAlterColumn.from_struct_kwarg(new_kwarg)]
                operations.extend(
                    self._add_operation(columns, new_pos, new_kwarg, current_struct, root_struct)
                )
        return operations

    def _alter_operation(
        self,
        columns: t.List[TableAlterColumn],
        pos: int,
        struct: exp.DataType,
        new_type: exp.DataType,
        current_type: t.Union[str, exp.DataType],
        root_struct: exp.DataType,
        new_kwarg: exp.StructKwarg,
    ) -> t.List[TableAlterOperation]:
        current_type = exp.DataType.build(current_type)
        if (
            new_type.this == current_type.this == exp.DataType.Type.STRUCT
            and self.support_struct_add_drop
        ):
            return self._get_operations(
                columns,
                current_type,
                new_type,
                root_struct,
            )
        if (
            new_type.this == current_type.this == exp.DataType.Type.ARRAY
            and self.support_struct_add_drop
        ):
            new_array_type = new_type.expressions[0]
            current_array_type = current_type.expressions[0]
            if new_array_type.this == current_array_type.this == exp.DataType.Type.STRUCT:
                return self._get_operations(
                    columns,
                    current_array_type,
                    new_array_type,
                    root_struct,
                )
        if self.is_compatible_type(current_type, new_type):
            struct.expressions.pop(pos)
            struct.expressions.insert(pos, new_kwarg)
            col_pos = (
                TableAlterColumnPosition.create(pos, struct.expressions, replacing_col=True)
                if self.support_positional_add
                else None
            )
            return [
                TableAlterOperation.alter_type(
                    columns,
                    new_type,
                    current_type,
                    root_struct.copy(),
                    col_pos,
                )
            ]
        else:
            return self._drop_operation(
                columns, root_struct, pos, root_struct
            ) + self._add_operation(columns, pos, new_kwarg, struct, root_struct)

    def _resolve_alter_operations(
        self,
        parent_columns: t.List[TableAlterColumn],
        current_struct: exp.DataType,
        new_struct: exp.DataType,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        operations = []
        for current_pos, current_kwarg in enumerate(current_struct.expressions.copy()):
            _, new_kwarg = self._get_matching_kwarg(current_kwarg, new_struct, current_pos)
            assert new_kwarg
            _, new_type = _get_name_and_type(new_kwarg)
            _, current_type = _get_name_and_type(current_kwarg)
            columns = parent_columns + [TableAlterColumn.from_struct_kwarg(current_kwarg)]
            if new_type == current_type:
                continue
            operations.extend(
                self._alter_operation(
                    columns,
                    current_pos,
                    current_struct,
                    new_type,
                    current_type,
                    root_struct,
                    new_kwarg,
                )
            )
        return operations

    def _get_operations(
        self,
        parent_columns: t.List[TableAlterColumn],
        current_struct: exp.DataType,
        new_struct: exp.DataType,
        root_struct: exp.DataType,
    ) -> t.List[TableAlterOperation]:
        root_struct = root_struct or current_struct
        parent_columns = parent_columns or []
        operations = []
        operations.extend(
            self._resolve_drop_operation(parent_columns, current_struct, new_struct, root_struct)
        )
        operations.extend(
            self._resolve_add_operations(parent_columns, current_struct, new_struct, root_struct)
        )
        operations.extend(
            self._resolve_alter_operations(parent_columns, current_struct, new_struct, root_struct)
        )
        return operations

    def _from_structs(
        self, current_struct: exp.DataType, new_struct: exp.DataType
    ) -> t.List[TableAlterOperation]:
        return self._get_operations([], current_struct, new_struct, current_struct)

    @classmethod
    def _dict_to_struct(cls, value: t.Dict[str, exp.DataType]) -> exp.DataType:
        return exp.DataType(
            this=exp.DataType.Type.STRUCT,
            expressions=[
                exp.StructKwarg(this=exp.to_identifier(k), expression=v) for k, v in value.items()
            ],
            nested=True,
        )

    def get_operations(
        self,
        current_table: t.Union[str, exp.Table],
        new_table: t.Union[str, exp.Table],
        engine_adapter: EngineAdapter,
    ) -> t.List[TableAlterOperation]:
        """
        Calculates a list of schema deltas between the two tables, applying which in order to the first table
        brings its schema in correspondence with the schema of the second table.

        Changes in positions of otherwise unchanged columns are currently ignored and are not reflected in the output.

        Additionally the implementation currently doesn't differentiate between regular columns and partition ones.
        It's a responsibility of a caller to determine whether a returned operation is allowed on partition columns or not.

        Args:
            current_table: The name of the table to which deltas will be applied.
            new_table: The schema of this table will be used for comparison.
            engine_adapter: The engine adapter to use to get current table schema.

        Returns:
            The list of deltas.
        """
        current_struct = self._dict_to_struct(engine_adapter.columns(current_table))
        new_struct = self._dict_to_struct(engine_adapter.columns(new_table))
        return self._from_structs(current_struct, new_struct)

    def is_compatible_type(self, current_type: exp.DataType, new_type: exp.DataType) -> bool:
        if current_type == new_type:
            return True
        if current_type in self.compatible_types:
            return new_type in self.compatible_types[current_type]
        return False


def _get_name_and_type(struct: exp.StructKwarg) -> t.Tuple[str, exp.DataType]:
    return struct.alias_or_name, struct.expression
