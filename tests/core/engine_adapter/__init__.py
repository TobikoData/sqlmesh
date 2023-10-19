import typing as t

from sqlglot import exp

from sqlmesh import EngineAdapter


def to_sql_calls(adapter: EngineAdapter, identify: bool = True) -> t.List[str]:
    output = []
    for call in adapter.cursor.execute.call_args_list:
        # Python 3.7 support
        value = call[0][0] if isinstance(call[0], tuple) else call[0]
        sql = (
            value.sql(dialect=adapter.dialect, identify=identify)
            if isinstance(value, exp.Expression)
            else str(value)
        )
        output.append(sql)
    return output


def temp_table_name(table_name: str, random_id: str):
    temp_table = t.cast(exp.Table, exp.to_table(table_name))
    temp_table.set("this", exp.to_identifier(f"__temp_{temp_table.name}_{random_id}"))
    return temp_table
