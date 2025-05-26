from sqlglot import exp
import sqlmesh.core.dialect as d
from sqlmesh.dbt.converter.common import SQLMESH_PREDEFINED_MACRO_VARIABLES, SQLGlotTransform


def unwrap_macros_in_string_literals() -> SQLGlotTransform:
    """
    Given a query containing string literals *that match SQLMesh predefined macro variables* like:

    > select * from foo where ds between '@start_dt' and '@end_dt'

    Unwrap them into:

    > select * from foo where ds between @start_dt and @end_dt
    """
    values_to_check = {f"@{var}": var for var in SQLMESH_PREDEFINED_MACRO_VARIABLES}

    def _transform(e: exp.Expression) -> exp.Expression:
        if isinstance(e, exp.Literal) and e.is_string:
            if (value := e.text("this")) and value in values_to_check:
                return d.MacroVar(
                    this=values_to_check[value]
                )  # MacroVar adds in the @ so dont want to add it twice
        return e

    return _transform
