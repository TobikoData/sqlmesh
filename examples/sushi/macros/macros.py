from macros.utils import between  # type: ignore
from sqlglot import exp

from sqlmesh import macro


@macro()
def incremental_by_ds(evaluator, column: exp.Column):
    return between(evaluator, column, evaluator.locals["start_date"], evaluator.locals["end_date"])


@macro()
def assert_has_columns(evaluator, model, columns_to_types):
    if evaluator.runtime_stage == "creating":
        expected_schema = {
            column_type.name: exp.maybe_parse(
                column_type.text("expression"), into=exp.DataType, dialect=evaluator.dialect
            )
            for column_type in columns_to_types.expressions
        }
        assert expected_schema.items() <= evaluator.columns_to_types(model).items()

    return None


@macro()
def waiter_names_threshold(evaluator):
    return 200
