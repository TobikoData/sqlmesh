from __future__ import annotations

import sqlglot.expressions as exp

from sqlmesh.core.audit.definition import Audit

# not_null(columns=[column_1, column_2])
not_null_audit = Audit(
    name="not_null",
    query="""
SELECT *
FROM @this_model
WHERE @REDUCE(
  @EACH(
    @columns,
    c -> c IS NULL
  ),
  (l, r) -> l OR r
)
    """,
)

# unique_values(columns=[column_1, column_2])
unique_values_audit = Audit(
    name="unique_values",
    query="""
SELECT *
FROM (
  SELECT
    @EACH(
      @columns,
      c -> row_number() OVER (PARTITION BY c ORDER BY 1) AS @SQL('@{c}_rank')
    )
  FROM @this_model
)
WHERE @REDUCE(
  @EACH(
    @columns,
    c -> @SQL('@{c}_rank') > 1
  ),
  (l, r) -> l OR r
)
    """,
)

# accepted_values(column=column_name, is_in=[1, 2, 3])
accepted_values_audit = Audit(
    name="accepted_values",
    query="""
SELECT *
FROM @this_model
WHERE @column NOT IN @is_in
""",
)

# number_of_rows(threshold=100)
number_of_rows_audit = Audit(
    name="number_of_rows",
    query="""
SELECT 1
FROM @this_model
LIMIT @threshold + 1
HAVING COUNT(*) <= @threshold
    """,
)

# forall(criteria=[
#   column_1 > 0,
#   column_2 in (1, 2, 3),
#   column_3 is not null
# ]))
forall_audit = Audit(
    name="forall",
    query="""
SELECT *
FROM @this_model
WHERE @REDUCE(
  @EACH(
    @criteria,
    c -> NOT (c)
  ),
  (l, r) -> l OR r
)
    """,
)

# relationship(to=some.model, source_column=id, target_column=id)
relationship_audit = Audit(
    name="relationship",
    defaults={"from_condition": exp.null(), "to_condition": exp.null()},
    query="""
SELECT
  child.source_column
FROM (
  SELECT @source_column as source_column
  FROM @this_model
  WHERE NOT @source_column IS NULL
  AND @IF(@from_condition IS NOT NULL, @from_condition, 1=1)
) AS child
LEFT JOIN (
  SELECT @target_column AS target_column
  FROM @to
  @WHERE(@to_condition IS NOT NULL) @to_condition
) AS parent
ON child.source_column = parent.target_column
WHERE parent.target_column IS NULL
    """,
)

# accepted_range(column=age, min_v=0, max_v=100)
# accepted_range(column=age, min_v=10)
# accepted_range(column=age, max_v=50)
accepted_range_audit = Audit(
    name="accepted_range",
    defaults={"min_v": exp.null(), "max_v": exp.null(), "inclusive": exp.true()},
    query="""
SELECT *
FROM @this_model
WHERE
  1=2
  OR @IF(@min_v IS NOT NULL AND @inclusive, @column <= @min_v, 1=2)
  OR @IF(@min_v IS NOT NULL AND NOT @inclusive, @column < @min_v, 1=2)
  OR @IF(@max_v IS NOT NULL AND @inclusive, @column >= @max_v, 1=2)
  OR @IF(@max_v IS NOT NULL AND NOT @inclusive, @column > @max_v, 1=2)
    """,
)

# at_least_one(column=column_name)
at_least_one_audit = Audit(
    name="at_least_one",
    query="""
WITH src AS (
  SELECT count(*) as cnt_nulls
  FROM @this_model
  WHERE @column IS NULL
), tgt AS (
  SELECT count(*) as cnt_tot
  FROM @this_model
)
SELECT *
FROM src
INNER JOIN tgt ON src.cnt_nulls = tgt.cnt_tot
    """,
)

# equality(columns=[column_1, column_2], to=some.model)
equality_audit = Audit(
    name="equality",
    query="""
SELECT @EACH(@columns, c -> c)
FROM @this_model
EXCEPT
SELECT @EACH(@columns, c -> c)
FROM @to
    """,
)

# equal_row_count(to=some.model)
equal_row_count_audit = Audit(
    name="equal_row_count",
    query="""
SELECT 1
FROM (
  SELECT count(*) as cnt
  FROM @this_model
) AS src
INNER JOIN (
  SELECT count(*) as cnt
  FROM @to
) AS tgt ON src.cnt != tgt.cnt
    """,
)

# fewer_rows_than(to=some.model)
fewer_rows_than_audit = Audit(
    name="fewer_rows_than",
    query="""
SELECT 1
FROM (
  SELECT count(*) as cnt
  FROM @this_model
) AS src
INNER JOIN (
  SELECT count(*) as cnt
  FROM @to
) AS tgt ON src.cnt >= tgt.cnt
    """,
)

# not_constant(column=column_name)
not_constant_audit = Audit(
    name="not_constant",
    query="""
SELECT 1
FROM @this_model
GROUP BY 1
HAVING COUNT(DISTINCT @column) <= 1
    """,
)

# not_empty_string(column=column_name)
not_empty_string_audit = Audit(
    name="not_empty_string",
    query="""
SELECT *
FROM @this_model
WHERE @column = ''
    """,
)

# cardinality_equality(source_column=column_1, target_column=column_2, to=some.model)
cardinality_equality_audit = Audit(
    name="cardinality_equality",
    query="""
WITH table_a AS (
  SELECT
    @source_column,
    count(*) AS num_rows
  FROM @this_model
  GROUP BY @source_column
),
table_b AS (
  SELECT
    @target_column,
    count(*) AS num_rows
  FROM @to
  GROUP BY @target_column
),
except_a AS (
  SELECT *
  FROM table_a
  EXCEPT
  SELECT *
  FROM table_b
),
except_b as (
  SELECT *
  FROM table_b
  EXCEPT
  SELECT *
  FROM table_a
),
unioned as (
  SELECT *
  FROM except_a
  UNION ALL
  SELECT *
  FROM except_b
)
SELECT * FROM unioned
    """,
)

# not_null_proportion(column=column_name, threshold=0.9)
not_null_proportion_audit = Audit(
    name="not_null_proportion",
    query="""
WITH src AS (
  SELECT count(*) as cnt_nulls
  FROM @this_model
  WHERE @column IS NULL
), tgt AS (
  SELECT count(*) as cnt_tot
  FROM @this_model
)
SELECT *
FROM src
INNER JOIN tgt ON src.cnt_nulls >= tgt.cnt_tot * @threshold
    """,
)

# not_accepted_values(column=column_name, is_in=[1, 2, 3])
not_accepted_values_audit = Audit(
    name="not_accepted_values",
    query="""
SELECT *
FROM @this_model
WHERE @column IN @is_in
""",
)

# mutually_exclusive(column=column_name, with=some.model)
# TODO: make inclusivity configurable
mutually_exclusive_ranges_audit = Audit(
    name="mutually_exclusive_ranges",
    defaults={"partition_clause": exp.null()},
    query="""
with window_functions as (
    select
        @lower_bound_column as lower_bound,
        @upper_bound_column as upper_bound,
        lead(@lower_bound_column) over (
            @if(@partition_clause is not null, @partition_clause)
            order by @lower_bound_column, @upper_bound_column
        ) as next_lower_bound,
        row_number() over (
            @if(@partition_clause is not null, @partition_clause)
            order by @lower_bound_column desc, @upper_bound_column desc
        ) = 1 as is_last_record
    from @this_model
),
calc as (
    select
        *,
        coalesce(
            lower_bound <= upper_bound,
            false
        ) as lower_bound_comp_upper_bound,
        coalesce(
            upper_bound <= next_lower_bound,
            is_last_record,
            false
        ) as upper_bound_comp_next_lower_bound
    from window_functions
),
validation_errors as (
    select
        *
    from calc
    where not(
        lower_bound_comp_upper_bound
        and upper_bound_comp_next_lower_bound
    )
)
select * from validation_errors
    """,
)
