from __future__ import annotations

from sqlglot import exp

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
      c -> row_number() OVER (PARTITION BY c ORDER BY 1) AS rank_@c
    )
  FROM @this_model
)
WHERE @REDUCE(
  @EACH(
    @columns,
    c -> rank_@c > 1
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
  False
  OR @IF(@min_v IS NOT NULL AND @inclusive, @column <= @min_v, False)
  OR @IF(@min_v IS NOT NULL AND NOT @inclusive, @column < @min_v, False)
  OR @IF(@max_v IS NOT NULL AND @inclusive, @column >= @max_v, False)
  OR @IF(@max_v IS NOT NULL AND NOT @inclusive, @column > @max_v, False)
    """,
)

# at_least_one(column=column_name)
at_least_one_audit = Audit(
    name="at_least_one",
    query="""
SELECT 1
FROM @this_model
GROUP BY 1
HAVING COUNT(@column) = 0
    """,
)

# not_constant(column=column_name)
not_constant_audit = Audit(
    name="not_constant",
    query="""
SELECT 1
FROM (
  SELECT COUNT(DISTINCT @column) AS t_cardinality
  FROM @this_model
) AS r
WHERE r.t_cardinality <= 1
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

# not_null_proportion(column=column_name, threshold=0.9)
not_null_proportion_audit = Audit(
    name="not_null_proportion",
    query="""
SELECT *
FROM (
  SELECT
    count(*) as cnt_tot,
    count(@column) as cnt_not_null,
    count(*) - count(@column) as cnt_null
  FROM @this_model
) AS s
WHERE s.cnt_not_null <= s.cnt_tot * @threshold
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

# sequential_values(column=column_name, interval=1)
# TODO: support grouping
sequential_values_audit = Audit(
    name="sequential_values",
    defaults={"interval": exp.Literal.number(1)},
    query="""
WITH windowed AS (
  SELECT
    @column,
    LAG(@column) OVER (
      ORDER BY @column
    ) AS prv
  FROM @this_model
), validation_errors AS (
    SELECT *
    FROM windowed
    WHERE NOT (@column = prv + @interval)
)

SELECT *
FROM validation_errors
    """,
)

# unique_combination_of_columns(columns=[column_1, column_2])
unique_combination_of_columns_audit = Audit(
    name="unique_combination_of_columns",
    query="""
SELECT @EACH(@columns, c -> c)
FROM @this_model
GROUP BY @EACH(@columns, c -> c)
HAVING COUNT(*) > 1
    """,
)

# mutually_exclusive_ranges(lower_bound_column=date_from, upper_bound_column=date_to)
# TODO: make inclusivity configurable
mutually_exclusive_ranges_audit = Audit(
    name="mutually_exclusive_ranges",
    defaults={"partition_clause": exp.false()},
    query="""
WITH window_functions AS (
  SELECT
    @lower_bound_column AS lower_bound,
    @upper_bound_column AS upper_bound,
    LEAD(@lower_bound_column) OVER (
      @if(@partition_clause, @partition_clause)
      ORDER BY @lower_bound_column, @upper_bound_column
    ) AS next_lower_bound,
    row_number() OVER (
      @if(@partition_clause, @partition_clause)
      ORDER BY @lower_bound_column desc, @upper_bound_column desc
    ) = 1 AS is_last_record
  FROM @this_model
), calc AS (
  SELECT
    *,
    COALESCE(
      lower_bound <= upper_bound,
      False
    ) AS lower_bound_lte_upper_bound,
    COALESCE(
      upper_bound <= next_lower_bound,
      is_last_record,
      False
    ) AS upper_bound_lte_next_lower_bound
  FROM window_functions
), validation_errors AS (
  SELECT *
  FROM calc
  WHERE NOT (
    lower_bound_lte_upper_bound
    AND upper_bound_lte_next_lower_bound
  )
)

SELECT *
FROM validation_errors
    """,
)

# The following audits are not yet implemented
# we are awaiting a first class way to express cross-model audits

# cardinality_equality(source_column=column_1, target_column=column_2, to=some.model)
# cardinality_equality_audit = Audit(
#     name="cardinality_equality",
#     query="""
# WITH table_a AS (
#   SELECT
#     @source_column,
#     count(*) AS num_rows
#   FROM @this_model
#   GROUP BY @source_column
# ),
# table_b AS (
#   SELECT
#     @target_column,
#     count(*) AS num_rows
#   FROM @to
#   GROUP BY @target_column
# ),
# except_a AS (
#   SELECT *
#   FROM table_a
#   EXCEPT
#   SELECT *
#   FROM table_b
# ),
# except_b AS (
#   SELECT *
#   FROM table_b
#   EXCEPT
#   SELECT *
#   FROM table_a
# ),
# unioned AS (
#   SELECT *
#   FROM except_a
#   UNION ALL
#   SELECT *
#   FROM except_b
# )
# SELECT * FROM unioned
#     """,
# )


# relationship(to=some.model, source_column=id, target_column=id)
# relationship_audit = Audit(
#     name="relationship",
#     defaults={"from_condition": exp.true(), "to_condition": exp.true()},
#     query="""
# SELECT
#   child.source_column
# FROM (
#   SELECT @source_column as source_column
#   FROM @this_model
#   WHERE NOT @source_column IS NULL
#   AND @from_condition
# ) AS child
# LEFT JOIN (
#   SELECT @target_column AS target_column
#   FROM @to
#   WHERE @to_condition
# ) AS parent
# ON child.source_column = parent.target_column
# WHERE parent.target_column IS NULL
#     """,
# )

# equality(columns=[column_1, column_2], to=some.model)
# equality_audit = Audit(
#     name="equality",
#     query="""
# SELECT @EACH(@columns, c -> c)
# FROM @this_model
# EXCEPT
# SELECT @EACH(@columns, c -> c)
# FROM @to
#     """,
# )

# equal_row_count(to=some.model)
# equal_row_count_audit = Audit(
#     name="equal_row_count",
#     query="""
# SELECT 1
# FROM (
#   SELECT count(*) as cnt
#   FROM @this_model
# ) AS src
# INNER JOIN (
#   SELECT count(*) as cnt
#   FROM @to
# ) AS tgt ON src.cnt != tgt.cnt
#     """,
# )

# fewer_rows_than(to=some.model)
# fewer_rows_than_audit = Audit(
#     name="fewer_rows_than",
#     query="""
# SELECT 1
# FROM (
#   SELECT count(*) as cnt
#   FROM @this_model
# ) AS src
# INNER JOIN (
#   SELECT count(*) as cnt
#   FROM @to
# ) AS tgt ON src.cnt >= tgt.cnt
#     """,
# )
