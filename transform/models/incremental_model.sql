MODEL (
    name sqlmesh_example.incremental_model,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column event_date
    ),
    start '2020-01-01',
    cron '@daily',
    grain (id, event_date)
);

SELECT
    id,
    2 * item_id as item_id,
    event_date,
    1 as new_col
FROM
    sqlmesh_example.seed_model
WHERE
    event_date BETWEEN @start_date AND @end_date

