MODEL (
  name sushi.test_parent,
  kind full,
);

SELECT
    id::INT AS id,
    item_id::INT AS item_id,
    ds::TEXT AS ds,
FROM
    (VALUES
        (1, 1, '2020-01-01'),
        (1, 2, '2020-01-01'),
        (2, 1, '2020-01-01'),
        (3, 3, '2020-01-03'),
        (4, 1, '2020-01-04'),
        (5, 1, '2020-01-05'),
        (6, 1, '2020-01-06'),
        (7, 1, '2020-01-07')
    ) AS t (id, item_id, ds)
