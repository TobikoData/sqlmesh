MODEL (
  name silver.d,
  kind FULL
);

SELECT
  *,
  @two() as two,
  @dup() AS dup
FROM silver.c
