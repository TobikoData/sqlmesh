MODEL (
  name silver.d
);

SELECT
  *,
  @two() as two,
  @dup() AS dup
FROM silver.c
