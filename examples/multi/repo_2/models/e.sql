MODEL (
  name silver.e,
  kind FULL
);

SELECT
  * EXCEPT(dup)
FROM bronze.a
