MODEL (
  name silver.e
);

SELECT
  * EXCEPT(dup)
FROM bronze.a
