MODEL (
  kind FULL,
  gateway memory
);

SELECT
  @overriden_var as item_id,
  @global_one as global_one,
  @one() AS macro_one
