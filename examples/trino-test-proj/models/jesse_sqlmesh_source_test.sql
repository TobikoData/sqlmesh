MODEL (
  name css_data.scratch.jesse_source_test,
  owner 'jesse.hodges',
  description 'Simple test model to verify potential source addition to sqlmesh',
  kind VIEW,
  cron '@daily'
);

-- Simple test query that returns static data
SELECT
  CURRENT_TIMESTAMP AS test_timestamp,
  'jesse_source_test' AS model_name,
  42 AS test_number,
  666 AS another_test_number_2,
  TRUE AS is_active;
