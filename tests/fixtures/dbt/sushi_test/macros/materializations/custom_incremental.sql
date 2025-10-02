{%- macro build_incremental_filter_sql(sql, time_column, existing_relation, interval_config) -%}
  {# macro to build the filter and also test use of macro inside materialisation #}
  WITH source_data AS (
    {{ sql }}
  )
  SELECT * FROM source_data
  WHERE {{ time_column }} >= (
    SELECT COALESCE(MAX({{ time_column }}), '1900-01-01')
    {%- if interval_config %} + INTERVAL {{ interval_config }} {%- endif %}
    FROM {{ existing_relation }}
  )
{%- endmacro -%}

{%- materialization custom_incremental, default -%}
  {%- set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set new_relation = api.Relation.create(database=database, schema=schema, identifier=identifier) -%}
  {%- set temp_relation = make_temp_relation(new_relation) -%}

  {%- set time_column = config.get('time_column') -%}
  {%- set interval_config = config.get('interval') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is none -%}
    {# The first insert creates new table if it doesn't exist #}
    {%- call statement('main') -%}
      CREATE TABLE {{ new_relation }}
      AS {{ sql }}
    {%- endcall -%}
  {%- else -%}
    {# Incremental load, appending new data with optional time filtering #}
    {%- if time_column is not none -%}
      {%- set filtered_sql -%}
        {{ build_incremental_filter_sql(sql, time_column, existing_relation, interval_config) }}
      {%- endset -%}
    {%- else -%}
      {%- set filtered_sql = sql -%}
    {%- endif -%}

    {{log(filtered_sql, info=true)}}

    {%- call statement('create_temp') -%}
      {{ create_table_as(True, temp_relation, filtered_sql) }}
      CREATE TABLE {{ temp_relation }}
      AS {{ filtered_sql }}
    {%- endcall -%}

    {%- call statement('insert') -%}
      INSERT INTO {{ new_relation }}
      SELECT * FROM {{ temp_relation }}
    {%- endcall -%}

    {%- call statement('drop_temp') -%}
      DROP TABLE {{ temp_relation }}
    {%- endcall -%}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [new_relation]}) }}
{%- endmaterialization -%}
