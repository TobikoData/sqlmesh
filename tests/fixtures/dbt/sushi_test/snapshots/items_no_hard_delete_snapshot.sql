{% snapshot items_no_hard_delete_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='ds',
      on_schema_change='sync_all_columns',
    )
}}

select * from {{ source('streaming', 'items') }}

{% endsnapshot %}
