{% snapshot items_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='ds',
      invalidate_hard_deletes=True,
      on_schema_change='sync_all_columns',
    )
}}

select * from {{ source('streaming', 'items') }}

{% endsnapshot %}

{% snapshot items_check_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['ds'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('streaming', 'items') }}

{% endsnapshot %}

{% snapshot items_check_with_cast_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['ds::DATE'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('streaming', 'items') }}

{% endsnapshot %}
