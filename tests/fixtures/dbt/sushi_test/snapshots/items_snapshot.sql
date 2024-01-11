{% snapshot items_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='ds',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('streaming', 'items') }}

{% endsnapshot %}
