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
