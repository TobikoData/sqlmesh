
MODEL (
  name sushi.waiter_as_customer_by_day,
  kind incremental_by_time_range (
    time_column (ds, '%Y-%m-%d')
  ),
  owner jen,
  cron '@daily',
  audits (
    not_null(columns = [waiter_id]),
    forall(criteria = [LENGTH(waiter_name) > 0])
  )
);

JINJA_QUERY_BEGIN;

{% set x = 1 %}

SELECT
  w.ds as ds,
  w.waiter_id as waiter_id,
  wn.name as waiter_name,
  {{ alias(identity(x), 'flag') }}
FROM sushi.waiters AS w
JOIN sushi.customers as c ON w.waiter_id = c.customer_id
JOIN sushi.waiter_names as wn ON w.waiter_id = wn.id;

JINJA_END;
