CREATE SCHEMA sushi__dev;
CREATE SCHEMA sushi;
CREATE SCHEMA raw;
CREATE SCHEMA sqlmesh__sushi;
CREATE SCHEMA sqlmesh;



CREATE TABLE raw.demographics(customer_id INTEGER, zip VARCHAR);
CREATE TABLE sqlmesh__sushi.sushi__waiter_revenue_by_day__1845519333(waiter_id INTEGER, revenue DOUBLE, dummy_col VARCHAR, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__waiter_revenue_by_day__1845519333__temp(waiter_id INTEGER, revenue DOUBLE, dummy_col VARCHAR, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__waiter_as_customer_by_day__788412128(waiter_id INTEGER, waiter_name VARCHAR, flag INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__waiter_as_customer_by_day__788412128__temp(waiter_id INTEGER, waiter_name VARCHAR, flag INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__waiter_revenue_by_day__797243606(waiter_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__waiter_revenue_by_day__797243606__temp(waiter_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__customers__755912653(customer_id INTEGER, status VARCHAR, zip VARCHAR);
CREATE TABLE sqlmesh__sushi.sushi__customers__755912653__temp(customer_id INTEGER, status VARCHAR, zip VARCHAR);
CREATE TABLE sqlmesh__sushi.sushi__customer_revenue_lifetime__224166080(customer_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__customer_revenue_lifetime__224166080__temp(customer_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__customer_revenue_by_day__3794141510(customer_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__customer_revenue_by_day__3794141510__temp(customer_id INTEGER, revenue DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__order_items__1900977740(id INTEGER, order_id INTEGER, item_id INTEGER, quantity INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__order_items__1900977740__temp(id INTEGER, order_id INTEGER, item_id INTEGER, quantity INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__marketing__2156224001(customer_id INTEGER, status VARCHAR, updated_at TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP);
CREATE TABLE sqlmesh__sushi.sushi__marketing__2156224001__temp(customer_id INTEGER, status VARCHAR, updated_at TIMESTAMP, valid_from TIMESTAMP, valid_to TIMESTAMP);
CREATE TABLE sqlmesh__sushi.sushi__waiter_names__235351925(id BIGINT, "name" VARCHAR);
CREATE TABLE sqlmesh__sushi.sushi__waiter_names__235351925__temp(id BIGINT, "name" VARCHAR);
CREATE TABLE sqlmesh__sushi.sushi__raw_marketing__580496419(customer_id INTEGER, status VARCHAR, updated_at TIMESTAMP);
CREATE TABLE sqlmesh__sushi.sushi__raw_marketing__580496419__temp(customer_id INTEGER, status VARCHAR, updated_at TIMESTAMP);
CREATE TABLE sqlmesh__sushi.sushi__orders__1209065160(id INTEGER, customer_id INTEGER, waiter_id INTEGER, start_ts INTEGER, end_ts INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__orders__1209065160__temp(id INTEGER, customer_id INTEGER, waiter_id INTEGER, start_ts INTEGER, end_ts INTEGER, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__items__4184930416(id INTEGER, "name" VARCHAR, price DOUBLE, event_date DATE);
CREATE TABLE sqlmesh__sushi.sushi__items__4184930416__temp(id INTEGER, "name" VARCHAR, price DOUBLE, event_date DATE);
CREATE TABLE sqlmesh._plan_dags(request_id VARCHAR, dag_id VARCHAR, dag_spec VARCHAR);
CREATE TABLE sqlmesh._intervals(id VARCHAR, created_ts BIGINT, "name" VARCHAR, identifier VARCHAR, "version" VARCHAR, start_ts BIGINT, end_ts BIGINT, is_dev BOOLEAN, is_removed BOOLEAN, is_compacted BOOLEAN);
CREATE TABLE sqlmesh._seeds("name" VARCHAR, identifier VARCHAR, "content" VARCHAR);
CREATE TABLE sqlmesh._versions(schema_version INTEGER, sqlglot_version VARCHAR, sqlmesh_version VARCHAR);
CREATE TABLE sqlmesh._environments("name" VARCHAR, snapshots VARCHAR, start_at VARCHAR, end_at VARCHAR, plan_id VARCHAR, previous_plan_id VARCHAR, expiration_ts BIGINT, finalized_ts BIGINT, promoted_snapshot_ids VARCHAR, suffix_target VARCHAR);
CREATE TABLE sqlmesh._snapshots("name" VARCHAR, identifier VARCHAR, "version" VARCHAR, "snapshot" VARCHAR, kind_name VARCHAR);

CREATE VIEW sqlmesh__sushi.sushi__top_waiters__2524838182 (waiter_id, revenue) AS SELECT CAST(waiter_revenue_by_day.waiter_id AS INTEGER) AS waiter_id, CAST(waiter_revenue_by_day.revenue AS DOUBLE) AS revenue FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__797243606 AS waiter_revenue_by_day WHERE (waiter_revenue_by_day.event_date = (SELECT max(waiter_revenue_by_day.event_date) AS _col_0 FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__797243606 AS waiter_revenue_by_day)) ORDER BY revenue DESC LIMIT 10;

CREATE VIEW sqlmesh__sushi.sushi__top_waiters__2524838182__temp (waiter_id, revenue) AS SELECT CAST(waiter_revenue_by_day.waiter_id AS INTEGER) AS waiter_id, CAST(waiter_revenue_by_day.revenue AS DOUBLE) AS revenue FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__1845519333 AS waiter_revenue_by_day WHERE (waiter_revenue_by_day.event_date = (SELECT max(waiter_revenue_by_day.event_date) AS _col_0 FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__1845519333 AS waiter_revenue_by_day)) ORDER BY revenue DESC LIMIT 10;

CREATE VIEW sushi__dev.top_waiters (waiter_id, revenue) AS SELECT * FROM sqlmesh__sushi.sushi__top_waiters__2524838182;

CREATE VIEW sushi__dev.waiter_revenue_by_day (waiter_id, revenue, dummy_col, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__1845519333;

CREATE VIEW sushi.waiter_as_customer_by_day (waiter_id, waiter_name, flag, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__waiter_as_customer_by_day__788412128;

CREATE VIEW sushi.top_waiters (waiter_id, revenue) AS SELECT * FROM sqlmesh__sushi.sushi__top_waiters__2524838182;

CREATE VIEW sushi.waiter_revenue_by_day (waiter_id, revenue, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__waiter_revenue_by_day__797243606;

CREATE VIEW sushi.customers (customer_id, status, zip) AS SELECT * FROM sqlmesh__sushi.sushi__customers__755912653;

CREATE VIEW sushi.customer_revenue_lifetime (customer_id, revenue, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__customer_revenue_lifetime__224166080;

CREATE VIEW sushi.customer_revenue_by_day (customer_id, revenue, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__customer_revenue_by_day__3794141510;

CREATE VIEW sushi.order_items (id, order_id, item_id, quantity, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__order_items__1900977740;

CREATE VIEW sushi.marketing (customer_id, status, updated_at, valid_from, valid_to) AS SELECT * FROM sqlmesh__sushi.sushi__marketing__2156224001;

CREATE VIEW sushi.waiter_names (id, "name") AS SELECT * FROM sqlmesh__sushi.sushi__waiter_names__235351925;

CREATE VIEW sushi.raw_marketing (customer_id, status, updated_at) AS SELECT * FROM sqlmesh__sushi.sushi__raw_marketing__580496419;

CREATE VIEW sushi.orders (id, customer_id, waiter_id, start_ts, end_ts, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__orders__1209065160;

CREATE VIEW sushi.items (id, "name", price, event_date) AS SELECT * FROM sqlmesh__sushi.sushi__items__4184930416;
