# Model Kinds

## INCREMENTAL_BY_TIME_RANGE

Incremental by time range load is the default model kind. It specifies that the data is incrementally computed. For example,
many models representing 'facts' or 'logs' should be incremental because new data is continuously added. This strategy
requires a time column.

## INCREMENTAL_BY_UNIQUE_KEY

Incremental by unique key will update or insert new records since the last load was run. This strategy requires a unique key.

## FULL
Full refresh is used when the entire table needs to be recomputed from scratch every batch.

## SNAPSHOT
Snapshot means recomputing the entire history of a table as of the compute date and storing that in a partition. Snapshots are expensive to compute and store but allow you to look at the frozen snapshot at a certain point in time. An example of a snapshot model would be computing and storing lifetime revenue of a user daily.

## VIEW
View models rely on datebase engine views and don't require any direct backfilling. Using a view will create a view in the same location as you may expect a physical table, but no table is computed. Other models that reference view models will incur compute cost because only the query is stored.

## EMBEDDED
Embedded models are like views except they don't interact with the data warehouse at all. They are embedded directly in models that reference them as expanded queries. They are an easy way to share logic across models.
