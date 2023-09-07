# Incremental by time guide

SQLMesh models are classified by [kind](../concepts/models/model_kinds.md). One powerful model kind is "incremental by time range" - this guide describes how these models work and demonstrates how to use them.

## Load the right data

The incremental by time approach to data loading is motivated by efficiency. It is based on the principle of only loading a given data row one time.

Model kinds such as `VIEW` or `FULL` reload the entirety of the source system's data every time they run. In some cases, reloading all the data is not feasible. In other cases, it is an inefficient use of time and computational resources - both of which equate to money your business could spend on something else.

## Counting time

Incremental by time models work by first identifying the date range for which data should be selected from the source table.

One approach to determining the date range bases it on the most recent record timestamp observed in the data. That approach is simple to implement, but it makes three assumptions: the table already exists, there are no temporal gaps in the data, and that the load is able to run in a single query.

SQLMesh takes a different approach by using time *intervals*.

### Calculating intervals

The first step to using time intervals is to create the set of all possible time intervals based on the model's *start date* and *interval unit*. The start date specifies when time "begins" for the model, and interval unit specifies how finely time should be divided.

For example, consider a model with a start datetime of 12am yesterday and an interval unit of 1 hour. We are working with the model today at 12pm. The model's set of time intervals has 36 entries: 24 for each hour of yesterday and 12 for each hour today from 12am to 12pm.

When we first execute and backfill the model as part of a `sqlmesh plan` today at 12pm, SQLMesh calculates its set of 36 time intervals and records that all 36 of them were backfilled. It retains this information in the SQLMesh state tables for future use.

If we `sqlmesh run` the model tomorrow at 12pm, SQLMesh calculates the set of all intervals as (24 for day 1) + (24 for day 2) + (12 for today 12am to 12pm) = 60 intervals. It compares this set of 60 to the stored set of 36 that we already backfilled to identify the 24 un-processed intervals from yesterday at 12pm to today at 12pm. It then processes only those 24 intervals during today's run.

In that example, we assumed that `sqlmesh run` used the default end datetime of now (when the command is issued). In some cases, people may instead specify a different end datetime, which can lead to gaps due to unprocessed intervals. The intervals approach automatically detects any gaps and includes them in the next `run` whose start/end datetimes include them.

### When `run` runs
