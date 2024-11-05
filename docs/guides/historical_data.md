# Historical Data

Data systems typically ingest new data on a regular cadence, such as daily. A core data engineering task is correctly ingesting and transforming that new data such that it conforms to the structure of data already in the system.

In some situations, processing new data requires ingesting or executing calculations against historical data. For example, we might calculate how much a new data point deviates from the average data values over the previous 30 days.

This guide describes methods for analyzing historical data with SQLMesh.

## Model kinds

The correct approach to analyzing a model's historical data is tied directly to its [model `kind`](../concepts/models/model_kinds.md).

Model kinds like `FULL` and `VIEW` process all data available from the source every time they are executed. If all historical data are present in the source system, these model kinds can use standard SQL queries to execute calculations against historical data and require no configuration adjustments.

Incremental model kinds, such as [`INCREMENTAL BY TIME RANGE`](../concepts/models/model_kinds.md#incremental_by_time_range), only ingest data from time intervals that have not been ingested in a previous run. Because of this, limited historical data are accessible, and incremental models may require adjustments to SQL queries or model configuration to process historical data.

## Incremental model time filtering

Incremental model queries filter data to only the relevant time interval in two ways: in the model's SQL query and with a data leakage guard SQLMesh adds automatically.

The model query's filter limits what data are *read* by the query, while the SQLMesh guard limits what data are *returned* by the query. The approaches described below are driven by the different roles played by the two filters.

Learn more about how incremental models filter time in the [incremental by time guide](../guides/incremental_time.md).

## Source system data retention

Each data point has three important temporal characteristics:

1. The date when the action generating the data point occurred (its "event date")
2. The date when the data point arrives in the source system from which our system ingests data (its "arrival date")
3. The date when our system ingests and processes the data point (its "processing date")

Event and arrival dates are usually coupled. In the simplest scenario, all data arrive shortly after the creating event occurred (e.g., a transaction occurs and is recorded in the source system in near real time). Alternatively, some or all data points may experience a delay between their event and arrival dates.

A source system must choose when and how it will store data based on the data's event and arrival dates, along with how long the system will retain the data it has stored.

Data points arriving after they are expected are *late-arriving* - for example, an event from yesterday that should have arrived yesterday but doesn't arrive until tomorrow. Systems have a number of options for handling late-arriving data, ranging from ignoring it (only accepting on-time data into the system) to always accepting it (data points of any event date may arrive at any time).

This section describes different source system designs, which determine (in part) the correct approach to transforming historical data. We break the description into three system behaviors: when the system accepts arriving data, how long the system keeps the data, and when our system loads from the source system.

### Keep it all forever

Conceptually, the simplest system design is to accept and store all data points forever no matter their event or arrival dates.

This is simple but requires continually increasing storage. Additionally, large tables can result in slow and/or expensive reads unless queries are narrowly tailored.

### Keep it all for a while




## Lookback

Some source data systems allow *late-arriving data*, where data from a specific time interval is recorded/processed by the system over multiple time intervals.

For example, a system recording credit card transactions might approve and store most transactions in real time, but flag some transactions for a manual review that takes up to 48 hours. Those flagged transactions are late-arriving because they are added to the system up to two days later than the date on which the transaction occurred.

We have two general options for ingesting late-arriving data in a source system that retains all historical data:

1. The "lag" approach: wait until the entire time window has elapsed to ingest any of the data. In the example above, this would mean waiting 48 hours until all flagged transactions are reviewed before ingesting a given day's transactions.
2. The "as available" approach: ingest a day's transactions on the regular system cadence and ingest late-arriving data on the day they arrive. For example, our system might regularly ingest the credit card transaction data on a daily cadence. In the "as available" approach, our system's run today would ingest all of yesterday's available transactions. Our system's run tomorrow would also ingest all of yesterday's available transactions, containing both the rows we already ingested and the late-arriving rows we have not yet ingested.


## Grouped calculations



## Windows



## Self-referencing queries



## Constant time lag
