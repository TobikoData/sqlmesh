# Apache Doris

## Overview

[Apache Doris](https://doris.apache.org/) is a modern analytical database product. Based on MPP architecture, it provides real-time analytical capabilities supporting both high-concurrency point queries and high-throughput complex analysis.

SQLMesh supports Doris through its MySQL-compatible protocol while providing Doris-specific optimizations and table model support.

## Connection Configuration

```yaml
connections:
  doris:
    type: doris
    host: fe.doris.cluster  # Frontend (FE) node address
    port: 9030              # Query port (default: 9030)
    user: doris_user
    password: your_password
    database: your_database
    
    # Optional MySQL-compatible settings
    charset: utf8mb4
    connect_timeout: 60
```

## Doris Table Models

Doris supports three table models, each optimized for different use cases. SQLMesh defaults to the **UNIQUE** model for optimal OLAP performance.

### UNIQUE Model (Default)

The UNIQUE model is ideal for dimension tables and scenarios requiring data updates. It ensures key uniqueness and supports efficient UPSERT operations.

**Features:**
- **Primary Key Updates**: New data overwrites existing records with matching keys
- **Merge-on-Write**: Enabled by default for better query performance
- **Automatic Deduplication**: Ensures data uniqueness based on specified key columns

**Example Configuration:**
```yaml
models:
  my_model:
    kind: TABLE
    table_properties:
      # UNIQUE model is default, but can be explicitly set
      TABLE_MODEL: UNIQUE
      UNIQUE_KEY: [user_id, email]  # Optional: specify unique key columns
      DISTRIBUTED_BY: HASH(user_id)
      BUCKETS: 16
      # merge-on-write is enabled by default
      enable_unique_key_merge_on_write: "true"
```

**Generated SQL:**
```sql
CREATE TABLE IF NOT EXISTS my_model (
    user_id BIGINT,
    email VARCHAR(100),
    username VARCHAR(50),
    created_at DATETIME
)
UNIQUE KEY(user_id, email)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"
);
```

### DUPLICATE Model

The DUPLICATE model allows duplicate data and is optimized for high-throughput scenarios like log data and streaming ingestion.

**Features:**
- **High Write Performance**: Optimized for append-only workloads
- **No Deduplication**: Allows duplicate records
- **Streaming Friendly**: Ideal for real-time data ingestion

**Example Configuration:**
```yaml
models:
  user_events:
    kind: TABLE
    table_properties:
      TABLE_MODEL: DUPLICATE
      DISTRIBUTED_BY: HASH(user_id)
      BUCKETS: 32
```

**Generated SQL:**
```sql
CREATE TABLE IF NOT EXISTS user_events (
    event_time DATETIME,
    user_id BIGINT,
    event_type VARCHAR(50),
    properties JSON
)
DISTRIBUTED BY HASH(user_id) BUCKETS 32;
```

### AGGREGATE Model

The AGGREGATE model supports pre-aggregation during data ingestion, perfect for metrics and analytical workloads.

**Features:**
- **Pre-aggregation**: Automatically aggregates data during ingestion
- **Metric Optimization**: Ideal for sum, count, min, max operations
- **Storage Efficiency**: Reduces storage requirements for aggregated data

**Example Configuration:**
```yaml
models:
  user_metrics:
    kind: TABLE
    table_properties:
      TABLE_MODEL: AGGREGATE
      AGGREGATE_KEY: [user_id, date]
      DISTRIBUTED_BY: HASH(user_id)
      BUCKETS: 16
```

## Advanced Configuration

### Distribution and Bucketing

Doris uses hash-based distribution for parallel processing:

```yaml
table_properties:
  DISTRIBUTED_BY: HASH(column_name)  # Distribution column
  BUCKETS: 32                        # Number of buckets (default: 10)
```

### Partitioning

Doris supports range and list partitioning for better query performance:

```yaml
table_properties:
  # Range partitioning by date
  PARTITION_BY: RANGE(date_column)
  # Will be translated to appropriate Doris PARTITION BY syntax
```

### Custom Properties

You can specify additional Doris-specific properties:

```yaml
table_properties:
  # Replication settings
  replication_allocation: "tag.location.default: 3"
  
  # Storage settings
  storage_type: COLUMN
  compression: LZ4
  
  # Performance tuning
  bloom_filter_columns: [user_id, email]
  enable_unique_key_merge_on_write: "true"
```

## Model Definition Examples

### Dimension Table
```sql
MODEL (
  name dim_users,
  kind TABLE,
  table_properties (
    TABLE_MODEL 'UNIQUE',
    UNIQUE_KEY ['user_id'],
    DISTRIBUTED_BY 'HASH(user_id)',
    BUCKETS 16,
    comment 'User dimension table'
  )
);

SELECT 
  user_id,
  username,
  email,
  created_at,
  updated_at
FROM source_users;
```

### Fact Table (High-Volume Logs)
```sql
MODEL (
  name fact_user_events,
  kind TABLE,
  table_properties (
    TABLE_MODEL 'DUPLICATE',
    DISTRIBUTED_BY 'HASH(user_id)',
    BUCKETS 64,
    comment 'User event fact table'
  )
);

SELECT 
  event_time,
  user_id,
  event_type,
  page_url,
  properties
FROM source_events;
```

### Aggregated Metrics Table
```sql
MODEL (
  name agg_daily_metrics,
  kind TABLE,
  table_properties (
    TABLE_MODEL 'AGGREGATE',
    AGGREGATE_KEY ['user_id', 'date'],
    DISTRIBUTED_BY 'HASH(user_id)',
    BUCKETS 32,
    comment 'Daily user metrics'
  )
);

SELECT 
  user_id,
  DATE(event_time) as date,
  COUNT(*) as event_count,
  SUM(revenue) as total_revenue,
  MAX(session_duration) as max_session_duration
FROM fact_user_events
GROUP BY user_id, DATE(event_time);
```

## Best Practices

### Model Selection
- **UNIQUE**: Use for dimension tables, user profiles, configuration data
- **DUPLICATE**: Use for event logs, clickstreams, time-series data
- **AGGREGATE**: Use for pre-computed metrics, dashboard data, reports

### Performance Optimization
1. **Choose appropriate distribution columns**: Use high-cardinality columns for even data distribution
2. **Set reasonable bucket counts**: Typically 2-4x the number of BE nodes
3. **Enable merge-on-write**: For UNIQUE tables requiring frequent queries
4. **Use partitioning**: For time-based data to improve query pruning

### Data Modeling
1. **Normalize dimensions**: Use UNIQUE tables for slowly changing dimensions
2. **Denormalize facts**: Use DUPLICATE tables for high-volume transactional data
3. **Pre-aggregate metrics**: Use AGGREGATE tables for commonly queried metrics

## Dependencies

To use Doris with SQLMesh, install the required MySQL driver:

```bash
pip install "sqlmesh[doris]"
# or
pip install pymysql
```

## Limitations

- **Transactions**: Doris doesn't support transactions (handled automatically by SQLMesh)
- **Complex Joins**: Large joins may require optimization for better performance
- **Schema Changes**: Some schema modifications may require table recreation

## Resources

- [Doris Documentation](https://doris.apache.org/docs/)
- [Doris Data Models Guide](https://doris.apache.org/docs/table-design/data-model/)
- [Doris SQL Reference](https://doris.apache.org/docs/sql-manual/) 