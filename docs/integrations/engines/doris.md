# Apache Doris

## Overview

[Apache Doris](https://doris.apache.org/) is a modern analytical database product based on an MPP architecture. It provides real-time analytical capabilities, supporting both high-concurrency point queries and high-throughput complex analysis.

SQLMesh supports Doris through its MySQL-compatible protocol, while providing Doris-specific optimizations for table models, indexing, partitioning, and other features. The adapter is designed to leverage Doris's strengths for analytical workloads, with sensible defaults and support for advanced configuration.

## Connection Configuration Example

```yaml
doris:
  connection:
    type: doris
    host: fe.doris.cluster  # Frontend (FE) node address
    port: 9030              # Query port (default: 9030)
    user: doris_user
    password: your_password
    database: your_database
    # Optional MySQL-compatible settings
    charset: utf8mb4
    connect_timeout: 60
  state_connection:
    # Use duckdb as state connection
    type: duckdb
```

## Table Models

Doris supports three table models: DUPLICATE, UNIQUE, and AGGREGATE. SQLMesh supports **DUPLICATE** and **UNIQUE** models through the `physical_properties` configuration.

### DUPLICATE Model (Default)

**Example Configuration:**
```sql
MODEL (
  name user_events,
  kind FULL,
  physical_properties (
    duplicate_key ('user_id', 'event_time'),
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 10
    )
  )
);
```

### UNIQUE Model

**Example Configuration:**
```sql
MODEL (
  name user_events,
  kind FULL,
  physical_properties (
    unique_key 'user_id',
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 16
    )
  )
);
```

## Table Properties

The Doris adapter supports a comprehensive set of table properties that can be configured in the `physical_properties` section of your model.

### Core Table Properties

| Property              | Type                  | Description                                 | Example                                                    |
| --------------------- | --------------------- | ------------------------------------------- | ---------------------------------------------------------- |
| `unique_key`          | `Tuple[str]` or `str` | Defines unique key columns for UNIQUE model | `('user_id')` or `'user_id'`                               |
| `duplicate_key`       | `Tuple[str]` or `str` | Defines key columns for DUPLICATE model     | `（'user_id', 'event_time'）`                              |
| `distributed_by`      | `Dict`                | Distribution configuration                  | See Distribution section                                   |
| `partitions`          | `Tuple[str]` or `str` | Custom partition expression                 | `'FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 1 MONTH'` |

### Distribution Configuration

The `distributed_by` property supports multiple formats:

**Dictionary Format:**
```sql
MODEL (
  name my_table,
  kind FULL,
  physical_properties (
    distributed_by (
      kind = 'HASH',
      expressions = 'user_id',
      buckets = 10
    )
  )
);
```

```sql
MODEL (
  name my_table,
  kind FULL,
  physical_properties (
    distributed_by (
      kind = 'RANDOM'
    )
  )
);
```

**Supported Distribution Types:**
- `HASH`: Hash-based distribution (most common)
- `RANDOM`: Random distribution

**Bucket Configuration:**
- Integer value: Fixed number of buckets (e.g., `10`)
- `'AUTO'`: Automatic bucket calculation

### Partitioning

Doris table supports range partitioning and list partitioning to improve query performance.

**Custom Partition Expression:**
```sql
MODEL (
  name my_partitioned_model,
  kind INCREMENTAL_BY_TIME_RANGE(time_column (event_date, '%Y-%m-%d')),
  partitioned_by RANGE(event_date),
  physical_properties (
    partitions = 'FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 2 YEAR',
  ),
);
```

```sql
MODEL (
  name my_custom_partitioned_model,
  kind FULL,
  partitioned_by RANGE(event_date),
  physical_properties (
    partitioned_by_expr = (
      'PARTITION `p2023` VALUES [("2023-01-01"), ("2024-01-01"))', 
      'PARTITION `p2024` VALUES [("2024-01-01"), ("2025-01-01"))', 
      'PARTITION `p2025` VALUES [("2025-01-01"), ("2026-01-01"))', 
      'PARTITION `other` VALUES LESS THAN MAXVALUE'
    ),
  )
);
```

### Generic Properties

Any additional properties in `physical_properties` are passed through as Doris table properties:

```sql
MODEL (
  name advanced_table,
  kind FULL,
  physical_properties (
    unique_key = 'id',
    distributed_by (
      kind = 'HASH',
      expressions = 'id',
      buckets = 8
    ),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false',
  )
);
```

## Materialized Views

SQLMesh supports creating materialized views in Doris with comprehensive configuration options.

### Basic Materialized View

```sql
MODEL (
  name user_summary_mv,
  kind VIEW (
    materialized true
  )
);

SELECT 
  user_id,
  COUNT(*) as event_count,
  MAX(event_time) as last_event
FROM user_events
GROUP BY user_id;
```

### Advanced Materialized View Configuration

```sql
MODEL (
  name sqlmesh_test.view_materialized1,
  kind VIEW (
    materialized true
  ),
  partitioned_by ds,
  physical_properties (
    build = 'IMMEDIATE',
    refresh = 'AUTO',
    refresh_trigger = 'ON SCHEDULE EVERY 12 hour',
    unique_key = id,
    distributed_by = (kind='HASH', expressions=id, buckets=10),
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  ),
  description "customer zip",
  columns (
    id int,
    ds datetime,
    zip int,
  ),
  column_descriptions (
    id = "order id",
    zip = "zip code",
  )
);
```

### Materialized View Properties

| Property              | Description                                                                     | Values                                                     |
| --------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| `build`               | Build strategy                                                                  | `'IMMEDIATE'`, `'DEFERRED'`                                |
| `refresh`             | Refresh strategy                                                                | `'COMPLETE'`, `'AUTO'`                                     |
| `refresh_trigger`     | Schedule for automatic refresh                                                  | `'MANUAL'`, `'ON SCHEDULE INTERVAL 1 HOUR'`, `'ON COMMIT'` |
| `unique_key`          | Unique key columns                                                              | `'user_id'` or `['user_id', 'date']`                       |
| `duplicate_key`       | Duplicate key columns                                                           | `'user_id'` or `['user_id', 'date']`                       |
| `materialized_type`   | Materialized type                                                               | `SYNC`, `ASYNC`                                            |
| `source_table`        | Source table of synchronous materialized view                                   | `schema_name`.`table_name`                                 |

## Indexing

SQLMesh supports creating indexes in Doris to accelerate queries. You can define indexes in your model's DDL.

**Example:**
```sql
MODEL (
  name my_indexed_table,
  kind FULL
);

SELECT 
   user_id,
   username,
   city
FROM 
   users;

@IF(
  @runtime_stage = 'creating',
  CREATE INDEX idx_username ON my_indexed_table (username) USING INVERTED COMMENT 'Inverted index on username'
);
```

## Comments

SQLMesh supports adding comments to tables and columns with automatic truncation to Doris limits.

- **Table Comments**: Use the `description` property in the `MODEL` definition
- **Column Comments**: Use the `column_descriptions` property in the `MODEL` definition

```sql
MODEL (
  name my_commented_table,
  kind TABLE,
  description 'This is a comprehensive table comment that describes the purpose and usage of this table in detail.',
  column_descriptions (
    id = "Unique identifier for each record",
    user_id = "Foreign key reference to users table",
    event_type = "Type of event that occurred"
  )
);
```

**Limits:**
- Table comments: 2048 characters (automatically truncated)
- Column comments: 255 characters (automatically truncated)

## Views

SQLMesh supports both regular and materialized views in Doris.

### Regular Views

```sql
MODEL (
  name user_summary_view,
  kind VIEW
);

SELECT 
  user_id,
  COUNT(*) as event_count,
  MAX(event_time) as last_event
FROM user_events
GROUP BY user_id;
```

## Dependencies

To use Doris with SQLMesh, install the required MySQL driver:

```bash
pip install "sqlmesh[doris]"
# or
pip install pymysql
```

## Resources

- [Doris Documentation](https://doris.apache.org/docs/)
- [Doris Data Models Guide](https://doris.apache.org/docs/table-design/data-model/)
- [Doris SQL Reference](https://doris.apache.org/docs/sql-manual/)