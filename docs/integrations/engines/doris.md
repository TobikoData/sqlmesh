# Apache Doris

## Overview

[Apache Doris](https://doris.apache.org/) is a modern analytical database product based on an MPP architecture. It provides real-time analytical capabilities, supporting both high-concurrency point queries and high-throughput complex analysis.

SQLMesh supports Doris through its MySQL-compatible protocol, while providing Doris-specific optimizations for table models, indexing, partitioning, and other features. The adapter is designed to leverage Doris's strengths for analytical workloads, with sensible defaults and support for advanced configuration.

## Connection Configuration

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
    # Use postgres as state connection
    type: postgres
    host: 127.0.0.1
    port: 5432
    user: your_user
    password: your_password
    database: your_database
```

## Doris Table Models

Doris supports three table models: DUPLICATE, UNIQUE, and AGGREGATE, each optimized for different use cases. SQLMesh supports **DUPLICATE** (default) and **UNIQUE** models. The AGGREGATE model is not supported yet.

To specify a table model, use the `TABLE_MODEL` property in your model's `table_properties`.

### DUPLICATE Model (Default)

The DUPLICATE model allows duplicate data and is optimized for high-throughput scenarios like log data and streaming ingestion.

**Features:**
- **High Write Performance**: Optimized for append-only workloads.
- **No Deduplication**: Allows duplicate records.
- **Streaming Friendly**: Ideal for real-time data ingestion.

**Example Configuration:**
```sql
MODEL (
  name user_events,
  kind FULL,
  physical_properties (
    TABLE_MODEL 'DUPLICATE',
    DISTRIBUTED_BY 'HASH(user_id)',
    BUCKETS 10
  )
);
```

### UNIQUE Model

The UNIQUE model is ideal for dimension tables and scenarios requiring data updates. It ensures key uniqueness and supports efficient UPSERT operations.

**Features:**
- **Primary Key Updates**: New data overwrites existing records with matching keys.
- **Merge-on-Write**: Can be enabled for better query performance.
- **Automatic Deduplication**: Ensures data uniqueness based on specified key columns.

**Example Configuration:**
```sql
MODEL (
  name dim_users,
  kind FULL,
  physical_properties (
    TABLE_MODEL 'UNIQUE',
    KEY_COLS ['user_id'],
    DISTRIBUTED_BY 'HASH(user_id)',
    BUCKETS 16,
    -- Enable merge-on-write for better query performance
    enable_unique_key_merge_on_write = 'true'
  )
);
```

## Advanced Table Properties

The Doris adapter supports a range of advanced table properties for fine-grained control over table creation. These can be set in the `physical_properties` section of your model.

| Property              | Description                                                                                 | Default                |
|-----------------------|---------------------------------------------------------------------------------------------|------------------------|
| `TABLE_MODEL`         | Table model: `DUPLICATE` or `UNIQUE`                                                        | `DUPLICATE`            |
| `KEY_COLS`            | List of key columns for UNIQUE or DUPLICATE models                                           | (all columns for DUPLICATE, required for UNIQUE) |
| `DISTRIBUTED_BY`      | Distribution method, e.g., `HASH(user_id)`                                                  | `HASH(<first_column>)` |
| `BUCKETS`             | Number of buckets for distribution                                                          | `10`                   |
| `AUTO_PARTITION`      | Enable Doris auto partitioning (`true`/`false`) for Doris 2.1                                             | `false`                |
| `PARTITION_FUNCTION`  | Custom partition function for advanced partitioning, e.g., `date_trunc(create_time, 'month')`                                         |                        |
| `PARTITIONED_BY_DEF`  | Partition definition string, e.g., `FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 2 YEAR`                                         |                        |
| Any other property    | Passed through as a Doris table property                                                     |                        |

**Example with advanced properties:**
```sql
MODEL (
  name advanced_table,
  kind FULL,
  partitioned_by create_time,
  physical_properties (
    TABLE_MODEL = 'UNIQUE',
    KEY_COLS = ['id'],
    DISTRIBUTED_BY = 'HASH(id)',
    BUCKETS = 8,
    PARTITIONED_BY_DEF 'FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 2 YEAR',
    replication_allocation = 'tag.location.default: 3',
    in_memory = 'false',
    storage_format = 'V2',
    disable_auto_compaction = 'false'
  )
);
```

**Notes:**
- For the UNIQUE model, partition column must be included in `KEY_COLS`.
- If `DISTRIBUTED_BY` is not specified, the first column is used by default.

## Partitioning

Doris supports range and list partitioning to improve query performance. SQLMesh will automatically translate time-based partitioning into Doris's `PARTITION BY RANGE` syntax. You can also use advanced partitioning options via table properties.

**Example (automatic time-based partitioning):**
```sql
MODEL (
  name my_partitioned_model,
  kind INCREMENTAL_BY_TIME_RANGE((event_date, '%Y-%m-%d')),
  -- other model properties
);
```

**Advanced partitioning:**
- Use `AUTO_PARTITION` to enable Doris's auto partitioning (supported since Doris 2.1).
- Use `PARTITION_FUNCTION` for custom partition expressions.
- Use `PARTITIONED_BY_DEF` to specify raw partition definitions.

**Note:** For UNIQUE KEY tables, partition column must be included in `KEY_COLS`. If not, partitioning will be skipped and a warning will be logged.

## Indexing

SQLMesh supports creating indexes in Doris to accelerate queries. You can define indexes in your model's DDL or use the adapter's API.

**Supported Index Types:** `INVERTED`, `BLOOMFILTER`, `NGRAM_BF`

**Example:**
```sql
MODEL (
  name my_indexed_table,
  kind FULL
);

SELECT 
   ...
FROM 
   ...

@IF(
  @runtime_stage = 'creating',
  CREATE INDEX idx_city ON my_indexed_table (city) USING BLOOMFILTER COMMENT 'Bloomfilter index on city'
);

@IF(
  @runtime_stage = 'creating',
  CREATE INDEX idx_username ON my_indexed_table (username) USING INVERTED COMMENT 'Inverted index on username'
);
```

- Both `CREATE INDEX` and `ALTER TABLE ... ADD INDEX` are supported for compatibility with different Doris versions.
- Index properties and comments are supported.

## Comments

SQLMesh supports adding comments to tables and columns.

- **Table Comments**: Use the `description` property in the `MODEL` definition.
- **Column Comments**: Use the `column_descriptions` property in the `MODEL` definition.

```sql
MODEL (
  name my_commented_table,
  kind TABLE,
  description 'This is a table comment.'
  column_descriptions (
    id = "This is a column comment"
);
```

**Note:** Doris has comment length limits: `2048` characters for tables and `255` for columns. SQLMesh will automatically truncate longer comments to fit these limits.

## Views

- Only regular views are supported. Doris materialized views are not supported by SQLMesh, due to Doris limitations (e.g., no table alias or schema name in materialized view definitions).
- `CREATE OR REPLACE VIEW` is not supported. If `replace=True`, SQLMesh will drop the view first and then create it.
- The `CASCADE` clause is not supported for dropping views.

## Dependencies

To use Doris with SQLMesh, install the required MySQL driver:

```bash
pip install "sqlmesh[doris]"
# or
pip install pymysql
```

## Limitations

- **Transactions**: Doris does not support transactions. SQLMesh manages data consistency by creating new table versions and swapping them atomically.
- **Unsupported DDL**:
  - `REPLACE TABLE` is not supported.
  - `DROP SCHEMA ... CASCADE` is not supported and will be executed without the `CASCADE` clause.
- **Materialized Views**: SQLMesh does not currently support creating materialized views in Doris.
- **Schema Changes**: Some schema modifications may require table recreation, which SQLMesh handles automatically.
- **Identifier Length**: Doris limits identifiers (e.g., table/column names) to 64 characters.

## Resources

- [Doris Documentation](https://doris.apache.org/docs/)
- [Doris Data Models Guide](https://doris.apache.org/docs/table-design/data-model/)
- [Doris SQL Reference](https://doris.apache.org/docs/sql-manual/)