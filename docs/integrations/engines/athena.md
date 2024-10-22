# Athena

## Installation

```
pip install "sqlmesh[athena]"
```

## Connection options

### PyAthena connection options

SQLMesh leverages the [PyAthena](https://github.com/laughingman7743/PyAthena) DBAPI driver to connect to Athena. Therefore, the connection options relate to the PyAthena connection options.
Note that PyAthena uses [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) under the hood so you can also use [boto3 environment variables](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables) for configuration.

| Option                  | Description                                                                                                                                                  |  Type  | Required |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                  | Engine type name - must be `athena`                                                                                                                          | string |    Y     |
| `aws_access_key_id`     | The access key for your AWS user                                                                                                                             | string |    N     |
| `aws_secret_access_key` | The secret key for your AWS user                                                                                                                             | string |    N     |
| `role_arn`              | The ARN of a role to assume once authenticated                                                                                                               | string |    N     |
| `role_session_name`     | The session name to use when assuming `role_arn`                                                                                                             | string |    N     |
| `region_name`           | The AWS region to use                                                                                                                                        | string |    N     |
| `work_group`            | The Athena [workgroup](https://docs.aws.amazon.com/athena/latest/ug/workgroups-manage-queries-control-costs.html) to send queries to                         | string |    N     |
| `s3_staging_dir`        | The S3 location for Athena to write query results. Only required if not using `work_group` OR the configured `work_group` doesnt have a results location set | string |    N     |
| `schema_name`           | The default schema to place objects in if a schema isnt specified. Defaults to `default`                                                                     | string |    N     |
| `catalog_name`          | The default catalog to place schemas in. Defaults to `AwsDataCatalog`                                                                                        | string |    N     |

### SQLMesh connection options

These options are specific to SQLMesh itself and are not passed to PyAthena

| Option                  | Description                                                                                                                                                                                      | Type   | Required |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|
| `s3_warehouse_location` | Set the base path in S3 where SQLMesh will instruct Athena to place table data. Only required if you arent specifying the location in the model itself. See [S3 Locations](#s3-locations) below. | string | N        |

## Model properties

The Athena adapter utilises the following model top-level [properties](../../concepts/models/overview.md#model-properties):

| Name             | Description                                                                                                                                                                                                                                                                                                                          | Type   | Required |
|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|
| `table_format`   | Sets the [table_type](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties) Athena uses when creating the table. Valid values are `hive` or `iceberg`.                                                                                                                                            | string | N        |
| `storage_format` | Configures the file format to be used by the `table_format`. For Hive tables, this sets the [STORED AS](https://docs.aws.amazon.com/athena/latest/ug/create-table.html#parameters) option. For Iceberg tables, this sets [format](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties) property. | string | N        |

The Athena adapter recognises the following model [physical_properties](../../concepts/models/overview.md#physical_properties):

| Name              | Description                                                                                                                                                                               | Type   | Default |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|---------|
| `s3_base_location`| `s3://` base URI of where the snapshot tables for this model should be written. Overrides `s3_warehouse_location` if one is configured.                                                   | string |         |


## S3 Locations
When creating tables, Athena needs to know where in S3 the table data is located. You cannot issue a `CREATE TABLE` statement without specifying a `LOCATION` for the table data.

In addition, unlike other engines such as Trino, Athena will not infer a table location if you set a _schema_ location via `CREATE SCHEMA <schema> LOCATION 's3://schema/location'`.

Therefore, in order for SQLMesh to issue correct `CREATE TABLE` statements to Athena, you need to configure where the tables should be stored. There are two options for this:

- **Project-wide:** set `s3_warehouse_location` in the connection config. SQLMesh will set the table `LOCATION` to be `<s3_warehouse_location>/<schema_name>/<snapshot_table_name>` when it creates a snapshot of your model.
- **Per-model:** set `s3_base_location` in the model `physical_properties`. SQLMesh will set the table `LOCATION` to be `<s3_base_location>/<snapshot_table_name>` every time it creates a snapshot of your model. This takes precedence over any `s3_warehouse_location` set in the connection config.


## Limitations
Athena was initially designed to read data stored in S3 and to do so without changing that data. This means that it does not have good support for mutating tables. In particular, it will not delete data from Hive tables.

Consequently, [forward only changes](../../concepts/plans.md#forward-only-change) that mutate the schemas of existing tables have a high chance of failure because Athena supports very limited schema modifications on Hive tables.

However, Athena does support [Apache Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html) tables which allow a full range of operations. These can be used for more complex model types such as [`INCREMENTAL_BY_UNIQUE_KEY`](../../concepts/models/model_kinds.md#incremental_by_unique_key) and [`SCD_TYPE_2`](../../concepts/models/model_kinds.md#scd-type-2).

To use an Iceberg table for a model, set `table_format iceberg` in the model [properties](../../concepts/models/overview.md#model-properties).

In general, Iceberg tables offer the most flexibility and you'll run into the least SQLMesh limitations when using them. However, we create Hive tables by default because Athena creates Hive tables by default, so Iceberg tables are opt-in rather than opt-out.
