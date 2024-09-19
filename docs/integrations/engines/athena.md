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

| Option                  | Description                                                                                                                                                                                                           | Type   | Required |
|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|----------|
| `s3_warehouse_location` | Set the base path in S3 where SQLMesh will place table data. Only required if the schemas dont have default locations set or you arent specifying the location in the model. See [S3 Locations](#s3-locations) below. | string | N        |

## Model properties

The Athena adapter recognises the following model [physical_properties](../../concepts/models/overview.md#physical_properties):

| Name              | Description                                                                                                                                                                               | Type   | Default |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|---------|
| `s3_base_location`| `s3://` base URI of where the snapshot tables for this model should be located. Overrides `s3_warehouse_location` if one is configured.                                                                            | string |         |
| `table_type`      | Sets the [table_type](https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html#ctas-table-properties) Athena uses when creating the table. Valid values are `hive` or `iceberg`. | string | `hive`  |


## S3 Locations
When creating tables, Athena needs to know where in S3 the table data is located. You cannot issue a `CREATE TABLE` statement without specifying a `LOCATION` for the table data.

If the schema you're creating the table under had a `LOCATION` set when it was created, Athena places the table in this location. Otherwise, it throws an error.

Therefore, in order for SQLMesh to issue correct `CREATE TABLE` statements to Athena, there are a few strategies you can use to ensure the Athena tables are pointed to the correct S3 locations:

- Manually pre-create the `sqlmesh__` physical schemas via `CREATE SCHEMA <schema> LOCATION 's3://base/location'`. Then when SQLMesh issues `CREATE TABLE` statements for tables within that schema, Athena knows where the data should go
- Set `s3_warehouse_location` in the connection config. SQLMesh will set the table `LOCATION` to be `<s3_warehouse_location>/<schema_name>/<table_name>` when it issues a `CREATE TABLE` statement
- Set `s3_base_location` in the model `physical_properties`. SQLMesh will set the table `LOCATION` to be `<s3_base_location>/<table_name>`. This takes precedence over the `s3_warehouse_location` set in the connection config or the `LOCATION` property on the target schema

Note that if you opt to pre-create the schemas with a `LOCATION` already configured, you might want to look at [physical_schema_mapping](../../guides/configuration.md#physical-table-schemas) for better control of the schema names.

## Limitations
Athena was initially designed to read data stored in S3 and to do so without changing that data. This means that it does not have good support for mutating tables. In particular, it will not delete data from Hive tables.

Consequently, any SQLMesh model types that needs to delete or merge data from existing tables will not work. In addition, [forward only changes](../../concepts/plans.md#forward-only-change) that mutate the schemas of existing tables have a high chance of failure because Athena supports very limited schema modifications on Hive tables.

However, Athena does support [Apache Iceberg](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html) tables which allow a full range of operations. These can be used for more complex model types such as [`INCREMENTAL_BY_UNIQUE_KEY`](../../concepts/models/model_kinds.md#incremental_by_unique_key) and [`SCD_TYPE_2`](../../concepts/models/model_kinds.md#scd-type-2).

To use an Iceberg table for a model, set `table_type='iceberg'` in the model [physical_properties](../../concepts/models/overview.md#physical_properties).

In general, Iceberg tables offer the most flexibility and you'll run into the least SQLMesh limitations when using them.
However, they're a newer feature of Athena so you may run into Athena limitations that arent present in Hive tables, [particularly around supported data types](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-supported-data-types.html).