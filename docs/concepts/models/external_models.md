# External models
Some SQLMesh models query "external" tables that were created by code unavailable to SQLMesh. External models are a special model kind used to store metadata about those external tables.

External models allow SQLMesh to provide column-level lineage and data type information for external tables queried with SELECT *.

## Generating external models schema
External models consist of an external table's schema information stored in the `schema.yaml` file in the SQLMesh project's root directory.

You can add schema information to the file by either (i) writing the YAML by hand or (ii) allowing SQLMesh to query the external table and add the information itself with the create_external_models CLI command.

Consider this example FULL model that queries an external table external_db.external_table:

```sql
MODEL (
  name my_db.my_table,
  kind FULL
);

SELECT
  *
FROM
  external_db.external_table;
```

The following sections demonstrate how to create an external model containing metadata about external_db.external_table, which contains columns column_a and column_b.

## Writing YAML by hand
This example demonstrates how the schema.yaml file should be formatted.

```yaml
- name: external_db.external_table
  description: An external table
  columns:
    column_a: int
    column_b: text
```

All the external models in a SQLMesh project are stored in one schema.yaml file. The file might look like this with an additional external model:

```yaml
- name: external_db.external_table
  description: An external table
  columns:
    column_a: int
    column_b: text
- name: external_db.external_table_2
  description: Another external table
  columns:
    column_c: bool
    column_d: float
```

### Using the create_external_models CLI command
Instead of writing the external model YAML by hand, SQLMesh can create it for you with the [create_external_models](../../../reference/cli#create_external_models) CLI command.

The command locates all external tables queried in your SQLMesh project, executes the queries, and infers the tables' column names and types from the results.

It then writes that information to the schema.yaml file.
