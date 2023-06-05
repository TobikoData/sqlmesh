# External models
Models may reference "external" tables that have been created outside SQLMesh. These external tables are captured in SQLMesh as models with a unique "external" kind. Each external model retains metadata about a table it represents.

This metadata allows SQLMesh to provide column-level lineage and data type information for models that reference external tables.

## Generating external models schema
External models are defined in the `schema.yaml` file in the SQLMesh project's root folder.

You can create this file by either (i) allowing SQLMesh to fetch information about external tables with the `create_external_models` CLI command or (ii) writing the YAML by hand.

Consider this example model that queries an external table `external_db.external_table`:

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

The following sections demonstrate how to create an external model containing metadata about `external_db.external_table`, which contains columns `column_a` and `column_b`.

### Using CLI
Instead of creating the `schema.yaml` file manually, SQLMesh can generate it for you. The [create_external_models](../../../reference/cli#create_external_models) CLI command does exactly this.

The command locates all external tables referenced in your SQLMesh project, fetches their schemas (column names and types), and then stores them in the `schema.yaml` file.

### Writing YAML by hand
The following example demonstrates a typical content of the `schema.yaml` file:

```yaml
- name: external_db.external_table
  description: An external table
  columns:
    column_a: int
    column_b: text
- name: external_db.some_other_external_table
  description: Another external table
  columns:
    column_c: bool
    column_d: float
```

All the external models in a SQLMesh project are stored in a single `schema.yaml` file.
