# External models

SQLMesh model queries may reference "external" tables that are created and managed outside the SQLMesh project. For example, a model might ingest data from a third party's read-only data system.

SQLMesh does not manage external tables, but it can use information about the tables' columns and data types to make features more useful. For example, column information allows column-level lineage to include external tables' columns.

SQLMesh stores external tables' column information as `EXTERNAL` models.

## External models are not run

`EXTERNAL` models consist solely of an external table's column information, so there is no query for SQLMesh to run.

SQLMesh has no information about the data contained in the table represented by an `EXTERNAL` model. The table could be altered or have all its data deleted, and SQLMesh will not detect it. All SQLMesh knows about the table is that it contains the columns specified in the `EXTERNAL` model's `schema.yaml` file (more information below).

SQLMesh will not take any actions based on an `EXTERNAL` model - its actions are solely determined by the model whose query selects from the `EXTERNAL` model.

The querying model's [`kind`](./model_kinds.md), [`cron`](./overview.md#cron), and previously loaded time intervals determine when SQLMesh will query the `EXTERNAL` model.

## Generating an external models schema file

External models are defined in the `schema.yaml` file in the SQLMesh project's root folder.

You can create this file by either writing the YAML by hand or allowing SQLMesh to fetch information about external tables with the `create_external_models` CLI command.

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

The following sections demonstrate how to create an external model containing `external_db.external_table`'s column information.

All of a SQLMesh project's external models are defined in a single `schema.yaml` file, so the files created below might also include column information for other external models.

### Writing YAML by hand

This example demonstrates the structure of a `schema.yaml` file:

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

It contains each `EXTERNAL` model's name, an optional description, and each of the external table's columns' name and data type.

The file can be constructed by hand using a standard text editor or IDE.

### Using CLI

Instead of creating the `schema.yaml` file manually, SQLMesh can generate it for you with the [create_external_models](../../reference/cli.md#create_external_models) CLI command.

The command identifies all external tables referenced in your SQLMesh project, fetches their column information from the SQL engine's metadata, and then stores the information in the `schema.yaml` file.

If SQLMesh does not have access to an external table's metadata, the table will be omitted from the file and SQLMesh will issue a warning.

`create_external_models` solely queries SQL engine metadata and does not query external tables themselves.
