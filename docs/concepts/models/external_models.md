# External models

An external model represents a table outside of the scope of SQLMesh. These models help the SQLMesh optimizer provide more accurate column level lineage and type information.

Defining external models allows you to perform a `SELECT *` when it was otherwise disallowed.

## Generating external models schema
You can manually define a `schema.yaml` in your root directory or run the [create_external_models](../../../reference/cli#create_external_models) command.

The format of the schema file is as follows:

```yaml
- name: my_db.my_table
  description: The description
  columns:
    column_a: int
    column_b: text
- name: my_db.my_other_table
  columns:
    column_a: bool
    column_b: float
```

The `create_external_models` finds all external models in your existing project and hits the configured metastore in order to automatically create the schema.yaml file.
