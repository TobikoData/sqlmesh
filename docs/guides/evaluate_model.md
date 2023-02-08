# Evaluate a model

## Evaluate a model

To evaluate a model, you can run the `evaluate` command with either the [CLI](/reference/cli) or [Notebook](/reference/notebook). This command will run a query against your database or engine and return a dataframe. It is used to test or iterate on models without side effects, and since SQLMesh isn't materializing any data, with minimal cost.

An example of the `evaluate` command using the example incremental model from the [quickstart](/quick_start) is as follows:

```
sqlmesh evaluate sqlmesh_example.example_incremental_model --start=2020-01-07 --end=2020-01-07
```

Once the `evaluate` command runs and detects the changes made to your model, the output from your new model will be shown as in the example below:

```
   id  item_id          ds
0   7        1  2020-01-07
```
