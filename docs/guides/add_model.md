# Add a model

## Add a model

---

Before adding a model, ensure that you have [already created your project](/guides/create_a_project) and that you are working in a [dev environment](/concepts/environments).

---

To add a model:

1. Within your **models** folder, create a new database file. For example, `example_new_model.sql`.
2. Within the file, define your model as follows:

        MODEL (
        name sqlmesh_example.example_new_model
        )

        SELECT *
        FROM sqlmesh_example.example_incremental_model
        WHERE ds BETWEEN @start_ds and @end_ds

    **Note:** The last line in this file is required if your model is incremental. Refer to [model kinds](/../concepts/models/model_kinds) for more information about the kinds of models you can create.

## Edit an existing model

To edit an existing model, open the model file you wish to edit in your preferred editor and make a change.

### Preview changes

To preview any changes you make to your model in order to understand their impact, use the `evaluate` command. Refer to [evaluate a model](/guides/evalute_model).
