# Add a model

## Add a model

---

Before adding a model, ensure that you have [created your project structure](/guides/create_a_project) and that you are working in a [dev environment](/concepts/environments).

---

To add a model:

1. Within your **models** folder, create a new file. For example: `example_new_model.sql`.
2. Within this file, define your model as follows:

    ```
    MODEL (
    ```

    ```
    name sqlmesh_example.example_new_model
    ```

    ```
    )
    ```

    ```
    SELECT *
    ```

    ```
    FROM sqlmesh_example.example_incremental_model
    ```

    ```
    WHERE ds BETWEEN @start_ds and @end_ds
    ```

    **Note:** The last line in this file is required if your model is incremental. Refer to [model kinds](/../concepts/models/model_kinds) for more information about the kinds of models you can create.

## Edit an existing model

To edit an existing model, open the model file you wish to edit in your preferred editor and make a change.

### Preview changes

You can then preview any changes you made to the model in order to understand their impact by using the `plan` command as follows:

1. Preview changes by entering the `sqlmesh plan dev` command.
2. Hit `Enter` to leave the backfill start and end dates empty.

Observe that SQLMesh detects your changes and shows you the difference between the old model and the new model. SQLMesh will also show you whether any downstream models are impacted; if so, SQLMesh will prompt you to classify the changes as [Breaking](/../concepts/plans#breaking-change) or [Non-Breaking](/../concepts/plans#non-breaking-change) before applying the changes.
