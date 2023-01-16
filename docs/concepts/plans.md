# Plans

A plan is a set of changes that summarizes the difference between the local state of a project and the state of a target environment. In order for any model changes to take effect in a target environment a plan needs to be created and applied.

During plan creation, the local state of the SQLMesh project is compared against the state of a target environment. The difference computed this way is what constitutes a plan. Additionally, a user is prompted to categorize changes (see [Change categories](#change-categories)) to existing models in order for SQLMesh to devise a backfill strategy for models that have been affected indirectly (by being downstream dependencies of updated models). Finally, each plan requires a date range to which it will be applied. If not specified, the date range is derived automatically based on model definitions and the target environment.

The benefit of having a plan is that all changes can be reviewed and verified before they are applied to the data warehouse. A typical plan contains a combination of the following:
- A list of added models.
- A list of removed models.
- A list of directly modified models and a text diff of changes that have been made.
- A list of indirectly modified models.
- Missing data intervals for affected models.
- A date range which will be affected by the plan application.

To create a new plan, run the following CLI command:
```bash
$ sqlmesh plan
```

## Environments
By default, the `sqlmesh plan` command targets the production (`prod`) environment. A custom name can be provided as an argument to create / update a development environment. For example, to target an environment with name `my_dev`, run:
```bash
$ sqlmesh plan my_dev
```
A new environment is created automatically the first time a plan is applied to it.

All environments other than `prod` are considered to be development environments. Models in development environments get a special suffix appended to the schema portion of their names. So if the model's name is `db.model_a` it will be available under name `db__my_dev.model_a` in the `my_dev` environment.

## Change categories
Categories only need to be provided for models that have been modified directly. The categorization of indirectly modified downstream models is inferred based on upstream decisions. If more than one upstream dependency of an indirectly modified model has been modified and they have conflicting categories, then the most conservative category (breaking) is assigned to this model.

### Breaking change
If a directly modified model change is categorized as breaking, then it will be backfilled along with all its downstream dependencies. In general, this is the safest option to choose because it guarantees all downstream dependencies will reflect the change. However, it is a more expensive option because it involves additional data reprocessing which has a runtime cost associated with it (see [Backfilling](#backfilling)). Choose this option when a change has been made to a model's logic which has a functional impact on its downstream dependencies.

### Non-breaking change
A directly modified model that is classified as non-breaking will be backfilled but its downstream dependencies will not. This is a common choice in scenarios like an addition of a new column, an action which doesn't affect downstream models, since new columns can't be used by downstream models without modifying them directly.

## Plan application
Once a plan has been created and reviewed, it should then be applied in order for changes that are part of it to take effect.

Normally each model changed in a plan gets assigned with a new version. Each model version, in turn, gets a separate physical location for data. Data between different model versions is never shared. Thus an environment is just a collection of references to physical tables of model versions which that environment has been created / updated with.

![Each model version gets its own physical table while environments only contain references to these tables](plans/model_versioning.png)

*Each model version gets its own physical table while environments only contain references to these tables.*

This approach allows SQLMesh to ensure complete isolation between environments on one hand, while letting it share physical data assets between environments when it's appropriate and safe to do so on the other. Additionally, since each model change is captured in a separate physical table, going back to a previous version becomes a simple and quick operation (see [Logical updates](#logical-updates)) as long as its physical table hasn't been garbage collected by the janitor process. SQLMesh makes it really hard to accidentally and irreversibly break things.

### Backfilling
Despite all its benefits the approach described above is not without trade-offs. When a new model version is just created, a physical table assigned to it is empty. Thus SQLMesh needs to re-apply the logic of the new model version to the entire date range of this model in order to populate the new version's physical table. This process is called backfilling.

Despite the fact that backfilling happens incrementally there is an extra cost associated with this operation due to additional runtime involved. If the runtime cost is a concern a [Forward-only plan](#forward-only-plans) can be used instead.

### Logical updates
Another benefit of the aforementioned approach is that data for a new model version can be fully pre-built while still in a development environment. This means that all changes and their downstream dependencies can be fully previewed before they get promoted to the production environment. Thus the process of promoting a change to production is reduced to reference swapping. If during plan creation no data gaps have been detected and only references to new model versions need to be updated, then such update is referred to as logical. Logical updates impose no additional runtime overhead or cost.

## Forward-only plans
Sometimes the runtime cost associated with rebuilding an entire physical table is too high and outweighs the benefits a separate table provides. This is when forward-only plans come in handy.

When a forward-only plan is applied all model changes that are part of it don't get separate physical tables assigned to them. Instead physical tables of previous model versions are reused. The benefit of such a plan is that no backfilling is required, so there is no runtime overhead and hence - cost. The drawback is that going to a previous version is no longer as straightforward and requires a combination of additional forward-only changes and restatements (see [Restatement plans](#restatement-plans)). Also note that once a forward-only change is applied to production all development environments that referred to previous versions of updated models will be impacted.

To preserve isolation between environments during development, SQLMesh creates temporary physical tables for forward-only model versions and uses them for evaluation in development environments. The implication of this, however, is that only a limited change preview is available in the development environment before the change makes it into production. The date range of the preview is provided as part of plan creation.

Please note that all changes made as part of a forward-only plan automatically get a forward-only category assigned to them. Such changes can't be mixed together with breaking and non-breaking (see [Change categories](#change-categories)) ones as part of the same plan.

To create a forward-only plan the `--forward-only` option has to be added to the `plan` command:
```bash
$ sqlmesh plan --forward-only
```

## Restatement plans
There are cases when models need to be re-evaluated for a given time range, even though no changes may have been made to those model definitions. This could be due to an upstream issue with a dataset defined outside the SQLMesh platform, or when a forward-only change (see [Forward-only plans](#forward-only-plans)) needs to be applied retroactively to a bounded interval of historical data.

For this reason the `plan` command supports the `--restate-model` option which allows users to specify one or more names of a model which has to be reprocessed. Each name can also refer to an external table, defined outside SQLMesh. Application of such a plan will trigger a cascading backfill for all specified models (excluding external tables) as well as for all models downstream from them. The plan's date range in this case determines data intervals that will be affected. For example:
```bash
sqlmesh plan --restate-model db.model_a --restate-model external.table_a
```
The command above creates a plan which restates the model `db.model_a` and all its downstream dependencies as well as all models that refer to the `external.table_a` table and their downstream dependencies.
