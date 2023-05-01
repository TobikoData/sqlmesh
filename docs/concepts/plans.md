# Plans

A plan is a set of changes that summarizes the difference between the local state of a project and the state of a target [environment](environments.md). In order for any model changes to take effect in a target environment, a plan needs to be created and applied.

During plan creation:

* the local state of the SQLMesh project is compared against the state of a target environment. The difference computed is what constitutes a plan.
* users may be prompted to categorize changes (refer to [change categories](#change-categories)) to existing models in order for SQLMesh to devise a backfill strategy for models that have been affected indirectly (by being downstream dependencies of updated models). By default, SQLMesh attempts to categorize changes automatically, but this behavior can be changed through [configuration](../reference/configuration.md#auto_categorize_changes).
* each plan requires a date range to which it will be applied. If not specified, the date range is derived automatically based on model definitions and the target environment.

The benefit of having a plan is that all changes can be reviewed and verified before they are applied to the data warehouse. A typical plan contains a combination of the following:

* A list of added models.
* A list of removed models.
* A list of directly modified models and a text diff of changes that have been made.
* A list of indirectly modified models.
* Missing data intervals for affected models.
* A date range that will be affected by the plan application.

To create a new plan, run the following command:
```bash
sqlmesh plan
```
## Change categories
Categories only need to be provided for models that have been modified directly. The categorization of indirectly modified downstream models is inferred based on upstream decisions. If more than one upstream dependency of an indirectly modified model has been modified and they have conflicting categories, the most conservative category (breaking) is assigned to this model.

### Breaking change
If a directly modified model change is categorized as breaking, then it will be backfilled along with all its downstream dependencies. In general, this is the safest option to choose because it guarantees all downstream dependencies will reflect the change. However, it is a more expensive option because it involves additional data reprocessing, which has a runtime cost associated with it (refer to [backfilling](#backfilling)). Choose this option when a change has been made to a model's logic that has a functional impact on its downstream dependencies.

### Non-breaking change
A directly-modified model that is classified as non-breaking will be backfilled, but its downstream dependencies will not. This is a common choice in scenarios such as an addition of a new column, an action which doesn't affect downstream models as new columns can't be used by downstream models without modifying them directly.

## Plan application
Once a plan has been created and reviewed, it should then be applied to a target [environment](environments.md) in order for the changes that are part of it to take effect.

Every time a model is changed as part of a plan, a new variant of this model gets created behind the scenes (a [snapshot](architecture/snapshots.md)) with a unique [fingerprint](architecture/snapshots.md#fingerprints) assigned to it. In turn, each model variant gets a separate physical location for data (i.e. table). Data between different variants of the same model is never shared (except for the [forward-only](#forward-only-plans) case).

When a plan is applied to an environment, that environment gets associated with a collection of model variants that are part of that plan. In other words, each environment is a collection of references to model variants and the physical tables associated with them.

![Each model variant gets its own physical table, while environments only contain references to these tables](plans/model_versioning.png)

*Each model variant gets its own physical table while environments only contain references to these tables.*

This unique approach to understanding and applying changes is what enables SQLMesh's Virtual Environments. This technology allows SQLMesh to ensure complete isolation between environments while allowing it to share physical data assets between environments when appropriate and safe to do so. Additionally, since each model change is captured in a separate physical table, reverting to a previous version becomes a simple and quick operation (refer to [Virtual Update](#virtual-update)) as long as its physical table hasn't been garbage collected by the janitor process. SQLMesh makes it easy to be correct, and really hard to accidentally and irreversibly break things.

### Backfilling
Despite all the benefits, the approach described above is not without trade-offs. When a new model version is just created, a physical table assigned to it is empty. Therefore, SQLMesh needs to re-apply the logic of the new model version to the entire date range of this model in order to populate the new version's physical table. This process is called backfilling.

At the moment, we are using the term backfilling broadly to describe any situation in which a model is updated. That includes these operations: 

* When a VIEW model is created
* When a FULL model is built 
* When an INCREMENTAL model is built for the first time
* When an INCREMENTAL model has recent data appended to it
* When an INCREMENTAL model has older data inserted (i.e., resolving a data gap or prepending historical data)

We will be iterating on terminology to better capture the nuances of each type in future versions. 

Note for incremental models: despite the fact that backfilling can happen incrementally (see `batch_size` parameter on models), there is an extra cost associated with this operation due to additional runtime involved. If the runtime cost is a concern, a [forward-only plan](#forward-only-plans) can be used instead.

### Virtual Update
Another benefit of the aforementioned approach is that data for a new model version can be fully pre-built while still in a development environment. This means that all changes and their downstream dependencies can be fully previewed before they get promoted to the production environment. Therefore, the process of promoting a change to production is reduced to reference swapping. If during plan creation no data gaps have been detected and only references to new model versions need to be updated, then such update is referred to as a Virtual Update. Virtual Updates impose no additional runtime overhead or cost.

## Forward-only plans
Sometimes the runtime cost associated with rebuilding an entire physical table is too high and outweighs the benefits a separate table provides. This is when a forward-only plan comes in handy.

When a forward-only plan is applied, all of the contained model changes will not get separate physical tables assigned to them. Instead, physical tables of previous model versions are reused. The benefit of such a plan is that no backfilling is required, so there is no runtime overhead and hence no cost. The drawback is that reverting to a previous version is no longer as straightforward, and requires a combination of additional forward-only changes and restatements (refer to [restatement plans](#restatement-plans)).

Also note that once a forward-only change is applied to production, all development environments that referred to the previous versions of the updated models will be impacted.

To preserve isolation between environments during development, SQLMesh creates temporary physical tables for forward-only model versions and uses them for evaluation in development environments. However, the implication of this is that only a limited change preview is available in the development environment before the change makes it to production. The date range of the preview is provided as part of plan creation.

 Note that all changes made as part of a forward-only plan automatically get a **forward-only** category assigned to them. These types of changes can't be mixed together with breaking and non-breaking changes (refer to [change categories](#change-categories)) as part of the same plan.

To create a forward-only plan, the `--forward-only` option has to be added to the `plan` command:
```bash
sqlmesh plan --forward-only
```

### Effective date
Changes that are part of the forward-only plan can also be applied retroactively to the production environment by specifying the effective date:
```bash
sqlmesh plan --forward-only --effective-from 2023-01-01
```
This way SQLMesh will know to recompute data intervals starting from the specified date once forward-only changes are deployed to production.

## Restatement plans
There are cases when models need to be re-evaluated for a given time range, even though changes may not have been made to those model definitions. This could be due to an upstream issue with a dataset defined outside the SQLMesh platform, or when a [forward-only plan](#forward-only-plans) change needs to be applied retroactively to a bounded interval of historical data.

For this reason, the `plan` command supports the `--restate-model` option, which allows users to specify one or more names of a model to be reprocessed. Each name can also refer to an external table defined outside SQLMesh.

Application of such a plan will trigger a cascading backfill for all specified models (excluding external tables), as well as all models downstream from them. The plan's date range in this case determines data intervals that will be affected. For example:

```bash
sqlmesh plan --restate-model db.model_a --restate-model external.table_a
```

The command above creates a plan that restates the model `db.model_a` and all its downstream dependencies, as well as all models that refer to the `external.table_a` table and their downstream dependencies.
