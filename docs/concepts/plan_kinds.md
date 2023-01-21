# Plan kinds

## Forward-only plans
Sometimes the runtime cost associated with rebuilding an entire physical table is too high, and outweighs the benefits a separate table provides. This is when a forward-only plan comes in handy.

When a forward-only plan is applied, all of the contained model changes will not get separate physical tables assigned to them. Instead, physical tables of previous model versions are reused. The benefit of such a plan is that no backfilling is required, so there is no runtime overhead and hence no cost. The drawback is that reverting to a previous version is no longer as straightforward, and requires a combination of additional forward-only changes and restatements (refer to [restatement plans](#restatement-plans)). 

Also note that once a forward-only change is applied to production, all development environments that referred to the previous versions of the updated models will be impacted.

To preserve isolation between environments during development, SQLMesh creates temporary physical tables for forward-only model versions and uses them for evaluation in development environments. However, the implication of this is that only a limited change preview is available in the development environment before the change makes it to production. The date range of the preview is provided as part of plan creation.

 Note that all changes made as part of a forward-only plan automatically get a **forward-only** category assigned to them. These types of changes can't be mixed together with breaking and non-breaking changes (refer to [change categories](/concepts/plans#change-categories)) as part of the same plan.

To create a forward-only plan, the `--forward-only` option has to be added to the `plan` command:
```bash
$ sqlmesh plan --forward-only
```

## Restatement plans
There are cases when models need to be re-evaluated for a given time range, even though changes may not have been made to those model definitions. This could be due to an upstream issue with a dataset defined outside the SQLMesh platform, or when a [forward-only plan](#forward-only-plans) change needs to be applied retroactively to a bounded interval of historical data.

For this reason, the `plan` command supports the `--restate-model` option, which allows users to specify one or more names of a model to be reprocessed. Each name can also refer to an external table defined outside SQLMesh.
 
Application of such a plan will trigger a cascading backfill for all specified models (excluding external tables), as well as all models downstream from them. The plan's date range in this case determines data intervals that will be affected. For example:

```bash
sqlmesh plan --restate-model db.model_a --restate-model external.table_a
```

The command above creates a plan that restates the model `db.model_a` and all its downstream dependencies, as well as all models that refer to the `external.table_a` table and their downstream dependencies.
