# Model Selection Guide

This guide describes how to select specific models to include in a SQLMesh plan, which can be useful when modifying a subset of the models in a SQLMesh project.

## Background

A SQLMesh [plan](../concepts/plans.md) automatically detects changes between the local version of a project and the version deployed in an environment. When applied, the plan backfills the directly modified models and their indirectly modified downstream children. This brings all model data into alignment with the local version of the project.

In large SQLMesh projects, a single model change may impact many downstream models, such that evaluating it and its affected children takes a significant amount of time. In some situations, a user is blocked by the long run time and can accomplish their task without backfilling all changed models and affected children.

SQLMesh model selection allows you to filter which direct model changes should be included into a plan. This can be useful when you only need to inspect the results of some of the model changes you have made.

Model selections only apply to models that have been directly modified. Selected models' indirectly modified children are always included in the plan, unless you additionally specify which models to backfill (more information [below](#backfill)).

## Syntax

Model selections are specified in the CLI `sqlmesh plan` argument `--select-model`. Selections may be specified in a number of ways.

The simplest selection is a single model name (e.g., `example.incremental_model`). The `--select-model` argument may be repeated to specify multiple individual model names:

```bash
sqlmesh plan --select-model "example.incremental_model" --select-model "example.full_model"
```

A selection may use the wildcard asterisk character `*` to select multiple models at once. Any model name matching the non-wildcard characters will match. For example:

- `"example.seed*"` would match both `example.seed_cities` and `example.seed_states`
- `"example.*l_model"` would match both `example.incremental_model` and `example.full_model`

### Upstream/downstream indicator

By default, only the directly changed models in a selection are included in the plan.

All of a model's changed upstream and/or downstream models may be included in a selection with the plus sign `+`. A plus sign at the beginning of a selection includes changed upstream models, and a plus sign at the end of a selection includes downstream models.

For example, consider a three model project with the following structure, where all three models have been changed:

`example.seed_model` --> `example.incremental_model` --> `example.full_model`

These selections would include different sets of models in the plan:

- `--select-model "example.incremental_model"` = `incremental_model` only
- `--select-model "+example.incremental_model"` = `incremental_model` and upstream `seed_model`
- `--select-model "example.incremental_model+"` = `incremental_model` and downstream `full_model`

The upstream/downstream indicator may be combined with the wildcard operator. For example, `--select-model "+example.*l_model"` would include all three models in the project:

- `example.incremental_model` matches the wildcard
- `example.seed_model` is upstream of the incremental model
- `example.full_model` matches the wildcard

The combination of the upstream/downstream indicator, wildcards, and multiple `--select-model` arguments enables granular and complex model selections for a plan.

## Backfill

By default, SQLMesh backfills all of a plan's directly and indirectly modified models. In large projects, a single model change may impact many downstream models, such that backfilling all the children takes a significant amount of time.

You can limit which downstream models are backfilled with `plan`'s `--backfill-model` argument, which uses the same selection [syntax](#syntax) as `--select-model`.

`--select-model` determines which directly modified models are included in a `plan`, and `--backfill-model` determines which of their children are backfilled by the `plan`.

A model's backfilled data is only current if its parents have also been backfilled, so the parents of each model specified with `--backfill-model` will also be backfilled. The `--backfill-model` option overrides `--select-model`, so an unselected directly modified model *will be* included in a plan if it is upstream of a `--backfill-model` model (see [example](#select-sushimarketing-and-backfill-sushicustomers) below).

NOTE: the `--backfill-model` argument can only be used in development environments (i.e., environments other than `prod`).

## Examples

We now demonstrate the use of `--select-model` and `--backfill-model` with the SQLMesh `sushi` example project, available in the `examples/sushi` directory of the [SQLMesh Github repository](https://github.com/TobikoData/sqlmesh).

### sushi

The sushi project generates and transforms data collected at a sushi restaurant. In this guide, we focus on a set of the project's models related to marketing and customers.

The DAG of those models displays the primary set we will use, connected by dark blue lines - `sushi.raw_marketing`, `sushi.marketing`, `sushi.customers`, and `sushi.waiter_as_customer_by_day` from bottom to top:

![SQLMesh sushi example project - customer models DAG](./model_selection/model-selection_sushi-dag.png)

To prepare for the examples, we have run an initial plan in `prod`, completed the backfill, and created an unmodified `dev` environment. We have modified the `sushi.raw_marketing` and `sushi.marketing` models to demonstrate how model selection impacts plans.

### Selection examples

#### No selection

If we run a `plan` without selecting specific models, SQLMesh includes the two directly modified models and the two indirectly modified models downstream of `sushi.marketing`:

```bash
❯ sqlmesh plan dev
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.raw_marketing
│   └── sushi.marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers
```

#### Select `marketing`

If we specify the `--select-model` option to select `"sushi.marketing"`, the directly modified `sushi.raw_marketing` model is no longer included in the plan:

```bash
❯ sqlmesh plan dev --select-model "sushi.marketing"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   └── sushi.marketing
└── Indirectly Modified:
    ├── sushi.customers
    └── sushi.waiter_as_customer_by_day
```

#### Select `+marketing`

If we specify the `--select-model` option with the upstream `+` to select `"+sushi.marketing"`, the `sushi.raw_marketing` model is selected because it is upstream of `sushi.marketing`:

```bash
❯ sqlmesh plan dev --select-model "+sushi.marketing"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.marketing
│   └── sushi.raw_marketing
└── Indirectly Modified:
    ├── sushi.customers
    └── sushi.waiter_as_customer_by_day
```

#### Select `raw_marketing`

If we specify the `--select-model` option to select `"sushi.raw_marketing"`, SQLMesh does not select `sushi.marketing` (so it is not classified as directly modified).

However, it does classify `sushi.marketing` as indirectly modified. Its direct modification is excluded by the model selection, but it is indirectly modified by being downstream of the selected `sushi.raw_marketing` model:

```bash hl_lines="7"
❯ sqlmesh plan dev --select-model "sushi.raw_marketing"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   └── sushi.raw_marketing
└── Indirectly Modified:
    ├── sushi.marketing
    ├── sushi.customers
    └── sushi.waiter_as_customer_by_day
```

#### Select `raw_marketing+`

If we specify the `--select-model` option with the downstream `+` to select `"sushi.raw_marketing+"`, the `sushi.marketing` model is selected and classified as directly modified because it is downstream of `sushi.raw_marketing`:

```bash
❯ sqlmesh plan dev --select-model "sushi.raw_marketing+"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.marketing
│   └── sushi.raw_marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers
```

#### Select `*marketing`

If we specify the `--select-model` option with the wildcard `*` to select `"sushi.*marketing"`, both `sushi.raw_marketing` and `sushi.marketing` are selected because they match the wildcard:

```bash
❯ sqlmesh plan dev --select-model "sushi.*marketing"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.raw_marketing
│   └── sushi.marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers
```

### Backfill examples

#### No backfill

Recall that a plan with no selection or backfill options includes all four models, two of which were directly and two of which were indirectly modified.

The `--backfill-model` option does not affect whether a model is included in a plan (i.e., it will still appear in the output shown in the selection examples above). Instead, it is excluded from the list of models needing backfill at the bottom of the plan's output.

With no options specified, the `plan` will backfill all four models (and others):

```bash hl_lines="16 17 21 24"
❯ sqlmesh plan dev
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.raw_marketing
│   └── sushi.marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers

< output omitted>

Models needing backfill (missing dates):
├── sushi__dev.items: 2023-12-06 - 2023-12-06
├── sushi__dev.orders: 2023-12-06 - 2023-12-06
├── sushi__dev.raw_marketing: 2023-11-30 - 2023-12-06
├── sushi__dev.marketing: 2023-12-06 - 2023-12-06
├── sushi__dev.order_items: 2023-12-06 - 2023-12-06
├── sushi__dev.customer_revenue_by_day: 2023-12-06 - 2023-12-06
├── sushi__dev.customer_revenue_lifetime: 2023-12-06 - 2023-12-06
├── sushi__dev.customers: 2023-11-30 - 2023-12-06
├── sushi__dev.waiter_revenue_by_day: 2023-12-06 - 2023-12-06
├── sushi__dev.top_waiters: 2023-12-06 - 2023-12-06
└── sushi__dev.waiter_as_customer_by_day: 2023-11-30 - 2023-12-06
```

#### Backfill `sushi.customers`

If we specify the `--backfill-model` option with `"sushi.customers"`, two things happen:

1. The indirectly modified `sushi.waiter_as_customer_by_day` model is excluded from the backfills list because it is downstream of `sushi.customers`
2. Many other models are excluded from the backfills list because they were neither indirectly modified nor in the path between `sushi.raw_marketing` and `sushi.customers`

```bash hl_lines="15-17"
❯ sqlmesh plan dev --backfill-model "sushi.customers"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   ├── sushi.marketing
│   └── sushi.raw_marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers

<output omitted>

Models needing backfill (missing dates):
├── sushi__dev.orders: 2023-12-06 - 2023-12-06
├── sushi__dev.raw_marketing: 2023-11-30 - 2023-12-06
├── sushi__dev.marketing: 2023-12-06 - 2023-12-06
└── sushi__dev.customers: 2023-11-30 - 2023-12-06
```

#### Select `sushi.marketing` and backfill `sushi.customers`

This example demonstrates one potentially surprising way model selection and backfill specification interact.

We select `sushi.marketing`, so `sushi.raw_marketing` does not appear in the list of directly modified models. However, it *does* appear in the list of models needing backfill because it is upstream of `sushi.customers`:

```bash hl_lines="4 5 14"
❯ sqlmesh plan dev --select-model "sushi.marketing" --backfill-model "sushi.customers"
Summary of differences against `dev`:
Models:
├── Directly Modified:
│   └── sushi.marketing
└── Indirectly Modified:
    ├── sushi.waiter_as_customer_by_day
    └── sushi.customers

<output omitted>

Models needing backfill (missing dates):
├── sushi__dev.orders: 2023-12-06 - 2023-12-06
├── sushi__dev.raw_marketing: 2023-12-06 - 2023-12-06
├── sushi__dev.marketing: 2023-12-06 - 2023-12-06
└── sushi__dev.customers: 2023-12-06 - 2023-12-06
```
