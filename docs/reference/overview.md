# Overview

SQLMesh can be used with a [CLI](cli.md), [Notebook](notebook.md), or directly through [Python](python.md). Each interface aims to have parity in both functionality and arguments. The following is a list of available commands.

## plan
Plan is the main command of SQLMesh. It allows you to interactively create a migration plan, understand the downstream impact, and apply it. All changes to models and environments are materialized through `plan`.

Read more about [plans](../concepts/plans.md).

## evaluate
Evaluate a model or snapshot (running its query against a DB/Engine). This command is used to test or iterate on models without side effects.

## render
Renders a model's SQL query with the provided arguments.

## fetchdf
Given a SQL query, fetches a pandas dataframe.

## test
Runs all tests.

Read more about [testing](../concepts/tests.md).

## audit
Runs all audits.

Read more about [auditing](../concepts/audits.md).

## format
Formats all SQL model and audit files in place.

## diff
Shows the diff between the local model and a model in an environment.

## dag
Shows the [DAG](../concepts/glossary.md#dag).
