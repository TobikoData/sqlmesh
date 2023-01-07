# Overview

SQLMesh can be used with a [cli](cli.md), [notebook](notebook.md), or directly through [Python](python.md). Each interface attempts to have equivalent functionality and arguments.

## plan
Plan is the main command of SQLMesh. It allows you to interactively create a migration plan, understand the downstream impact, and apply it. All changes to models and environments are materialized through plan.

Read more about [plan](/concepts/plans).

## evaluate
Evaluate a model or snapshot (running its query against a DB/Engine). This method is used to test or iterate on models without side effects.

## render
Renders a model's SQL query with the provided arguments.

## fetchdf
Given a SQL query, fetches a pandas dataframe.

## test
Runs all tests.

## audit
Runs all audits.

## format
Formats all SQL model files in place.

## diff
Shows the diff between the local model and a model in an evironment.

## dag
Shows the DAG.
