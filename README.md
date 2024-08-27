<p align="center">
  <img src="sqlmesh.png" alt="SQLMesh logo" width="50%" height="50%">
</p>

SQLMesh is a next-generation data transformation and modeling framework that is backwards compatible with dbt. It aims to be easy to use, correct, and efficient.

SQLMesh enables data teams to efficiently run and deploy data transformations written in SQL or Python.

It is more than just a [dbt alternative](https://tobikodata.com/reduce_costs_with_cron_and_partitions.html).

<p align="center">
  <img src="architecture_diagram.png" alt="Architecture Diagram">
</p>

## Core Features
<p align="center">
  <img src="sqlmesh_plan_mode.gif" alt="SQLMesh Plan Mode">
</p>

> Get instant SQL impact analysis of your changes, whether in the CLI or in [SQLMesh Plan Mode](https://sqlmesh.readthedocs.io/en/stable/guides/ui/?h=plan+mode#working-with-an-ide)

Virtual data environments with a toggle to show the disco deck slides in order
* TODO: include a couple bullets that summarize the features
* Virtual Data Environments
    * Plan / Apply workflow like [Terraform](https://www.terraform.io/) to understand potential impact of changes
    * Automatic [column level lineage](https://tobikodata.com/automatically-detecting-breaking-changes-in-sql-queries.html) and data contracts
    * Easy to use [CI/CD bot](https://sqlmesh.readthedocs.io/en/stable/integrations/github/)
* Efficiency and Testing
    * Never builds a table [more than once](https://tobikodata.com/simplicity-or-efficiency-how-dbt-makes-you-choose.html)
    * Partition-based [incremental models](https://tobikodata.com/correctly-loading-incremental-data-at-scale.html)
    * [Unit tests](https://tobikodata.com/we-need-even-greater-expectations.html) and audits

    <details>
    <summary>Take SQL Anywhere</summary>
    <img src="transpile_example.png" alt="Transpile Example">
    </details>

    * Compile time error checking and can transpile 10+ different SQL dialects
    * Definitions using [simply SQL](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/#sql-based-definition) (no need for redundant and confusing Jinja + YAML)
    * [Self documenting queries](https://tobikodata.com/metadata-everywhere.html) using native SQL Comments


For more information, check out the [website](https://sqlmesh.com) and [documentation](https://sqlmesh.readthedocs.io/en/stable/).

## Getting Started
Install SQLMesh through [pypi](https://pypi.org/project/sqlmesh/) by running:

```bash
mkdir sqlmesh-example
cd sqlmesh-example
python -m venv .env
source .env/bin/activate
pip install sqlmesh
sqlmesh init duckdb # get started right away with a local duckdb instance
```

Follow the [quickstart guide](https://sqlmesh.readthedocs.io/en/stable/quickstart/cli/#1-create-the-sqlmesh-project) to learn how to use SQLMesh. You already have a head start!

## Join Our Community
We want to ship better data with you. Connect with us in the following ways:

* Join the [Tobiko Slack Community](https://tobikodata.com/slack) to ask questions, or just to say hi!
* File an issue on our [GitHub](https://github.com/TobikoData/sqlmesh/issues/new)
* Send us an email at [hello@tobikodata.com](mailto:hello@tobikodata.com) with your questions or feedback

## Contribution
Contributions in the form of issues or pull requests are greatly appreciated. [Read more](https://sqlmesh.readthedocs.io/en/stable/development/) on how to contribute to SQLMesh open source.

