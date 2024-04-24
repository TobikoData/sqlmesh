![SQLMesh logo](sqlmesh.svg)

SQLMesh is a next-generation data transformation and modeling framework that is backwards compatible with dbt. It aims to be easy to use, correct, and efficient. 

SQLMesh enables data practitioners to efficiently run and deploy data transformations written in SQL or Python.

Although SQLMesh will make your dbt projects more efficient, reliable, and maintainable, it is more than just a [dbt alternative](https://tobikodata.com/sqlmesh_for_dbt_1.html). 

## Select Features
* [Semantic Understanding of SQL](https://tobikodata.com/semantic-understanding-of-sql.html)
    * Compile time error checking (for 10 different SQL dialects!)
    * Definitions using [simply SQL](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/#sql-based-definition) (no need for redundant and confusing Jinja + YAML)
    * [Self documenting queries](https://tobikodata.com/metadata-everywhere.html) using native SQL Comments
* Efficiency
    * Never builds a table [more than once](https://tobikodata.com/simplicity-or-efficiency-how-dbt-makes-you-choose.html)
    * Partition-based [incremental models](https://tobikodata.com/correctly-loading-incremental-data-at-scale.html)
* Confidence
    * Plan / Apply workflow like [Terraform](https://www.terraform.io/) to understand potential impact of changes
    * Easy to use [CI/CD bot](https://sqlmesh.readthedocs.io/en/stable/integrations/github/)
    * Automatic [column level lineage](https://tobikodata.com/automatically-detecting-breaking-changes-in-sql-queries.html) and data contracts
    * [Unit tests](https://tobikodata.com/we-need-even-greater-expectations.html) and audits

For more information, check out the [website](https://sqlmesh.com) and [documentation](https://sqlmesh.readthedocs.io/en/stable/).

## Getting Started
Install SQLMesh through [pypi](https://pypi.org/project/sqlmesh/) by running:

```pip install sqlmesh```

Follow the [tutorial](https://sqlmesh.readthedocs.io/en/stable/quick_start/) to learn how to use SQLMesh.

## Join our community
We'd love to join you on your data journey. Connect with us in the following ways:

* Join the [Tobiko Slack community](https://tobikodata.com/slack) to ask questions, or just to say hi!
* File an issue on our [GitHub](https://github.com/TobikoData/sqlmesh/issues/new).
* Send us an email at [hello@tobikodata.com](hello@tobikodata.com) with your questions or feedback.

## Contribution
Contributions in the form of issues or pull requests are greatly appreciated. [Read more](https://sqlmesh.readthedocs.io/en/stable/development/) about how to develop for SQLMesh.

