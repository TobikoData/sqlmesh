# FAQ

## General

???+ question "What is SQLMesh?"
    SQLMesh is an open source data transformation framework that brings the best practices of DevOps to data teams. It enables data engineers, scientists, and analysts to efficiently run and deploy data transformations written in SQL or Python. 
    
    It is created and maintained by Tobiko Data, a company founded by data leaders from Airbnb, Apple, and Netflix. 
    
    Check out the [quickstart guide](./quick_start.md) to see it in action.

??? question "What is SQLMesh used for?"
    SQLMesh is used to manage and execute data transformations - the process of converting raw data into a form useful for making business decisions. 

??? question "What problems does SQLMesh solve?"
    **Problem: organizing, maintaining, and changing data transformation code in SQL or Python**

    Solutions:

    - Identify dependencies among data transformation models and determine the order in which they should run
    - Run data audits and unit tests to prevent unintended side effects from code changes
    - Implement best practices from the DevOps paradigm, such as development environments and continuous integration/continuous development (CI/CD)
    - Execute transformations written in one SQL dialect on an engine/database that runs a different SQL dialect (SQL transpilation)

    <br>

    **Problem: understanding a complex set of data transformations**

    Solutions:

    - Determine and display the flow of data through data transformation models
    - Trace which columns in a table contribute to a column in another table (column-level lineage)

    <br>

    **Problem: inefficient, unnecessarily expensive data transformations**

    Solutions:

    - Understand the impacts of a code change on the codebase and underlying data tables *without running the code*
    - Efficiently deploy code changes by only running the transformations impacted by the changes
    - Safely promote transformations executed in a development environment to production so computations aren’t needlessly re-executed

    <br>

    **Problem: complex business requirements and data transformations**

    Solutions:

    - Easily and safely implement incremental data loading
    - Perform complex data transformations or operations with Python models (e.g., machine learning models, geocoding)

    <br>

    ...and more!

??? question "What is semantic understanding of SQL?"
    Semantic understanding is the result of analyzing SQL code to determine what it does at a granular level. SQLMesh uses the free, open-source Python library [SQLGlot](https://github.com/tobymao/sqlglot) to parse the SQL code and build the semantic understanding.
    
    Semantic understanding allows SQLMesh to do things like transpilation (executing one SQL dialect on an engine running another dialect) and protecting incremental loading queries from duplicating data.

## Getting started

??? question "How do I install SQLMesh?"
    SQLMesh is a Python library. After ensuring you have [an appropriate Python runtime](./prerequisites.md), install it [with `pip`](./installation.md).  

??? question "How do I use SQLmesh?"
    SQLMesh has three interfaces: [command line](./reference/cli.md), [Jupyter or Databricks notebook](./reference/notebook.md), and graphical user interface. 
    
    The [quickstart guide](./quick_start.md) demonstrates an example project in each of the interfaces.

## Databases/Engines

??? question "What databases/engines does SQLMesh work with?"
    SQLMesh works with BigQuery, Databricks, DuckDB, PostgreSQL, GCP PostgreSQL, Redshift, Snowflake, and Spark. See [this page](./integrations/engines.md) for more information.

??? question "When would you use different databases for executing data transformations and storing state information?"
    SQLMesh requires storing information about projects and when their transformations were run. By default, it stores this information in the same database where the models run. 
    
    Unlike data transformations, storing state information requires database transactions. Some databases, like BigQuery, aren’t optimized for executing transactions, so storing state information in them can slow down your project. If this occurs, you can store state information in a different database, such as PostgreSQL, that executes transactions more efficiently.

## How is this different from dbt?

??? question "Terminology differences?"
    - dbt “materializations” are analogous to [`model kinds` in SQLMesh](./concepts/models/model_kinds.md)
    - dbt seeds are a [model kind in SQLMesh](./concepts/models/model_kinds.md#seed)
    - dbt’s “tests” are called [`audits` in SQLMesh](./concepts/audits.md) because they are auditing the contents of *data* that already exists. [SQLMesh `tests`](./concepts/tests.md) are equivalent to “unit tests” in software engineering - they evaluate the correctness of *code* based on known inputs and outputs.
    - `dbt build` is analogous to [`sqlmesh run`](./reference/cli.md#run)

??? question "Workflow differences?"
    **dbt workflow**

    - Configure your project and set up one database connection target for each environment you will use during development
    - Create, configure, and modify models, seeds, tests, and other project components
    - Execute `dbt build` (or its constituent parts `dbt run`, `dbt seed`, etc.) to evaluate and test the project components
    - Execute `dbt build` (or its constituent parts `dbt run`, `dbt seed`, etc.) on a schedule to ingest and transform new data

    **SQLMesh workflow**

    - Configure your project and set up a project database (using DuckDB locally or a database connection)
    - Create, configure, and modify models, audits, tests, and other project components
    - Execute `sqlmesh plan [environment name]` to: 
        - Generate a summary of the differences between your project files and the environment and whether each change is `breaking`. The `plan` includes a list of the actions needed to implement the changes and automatically runs the project's unit `test`s. 
        - Optionally apply the plan to implement the actions and run the project's `audit`s.
    - Execute `sqlmesh run` on a schedule to ingest and transform new data

??? question "Differences in running models?"
    dbt projects are executed with the commands `dbt run` (models only) or `dbt build` (models, tests, snapshots). 
    
    In SQLMesh, the execution depends on whether the project’s contents have been modified since the last execution:

    - If they have been modified, the `sqlmesh plan` command both:
        1. Generates a summary of the actions that will occur to implement the code changes and 
        2. Prompts the user to "apply" the plan and execute those actions.
    - If they have not been modified, the [`sqlmesh run`](./reference/cli.md#run) command will evaluate the project models and run the audits. SQLMesh determines which project models should be executed based on their [`cron` configuration parameter](./concepts/models/overview.md#cron). 
    
        For example, if a model’s `cron` is `daily` then `sqlmesh run` will only execute the model once per day. If you issue `sqlmesh run` the first time on a day the model will execute; if you issue `sqlmesh run` again nothing will happen because the model shouldn’t be executed again until tomorrow.

??? question "Differences in state management?"
    **dbt**

    By default, dbt runs/builds are independent and have no knowledge of previous runs/builds. This knowledge is called “state” (as in “the state of things”).
    
    dbt has the ability to store/maintain state with the `state` selector method and the `defer` feature. dbt stores state information in `artifacts` like the manifest JSON file and reads the files at runtime.

    The dbt documentation [“Caveats to state comparison” page](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats) comments on those features: “The state: selection method is a powerful feature, with a lot of underlying complexity.”

    **SQLMesh**

    SQLMesh always maintains state about the project structure, contents, and past runs. State information enables powerful SQLMesh features like virtual data environments and easy incremental loads.

    State information is stored by default - you do not need to take any action to maintain or to use it when executing models. As the dbt caveats page says, state information is powerful but complex. SQLMesh handles that complexity for you so you don't need to learn about or understand the underlying mechanics.

    SQLMesh stores state information in database tables. By default, it stores this information in the same [database/connection where your project models run](./reference/configuration.md#gateways). You can specify a [different database/connection](./reference/configuration.md#state-connection) if you would prefer to store state information somewhere else. 
    
    SQLMesh adds information to the state tables via transactions, and some databases like BigQuery are not optimized to execute transactions. Changing the state connection to another database like PostgreSQL can alleviate performance issues you may encounter due to state transactions.
