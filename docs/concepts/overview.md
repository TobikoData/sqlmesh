# Overview

This page provides a conceptual overview of what SQLMesh does and how its components fit together.

## What is data transformation?
Data is essential for understanding what is happening with your business or application; it helps you make informed decisions. 

However, data in its raw form (application logs, transactional database tables, etc.) is not particularly useful for making decisions. By joining various tables together or computing aggregations, it's easier to interpret, analyze, and take action based on data. 

As data becomes integrated into your business operations, you are likely to add more raw data sources, create more joins and aggregations, and send results to more places (such as BI dashboards or executive reports). Code and configurations proliferate, and the overall system complexity becomes overwhelming.

This is where a data transformation platform comes in: to make it easy to create and organize complex data systems with many dependencies and relationships.

## Data transformation processes

There are many types of data transformation processes. We will focus on three, which we refer to as manual, orchestrator-based, and model-aware. 

Data transformation tools are typically built with only one of these processes in mind. It may be possible to use different processes with one tool, but the tool's design likely makes one process easier and cleaner to implement than others.

In the context of data transformation tools, a "model" is a unit of of code that returns a single table, view, or analogous entity (e.g., Spark dataframe). Models are typically written in SQL, although some tools allow models in other languages like Python. At a high level, all the data transformations executed in a project are specified in its collection of models. 

### Manual
If your data or organization is small, you may only have a small number of models to compute. In these scenarios, running SQL queries or Python scripts manually will get the job done. As your organization grows (more people or more data), however, a manual process quickly becomes unmaintainable.

### Orchestrator-based
Organizations that have outgrown manual processes often switch to an orchestrator-based process using tools such as [Airflow](https://airflow.apache.org/) or [Prefect](https://www.prefect.io/). Although these tools handle dependencies and scheduling, they are very generic. 

You may need to develop custom tooling to make it easier to work with these frameworks. Additionally, less technical data professionals may have trouble working with them because they are complex and designed for engineers.

### Model-aware
Model-aware data transformation processes integrate more elements of building data systems than just orchestration. They are implemented with tools like SQLMesh, [dbt](https://www.getdbt.com/), and [coalesce](https://coalesce.io/). 

These tools are "model-aware" because they can automatically determine the set of actions needed to accomplish a task based on the project's models and their dependency structure. 

Unlike generic orchestrators, these tools automate common data modeling patterns. For example, you may want some (but not all) models' outputs to be "materialized" and stored on disk. Model-aware tools may support materialization *strategies* instead of requiring you to specify materializations for each individual model.

Read more about why SQLMesh is the most efficient and powerful model-aware platform [here](../index.md).

## How SQLMesh works
SQLMesh is a Python framework that automates everything needed to run a scaleable data transformation platform. SQLMesh works with a variety of [engines and orchestrators](../integrations/overview.md). 

It was created with a focus on both data and organizational scale and works regardless of your data warehouse or SQL engine's capabilities.

You can use SQLMesh with the [CLI](../reference/cli.md), [notebook](../reference/notebook.md), or [Python](../reference/python.md) APIs.

### Create models
You begin by writing your business logic in SQL or Python. A model consists of code that returns a single table, view, or analogous entity (e.g., Spark dataframe).

### Plan
Creating new models or changing existing models can have dramatic effects downstream when working with large data transformation systems. Complex interdependencies between models make it challenging to determine the implications of changes to even a single model. 

Beyond understanding the logical implications of a change, you also need to understand the computations required to implement the change before expending the time and resources to actually perform the computations.

SQLMesh automatically identifies all affected models and the computations a model change entails by creating a "SQLMesh plan." When you execute the `plan` command, SQLMesh generates the plan *for the environment specified in the command* (e.g., dev, test, prod). 

The plan allows you to understand the full scope of a change's effects in the environment by automatically identifying both directly and indirectly-impacted models. This gives a holistic view of all impacts a change will have. Learn more about [plans](./plans.md).

### Apply
After using `plan` to understand the impacts of a change in an environment, SQLMesh offers to execute the computations by `apply`ing the plan. However, you must provide additional information that determines the scope of what computations are executed. 

The computations needed to apply a SQLMesh plan are determined by both the code changes reflected in the plan and the backfill parameters you specify.

"Backfilling" is the process of updating existing data to align with your changed models. For example, if your model change alters a calculation, then all existing data based on the old calculation method will be inaccurate once the new model is deployed. Backfilling entails re-calculating the existing fields whose calculation method has now changed.

Most business data is temporal - each data fact was collected at a specific moment in time. The scale of backfill computations is directly tied to how much historical data is to be re-calculated.

The SQLMesh plan automatically determines and displays which models and dates require backfill due to your changes. Based on this information, you specify the dates for which backfills will occur before you apply the plan.

### Deploy
Development activities for complex data systems should occur in a non-production environment so errors can be detected before being deployed in production systems.

One challenge with using multiple data environments is that backfill and other computations must happen twice - once for the non-production and again for the production environment. This process consumes time and computing resources, resulting in delays and extra costs.

SQLMesh solves this problem by maintaining a record of all model versions, model outputs, and their changes. It uses this record to determine when computations executed in a non-production environment generate results identical to what they would generate in the production environment. 

When the results are identical, SQLMesh replaces references to outdated tables in the production environment with references to newly computed tables in the non-production environment. It effectively promotes tables themselves from non-production to production, but *without computation or data movement*.

Because of these features, promoting changes to production is quick and has no downtime. 

## Infrastructure and deployment
Every company's data infrastructure is different. SQLMesh is flexible with regard to which engines and orchestration frameworks you use - its only requirement is access to the target SQL / analytics engine.

SQLMesh keeps track of model versions and processed data intervals using your existing infrastructure. If SQLMesh is configured without an external orchestrator (like Airflow), it automatically creates a `sqlmesh` database in your data warehouse for its internal metadata. 

If SQLMesh is configured with Airflow, then it will store all its metadata in the Airflow database. Read more about how [SQLMesh integrates with Airflow](../integrations/airflow.md).
