# Glossary

## Abstract Syntax Tree
A tree representation of the syntactic structure of source code. Each tree node represents a construct that occurs. The tree is abstract because it does not represent every detail appearing in the actual syntax; it also does not have a standard representation.

## Automatic Data Rebasing
When merging in changes from another branch, SQLMesh automatically reads the history of changes and applies the changes and determines what needs to be rebuilt (if anything). This is done by leveraging the change category of each change to determine the lineage of breaking/non-breaking changes and where there might be overlap.  

## CI/CD
An engineering process that combines both Continuous Integration (automated code creation and testing) and Continuous Delivery (deployment of code and tests) in a manner that is scalable, reliable, and secure. SQLMesh accomplishes this with [tests](tests.md) and [audits](audits.md).

## CTE
A Common Table Expression is a temporary named result set created from a SELECT statement, which can then be used in a subsequent SELECT statement. For more information, refer to [tests](tests.md).

## DAG
Directed Acyclic Graph. In this type of graph, objects are represented as nodes with relationships that show the dependencies between them; as such, the relationships are directed, meaning there is no way for data to travel through the graph in a loop that can circle back to the starting point. SQLMesh uses a DAG to keep track of a project's models. This allows SQLMesh to easily determine a model's lineage and to identify upstream and downstream dependencies.

## Data modeling
Data modeling allows practitioners to visualize and conceptually represent how data is stored in a data warehouse. This can be done using diagrams that represent how data is interrelated.

## Data pipeline
The set of tools and processes for moving data from one system to another. Datasets are then organized, transformed, and inserted into some type of database, tool, or app, where data scientists, engineers, and analysts can access the data for analysis, insights, and reporting.

## Data transformation
Data transformation is the process of converting data from one format to another; for example, by converting raw data into a form usable for analysis by harmonizing data types, removing duplicate data, and organizing data.

## Data warehouse
The repository that houses the single source of truth where data is stored, which is integrated from various sources. This repository, normally a relational database, is optimized for handling large volumes of data.

## ELT
Acronym for Extract, Load, and Transform. The process of retrieving data from various sources, loading it into a data warehouse, and then transforming it into a usable and reliable resource for data practitioners.

## ETL
Acronym for Extract, Transform, and Load. The process of retrieving data from various sources, transforming the data into a usable and reliable resource, and then loading it into a data warehouse for data practitioners.

## Full refresh
In a full data refresh, a complete dataset is deleted and then entirely overwritten with an updated dataset.

## Idempotency
The property that, given a particular operation, the same outputs will be produced when given the same inputs no matter how many times the operation is applied.

## Incremental Loads
Incremental loads are a type of data refresh that only updates the data that has changed since the last refresh. This is significantly faster and more efficient than a full refresh loads. SQLMesh encourages developers to incrementally load when possible by offering easy to use variables and macros to help define your incremental models. See [Model Kinds](models/model_kinds.md) for more information.

## Integration
Combining data from various sources (such as from a data warehouse) into one unified view.

## Lineage
The lineage of your data is a visualization of the life cycle of your data as it flows from data sources downstream to consumption.

## Plan Summaries
An upcoming feature that allows users to see a summary of changes applied to a given environment.

## Semantic Understanding
SQLMesh, by leveraging [SQLGlot](https://github.com/tobymao/sqlglot), understands the full meaning of a SQL model. That means it can not only validate that what is written is valid SQL but also transpile (convert) that SQL into other engine dialects if needed.

## Slowly Changing Dimension (SCD)
A dimension (in a data warehouse, typically a dataset) containing relatively static data that can change slowly but unpredictably, rather than on a regular schedule. Some examples of typical slowly changing dimensions are places and products.

## Table
A table is the visual representation of data stored in rows and columns.

## User-Defined Function (UDF)
Functions that a user of a database server provides to extend its functionality, in contrast to built-in functions that are already provided. UDFs are typically written to satisfy the particular requirements of the user.

## View
A view is the result of a SQL query on a database.

## Virtual Environments
SQLMesh's unique approach to environment that allows it to provide both environment isolation and the ability to share tables across environments. This is done in a way to ensure data consistency and accuracy. See [plan application](plans.md#plan-application) for more information. 

## Virtual Update
Term used to describe a plan that can be applied without having to load any additional data or build any additional tables. See [Virtual Update](plans.md#virtual-update) for more information.

## Virtual Preview
Term used to describe the ability to create an environment without having to build any additional tables. By comparing the version of models in the repo against what currently exists, SQLMesh can create an environment that exactly represents what is in the repo without by just updating views.