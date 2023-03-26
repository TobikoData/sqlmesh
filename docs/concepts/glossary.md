# Glossary

## Abstract Syntax Tree
A tree representation of the syntactic structure of source code. Each tree node represents a construct that occurs. The tree is abstract because it does not represent every detail appearing in the actual syntax; it also does not have a standard representation.

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

## Integration
Combining data from various sources (such as from a data warehouse) into one unified view.

## Lineage
The lineage of your data is a visualization of the life cycle of your data as it flows from data sources downstream to consumption.

## Slowly Changing Dimension (SCD)
A dimension (in a data warehouse, typically a dataset) containing relatively static data that can change slowly but unpredictably, rather than on a regular schedule. Some examples of typical slowly changing dimensions are places and products.

## Table
A table is the visual representation of data stored in rows and columns.

## User-Defined Function (UDF)
Functions that a user of a database server provides to extend its functionality, in contrast to built-in functions that are already provided. UDFs are typically written to satisfy the particular requirements of the user.

## View
A view is the result of a SQL query on a database.

## Virtual Data Marts
Term used to describes SQLMesh's ability to share tables across environments to ensure tables are only built once while maintaining data integrity and environment isolation. See [plan application](plans.md#plan-application) for more information. 

## Virtual Update
Term used to describe a plan that can be applied without having to load any additional data or build any additional tables. See [Virtual Update](plans.md#virtual-update) for more information.
