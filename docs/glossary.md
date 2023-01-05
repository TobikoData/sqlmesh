# Glossary

**CI/CD**
<br>
An engineering process that combines both Continuous Integration (automated code creation and testing) and Continuous Delivery (deployment of code and tests) in a manner that is scalable, reliable, and secure.

**CTE**
<br>
A Common Table Expression is a temporary named result set created from a SELECT statement, which can then be used in a subsequent SELECT statement.

**DAG**
<br>
Directed Acyclic Graph. In this type of graph, objects are represented as nodes with relationships that show the dependencies between them; as such, the relationships are directed, meaning there is no way for data to travel through the graph in a loop that can circle back to the starting point. SQLMesh uses a DAG to keep track of a project's models. This allows SQLMesh to easily determine a model's lineage and to identify upstream and downstream dependencies.

**Data pipeline**
<br>
The set of tools and processes for moving data from one system to another. Datasets are then organized, transformed, and inserted into some type of database, tool, or app, where data scientists, engineers, and analysts can access the data for analysis, insights, and reporting.

**Data warehouse**
<br>
The repository that houses the single source of truth where data is stored, which is integrated from various sources. This repository, normally a relational database, is optimized for handling large volumes of data.

**Full refresh**
<br>
In a full data refresh, a complete dataset is deleted and then entirely overwritten with an updated dataset.

**Idempotency**
<br>
The property that, given a particular operation, the same outputs will be produced when given the same inputs no matter how many times the operation is applied.

**Incremental refresh**
<br>
In an incremental data refresh, incoming data is compared to existing data, and only changes are updated.

**Lineage**
<br>
The lineage of your data is a visualization of the life cycle of your data as it flows from data sources to consumption.
