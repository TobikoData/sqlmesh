# Airflow

SQLMesh provides first-class support for Airflow with the following capabilities:

* A Directed Acyclic Graph (DAG) generated dynamically for each model version. Each DAG accounts for all its upstream dependencies defined within SQLMesh, and only runs after upstream DAGs succeed for the time period being processed.
* Each plan application leads to the creation of a dynamically-generated DAG dedicated specifically to that Plan.
* The Airflow [Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) is used for persistence of the SQLMesh state, meaning no external storage or additional configuration is required for SQLMesh to work.
* The janitor DAG runs periodically and automatically to clean up DAGs and other SQLMesh artifacts that are no longer needed.
* Support for any SQL engine can be added by providing a custom Airflow Operator.

SQLMesh provides [partial support for AWS MWAA](#aws-mwaa) (Amazon Managed Workflows for Apache Airflow) because MWAA does not expose the standard Airflow API.

## Airflow cluster configuration
To enable SQLMesh support on a target Airflow cluster, the SQLMesh package should first be installed on that cluster. Ensure it is installed with the extras for your engine if needed; for example: `sqlmesh[databricks]` for Databricks. Check [setup.py](https://github.com/TobikoData/sqlmesh/blob/main/setup.py) for a list of extras.

**Note:** The Airflow Webserver instance(s) must be restarted after **installation** and every time the SQLMesh package is **upgraded**.

Once the package is installed, the following Python module must be created in the `dags/` folder of the target DAG repository with the following contents:

```python linenums="1"
from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

sqlmesh_airflow = SQLMeshAirflow("spark")

for dag in sqlmesh_airflow.dags:
    globals()[dag.dag_id] = dag
```
The name of the module file can be arbitrary, but we recommend something descriptive such as `sqlmesh.py` or `sqlmesh_integration.py`.

**Note**: The name of the engine operator is the only mandatory parameter needed for `sqlmesh.schedulers.airflow.integration.SQLMeshAirflow`. Currently supported engines are listed in the [Engine support](#engine-support) section.

### State connection

By default, SQLMesh uses the Airflow's database connection to read and write its state.

To configure a different storage backend for the SQLMesh state you need to create a new [Airflow Connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) with ID `sqlmesh_state_db` and type `Generic`. The configuration should be provided in the connection's `extra` field in JSON format.

![SQLMesh state connection](airflow/airflow_sqlmesh_state_connection.png)

Refer to the [Connection Configuration](../reference/configuration.md#connection) for supported fields.

## SQLMesh client configuration
In your SQLMesh repository, create the following configuration within config.yaml:
```yaml linenums="1"
default_scheduler:
    type: airflow
    airflow_url: https://<Airflow Webserver Host>:<Airflow Webserver Port>/
    username: <Airflow Username>
    password: <Airflow Password>
```

## Engine support
SQLMesh supports a variety of engines in Airflow. Support for each engine is provided by a custom Airflow operator implementation. Below is a list of links to operators supported out of the box with information on how to configure them.

* [BigQuery](engines/bigquery.md#airflow-scheduler)
* [Databricks](engines/databricks.md#airflow-scheduler)
* [Postgres](engines/postgres.md#airflow-scheduler)
* [Redshift](engines/redshift.md#airflow-scheduler)
* [Snowflake](engines/snowflake.md#airflow-scheduler)
* [Spark](engines/spark.md#airflow-scheduler)

## Managed Airflow instances

Multiple companies offer managed Airflow instances that integrate with their products. This section describes SQLMesh support for some of the options.

### Google Cloud Composer

SQLMesh fully supports Airflow hosted on [Google Cloud Composer](https://cloud.google.com/composer/docs/composer-2/composer-overview) - see the [configuration reference page](../reference/configuration.md#cloud-composer) for more information.

### Astronomer

Astronomer provides [managed Airflow instances](https://www.astronomer.io/product/) running on AWS, GCP, and Azure. SQLMesh fully supports Airflow hosted by Astronomer.

### AWS MWAA

SQLMesh does not fully support AWS MWAA (Amazon Managed Workflows for Apache Airflow) because MWAA does not expose the standard Airflow API. However, SQLMesh can run scheduled DAGs on MWAA via an external state connection.

In this approach, `sqlmesh plan` and `sqlmesh run` behave differently:

- The `sqlmesh plan` command is issued by users. When they issue the command and choose to backfill, the built-in scheduler executes the project's models in the data warehouse.
- The `sqlmesh run` command is issued by the MWAA Airflow instance on an appropriate cadence - users do not run the command themselves. MWAA then runs the DAG and executes the models in the data warehouse.

The SQLMesh janitor process will also run automatically to clean up DAGs and other SQLMesh artifacts that are no longer needed.

To implement this approach:

1. Follow the normal [Airflow cluster configuration steps](#airflow-cluster-configuration) on the MWAA instance.
2. In MWAA, configure an [Airflow state connection](#state-connection) to a database accessible by your SQLMesh client.
3. In your SQLMesh project [`config` file](../reference/configuration.md#configuration-files), configure an [external state connection](../guides/connections.md#state-connection) to the same database from step 2.
4. Do **not** specify a [gateway scheduler configuration](../reference/configuration.md#scheduler) in your project's [`config` file](../reference/configuration.md#configuration-files).
