# Airflow

SQLMesh provides first-class support for Airflow with the following capabilities:

* A Directed Acyclic Graph (DAG) generated dynamically for each model version. Each DAG accounts for all its upstream dependencies defined within SQLMesh, and only runs after upstream DAGs succeed for the time period being processed.
* Each plan application leads to the creation of a dynamically-generated DAG dedicated specifically to that Plan.
* The Airflow [Database Backend](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html) is used for persistence of the SQLMesh state, meaning no external storage or additional configuration is required for SQLMesh to work.
* The janitor DAG runs periodically and automatically to clean up DAGs and other SQLMesh artifacts that are no longer needed.
* Support for any SQL engine can be added by providing a custom Airflow Operator.

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

Due to MWAA not supporting the Airflow REST API, users are required to configure an external state connection for both the [client](../guides/connections.md#state-connection) and [Airflow cluster](#state-connection) to point to the same database.

Additional dependencies need to be installed:
```bash
pip install "sqlmesh[mwaa]"
```

Additionally, the scheduler needs to be configured accordingly:
```yaml linenums="1"
default_scheduler:
    type: mwaa
    environment: <The MWAA Environment Name>
```

Alternatively, the Airflow Webserver URL and the MWAA CLI token can be provided directly instead of the environment name:
```yaml linenums="1"
default_scheduler:
    type: mwaa
    airflow_url: https://<Airflow Webserver Host>/
    auth_token: <The MWAA CLI Token>
```
