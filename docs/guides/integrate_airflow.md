# Integrate with Airflow

## Integrate with Airflow

To integrate with [Airflow](/integrations/airflow):

1. Install the SQLMesh Python package on all nodes of the Airflow cluster using the following command:

        pip install sqlmesh

    **Note:** The Airflow webserver must be restarted after installation.

2. Within the Airflow dags folder, create a file called `sqlmesh.py`.

3. Within the file, add the following:

        from sqlmesh.schedulers.airflow.integration import SQLMeshAirflow

        sqlmesh_airflow = SQLMeshAirflow("spark")

        for dag in sqlmesh_airflow.dags:
            globals()[dag.dag_id] = dag
        
Now, when running the `sqlmesh plan` command, all changes will be applied on Airflow.

### Engines
Other engines can be used in place of "spark" in the example above:

* SQLMeshAirflow("databricks")
* SQLMeshAirflow("bigquery")
* SQLMeshAirflow("snowflake")
* SQLMeshAirflow("redshift")

For more information about engines, refer to [engine support](/integrations/airflow#engine-support).

On the client side, you must configure the connection to your Airflow cluster in the `config.yaml` file as follows:

        scheduler:
            type: airflow
            airflow_url: http://localhost:8080/
            username: airflow
            password: airflow
