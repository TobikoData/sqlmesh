# Spark

## Local/Built-in Scheduler
**Engine Adapter Type**: `spark`

NOTE: Spark may not be used for the SQLMesh [state connection](../../reference/configuration.md#connections).

### Connection options

| Option          | Description                                               |  Type  | Required |
|-----------------|-----------------------------------------------------------|:------:|:--------:|
| `type`          | Engine type name - must be `spark`                        | string |    Y     |
| `config_dir`    | Value to set for `SPARK_CONFIG_DIR`                       | string |    N     |
| `catalog`       | Spark 3.4+ Only. The catalog to use when issuing commands | string |    N     |
| `config`        | Key/value pairs to set for the Spark Configuration.       |  dict  |    N     |

## Airflow Scheduler
**Engine Name:** `spark`

The SQLMesh Spark operator is very similar to the Airflow [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html#sparksubmitoperator), and relies on the same [SparkSubmitHook](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/_api/airflow/providers/apache/spark/hooks/spark_submit/index.html#airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook) implementation.

To enable support for this operator, the Airflow Spark provider package should be installed on the target Airflow cluster as follows:
```
pip install apache-airflow-providers-apache-spark
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target cluster, queue, and deploy mode in which the Spark Job should be submitted. Refer to [Apache Spark connection](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/connections/spark.html) for more details.

By default, the connection ID is set to `spark_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "spark",
    default_catalog="<catalog name>",
    engine_operator_args={
        "connection_id": "<Connection ID>"
    },
)
```
Similarly, the `engine_operator_args` parameter can be used to override other job submission parameters, such as number of allocated cores, executors, and so forth. The full list of parameters that can be overridden can be found in `sqlmesh.schedulers.airflow.operators.spark_submit.SQLMeshSparkSubmitOperator`.

**Cluster mode**
<br><br>

Each Spark job submitted by SQLMesh is a PySpark application that depends on the SQLMesh library in its Driver process (but not in Executors). This means that if the Airflow connection is configured to submit jobs in `cluster` mode as opposed to `client` mode, the user must ensure that the SQLMesh Python library is installed on each node of a cluster where Spark jobs are submitted. This is because there is no way to know in advance which specific node to which a Driver process will be scheduled. No additional configuration is required if the deploy mode is set to `client`.
