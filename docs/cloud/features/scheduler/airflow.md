# Airflow
To integrate with Airflow, add the following library to your Airflow runtime environment:

```
$ pip install tobiko-cloud-scheduler-facade[airflow]
```

You also need to add an Airflow [connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui) containing your Tobiko Cloud credentials:

```
id: tobiko_cloud
type: HTTP
host: https://tobiko/cloud/base/url
password: <tobiko cloud API token>
```

!!! info

    Note that you can name it whatever you want, you just need to remember the name because it's used for the `conn_id` parameter below.

After setting up the connection, you can then create a DAG with the following content:
```
from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

tobiko_cloud = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud")

first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")
```

This is all that's required to integrate with Tobiko Cloud!

Once this DAG is loaded in Airflow, it will be populated with your SQLMesh model structure for the specified `environment` and will be automatically configured to trigger at the same time as the next Cloud Scheduler run.

## Configuration options

We provide the ability to customise the generated DAG.

When creating the `SQLMeshEnterpriseAirflow` object, the following parameters are supported:

| Option    | Description                                                              | Type | Required |
|-----------|--------------------------------------------------------------------------|:----:|:--------:|
| `conn_id` | The Airflow connection ID containing the Tobiko Cloud connection details | str  | Y        |

When calling `create_cadence_dag()`, the following parameters are supported:

| Option    | Description                                                                                       | Type | Required |
|---------------|-----------------------------------------------------------------------------------------------|:----:|:--------:|
| `environment` | Which SQLMesh environment to target. Default: `prod`                                          | str  | N        |
| `dag_kwargs`  | A dict of arguments to pass to the Airflow DAG object when it is created.                 | dict | N        |
| `common_task_kwargs` | A dict of kwargs to pass to all task operators in the DAG                              | dict | N        |
| `sensor_task_kwargs` | A dict of kwargs to pass to just the sensor task operators in the DAG                  | dict | N        |
| `report_task_kwargs` | A dict of kwargs to pass to just the model / progress report task operators in the DAG | dict | N        |

For example, to create the cadence dag with specific tags:
```
first_task, last_task, dag = tobiko_cloud.create_cadence_dag(
    environment="prod",
    dag_kwargs=dict(
        tags=["production", "data-engineering"]
    )
)
```

## Extending the DAG

The `create_cadence_dag()` function returns a tuple of references:

- `first_task` - a reference to the first task in the DAG, which is the `Sensor` task that synchronises with Tobiko Cloud before continuting
- `last_task` - a reference to the last task in the DAG, which is the `DummyOperator` that ensures all the models with no downstream dependencies have completed before declaring the DAG completed
- `dag` - a reference to the Airflow `DAG` object itself.

You can use these references to manipulate the DAG and attach extra behaviour.

For example, to trigger a Slack notification when a run completes:
```
first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")

first_task >> SlackAPIPostOperator(channel="#notifications", ...)
```

Or send an email and then trigger another DAG:
```
last_task >> EmailOperator(to="admin@example.com", subject="SQLMesh run complete")
last_task >> TriggerDagRunOperator(trigger_dag_id="some_downstream_job")
```

Or to trigger another DAG after a specific model has completed, without waiting for the entire DAG to complete:
```
model_a_task = dag.get_task("foo.model_a")
model_a_task >> TriggerDagRunOperator(trigger_dag_id="some_other_pipeline", ...)
```

Note:

- The Task `task_id`'s are named after the `name:` attribute on the SQLMesh `MODEL(..)` definition.
- The Task display names are the *table* portion of the model name. For example, a model called `foo.model_a` will show up as `model_a` in the Airflow DAG view, but the `task_id` will still be `foo.model_a`.
