# Airflow

This integration with Tobiko Cloud enables you to synchronize your Airflow monitoring and debugging experience with Tobiko Cloud.

To set up the integration, add the following library to your Airflow runtime environment:

```
$ pip install tobiko-cloud-scheduler-facade[airflow]
```

You also need to add an Airflow [connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui) containing your Tobiko Cloud credentials:

```yaml
id: tobiko_cloud
type: HTTP
host: https://tobiko/cloud/base/url
password: <tobiko cloud API token>
```

![add_connection](./airflow/add_connection.png)

!!! info

    You can name the connection whatever you like, you just need to remember the name because it's used for the `conn_id` parameter below.

![connection_list](./airflow/connection_list.png)

After setting up the connection, you can then create a DAG with the following content:

```python
# folder: dags/
# file name: tobiko_cloud_airflow_integration.py

from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

tobiko_cloud = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud")

first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")
```

This is all that's required to integrate with Tobiko Cloud!

Once this DAG is loaded by Airflow, it will be populated with your SQLMesh model structure for the specified `environment` and will be automatically configured to trigger at the same time as the next Cloud Scheduler run.

You will get an entry in the DAG list:
![dag_list](./airflow/dag_list.png)

Which you can browse like any other DAG:
![dag_view](./airflow/dag_view.png)

## How it works

When the Airflow DAG runs, it tracks the progress of the equivalent Tobiko Cloud scheduler run. The local tasks reflect the outcome of the remote tasks which allows you to observe at a glance how your data pipeline is progressing, in context with your other pipelines without having to switch to Tobiko Cloud.

This approach allows us to perform scheduling optimisations that are only possible within our SQLMesh-aware scheduler, while still allowing you to retain the flexibility to attach extra tasks or loop off extra logic within your usual orchestration environment.

Due to the fact that runs are still triggered by the Tobiko Cloud scheduler and the tasks in the local DAG just report on the progress of their remote equivalent in Tobiko Cloud, we call the integration a *facade*.

## Debugging

Each task in the DAG writes some log output containing links to the remote task in Tobiko Cloud:

![task_logs](./airflow/task_logs.png)

Following this link allows you to debug the task in context in our [Debugger View](../debugger_view.md):

![cloud_debugger](./airflow/cloud_debugger.png)

## Configuration options

We provide the ability to customise the generated DAG.

When creating the `SQLMeshEnterpriseAirflow` object, the following parameters are supported:

| Option    | Description                                                              | Type | Required |
|-----------|--------------------------------------------------------------------------|:----:|:--------:|
| `conn_id` | The Airflow connection ID containing the Tobiko Cloud connection details | str  | Y        |

When calling `create_cadence_dag()`, the following parameters are supported:

| Option               | Description                                                                            | Type | Required |
|----------------------|----------------------------------------------------------------------------------------|:----:|:--------:|
| `environment`        | Which SQLMesh environment to target. Default: `prod`                                   | str  | N        |
| `dag_kwargs`         | A dict of arguments to pass to the Airflow DAG object when it is created.              | dict | N        |
| `common_task_kwargs` | A dict of kwargs to pass to all task operators in the DAG                              | dict | N        |
| `sensor_task_kwargs` | A dict of kwargs to pass to just the sensor task operators in the DAG                  | dict | N        |
| `report_task_kwargs` | A dict of kwargs to pass to just the model / progress report task operators in the DAG | dict | N        |

For example, to create the cadence dag with specific tags:

```python
first_task, last_task, dag = tobiko_cloud.create_cadence_dag(
    environment="prod",
    dag_kwargs=dict(
        tags=["production", "data-engineering"]
    )
)
```

## Extending the DAG

The `create_cadence_dag()` function returns a tuple of references:

- `first_task` - a reference to the first task in the DAG, which is the `Sensor` task that synchronises with Tobiko Cloud before continuing
- `last_task` - a reference to the last task in the DAG, which is the `DummyOperator` that ensures all the models with no downstream dependencies have completed before declaring the DAG completed
- `dag` - a reference to the Airflow `DAG` object itself.

You can use these references to manipulate the DAG and attach extra behaviour.

For example, to trigger a Slack notification when a run completes:

```python
first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")

first_task >> SlackAPIPostOperator(task_id="notify_slack", channel="#notifications", ...)
```

![add_task_at_start](./airflow/add_task_at_start.png)

Or send an email and then trigger another DAG:

```python
last_task >> EmailOperator(task_id="notify_admin", to="admin@example.com", subject="SQLMesh run complete")
last_task >> TriggerDagRunOperator(task_id="trigger_job", trigger_dag_id="some_downstream_job")
```

![add_task_at_end](./airflow/add_task_at_end.png)

Or to trigger another DAG after a specific model has completed, without waiting for the entire DAG to complete:

```python
customers_task = dag.get_task("sushi.customers")
customers_task >> TriggerDagRunOperator(task_id="customers_updated", trigger_dag_id="some_other_pipeline", ...)
```

![add_task_after_specific_model](./airflow/add_task_after_specific_model.png)

!!!info

    - The Task `task_id`'s are named after the fully qualified model name. You can hover over a task in the Airflow DAG view to check its `task_id`.
    - The Task display names are the *table* portion of the model name. For example, a model called `foo.model_a` will show up as `model_a` in the Airflow DAG view, but the `task_id` will still be `foo.model_a`.
