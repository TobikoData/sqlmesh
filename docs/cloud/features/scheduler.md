# Scheduler
Tobiko Cloud utilises a robust internal scheduler to apply schema changes to your data warehouse and to ensure that data always remains up to date.

This allows us to do things like:
 - Merge concurrent operations triggered by independent users into single operations, increasing efficiency
 - Ensure the depenency graph is respected with maximum parallelism
 - ???
 - Profit!


# External schedulers
The internal scheduler is great, but what if you're already using an existing orchestration tool like Airflow or Dagster to coordinate your data pipelines? If Tobiko Cloud operates independently, how can you trigger other business processes when certain models are evaluated? Or even just position your data pipeline next to all your other pipelines so you can see the results in context?

To address these issues, we have built integrations that allow the Tobiko Cloud scheduler to be exposed in other scheduling systems. We expose your SQLMesh models as a DAG native to the target platform which allows you to loop extra logic off the tasks.

However, the main difference is that the models are still executed by Tobiko Cloud. The DAG running in your local scheduling platform just consists of simple tasks that use the Tobiko Cloud API to poll the status of the equivalent task in Tobiko Cloud.

This is why we call the integration a *facade*.

## Airflow
To integrate with Airflow, add the following library to your Airflow runtime environment:

```
$ pip install tobiko-cloud-scheduler-facade[airflow]
```

You also need to add an Airflow [connection]() containing your

```
id: tobiko_cloud
type: HTTP
host: https://tobiko/cloud/base/url
password: <tobiko cloud API token>
```

Note that you can name it whatever you want, you just need to remember the name so you can supply it as for `conn_id` below.

After setting up the connection, you can then create a DAG with the following content:
```
from tobikodata.scheduler_facades.airflow import SQLMeshEnterpriseAirflow

tobiko_cloud = SQLMeshEnterpriseAirflow(conn_id="tobiko_cloud")

first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")
```

This is all that's required to integrate with Tobiko Cloud. Once this DAG is loaded in Airflow, it will be populated with your SQLMesh model structure for the specified `environment` and will be automatically configured to trigger at the same time as the next Cloud Scheduler run.

When the Airflow DAG runs, it will do the following:
    - Block until it picks up a Cloud Scheduler run that it hasnt already reported on
        - Once it find one, a link to the Tobiko Cloud UI is printed to the task logs so you can quickly jump to the overview for this run
    - Run through each corresponding task in the Cloud Scheduler run, blocking until each task has completed remotely
        - A Tobiko Cloud UI link to the task logs is written to the Airflow logs so you can follow that to debug any failures in context
        - The state of the Airflow task will match the state of the remote task that it's tracking (eg running, success, skipped, failed)


### Configuration options


### Extending the DAG

Each task in the Airflow DAG is named after the model it's tracking. In addition, the `create_cadence_dag()` function returns a tuple of references to the `first_task`, `last_task` and the `dag` itself.

You can use these references to manipulate the DAG and attach extra behaviour.

For example:
```
first_task, last_task, dag = tobiko_cloud.create_cadence_dag(environment="prod")

first_task >> SlackAPIPostOperator(channel="#notifications", ...)

last_task >> EmailOperator(to="admin@example.com", subject="SQLMesh run complete")
last_task >> TriggerDagRunOperator(trigger_dag_id="some_downstream_job")

model_a_task = dag.find_task("foo.model_a")
model_a_task >> TriggerDagRunOperator(trigger_dag_id="some_other_pipeline", ...)
```

You can also register asset information:

```

```


## Dagster

Coming soon!
