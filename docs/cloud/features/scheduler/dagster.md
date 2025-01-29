# Dagster

Tobiko Cloud's Dagster integration allows you to combine Dagster system monitoring with the powerful debugging tools in Tobiko Cloud.

## Setup

Your SQLMesh project must be configured and connected to Tobiko Cloud before using the Dagster integration.

Learn more about connecting to Tobiko Cloud in the [Getting Started page](../../tcloud_getting_started.md).

### Install libraries

After connecting your project to Tobiko Cloud, you're ready to set up the Dagster integration.

Start by adding the `tobiko-cloud-scheduler-facade` library to your [Dagster project](https://docs.dagster.io/guides/understanding-dagster-project-files).

To do this, add it to the `install_requires` section of `setup.py`:

```python title="setup.py" hl_lines="3"
install_requires=[
    "dagster",
    "tobiko-cloud-scheduler-facade[dagster]"
],
```

### Connect Dagster to Tobiko Cloud

Dagster recommends [injecting secret values using Environment Variables](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets#using-environment-variables-and-secrets). The exact method is specific to your Dagster deployment.

Here, we will describe the method for **local development**.

In your Dagster project, add the following entries to the `.env` file (creating it if it doesnt exist):

```sh title=".env"
TOBIKO_CLOUD_BASE_URL=<URL for your Tobiko Cloud project>
TOBIKO_CLOUD_TOKEN=<your Tobiko Cloud API token>
```

The base URL and password values will be provided to you during your Tobiko Cloud onboarding.

### Create Dagster objects

You are now ready to create Dagster objects connected to Tobiko Cloud.

This example code demonstrates the creation process, which requires:

- Importing the `SQLMeshEnterpriseDagster` class
- Creating a `SQLMeshEnterpriseDagster` instance configured with the environment variables you declared above
- Creating the `Definitions` object with the `create_definitions()` method

In the `definitions.py` file in your project, insert the following:

```python title="definitions.py" linenums="1"
from dagster import EnvVar
from tobikodata.scheduler_facades.dagster import SQLMeshEnterpriseDagster

sqlmesh = SQLMeshEnterpriseDagster(
    url=EnvVar("TOBIKO_CLOUD_BASE_URL").get_value(),
    token=EnvVar("TOBIKO_CLOUD_TOKEN").get_value(),
)

tobiko_cloud_definitions = sqlmesh.create_definitions(environment="prod")
```

!!! info
    If there is an existing definitions object already declared in your Dagster project, you can merge in the Tobiko Cloud definitions like so:

    ```python
    defs = Definitions(...)

    defs.merge(tobiko_cloud_definitions)
    ```

This is all that's needed to integrate with Tobiko Cloud!

Once Dagster loads your project, you will see some new objects become available.

## Available Dagster objects

The Dagster integration exports the following:

- An `Asset` object for every SQLMesh Model
  ![Dagster UI Asset Lineage](./dagster/asset_lineage.png)
- An `AssetCheck` object attached to the relevant `Asset`'s for every SQLMesh Audit
  ![Dagster UI Asset Checks](./dagster/asset_check_list.png)
- Two `Jobs`:
    - A `sync` job to synchronise the current state of all Assets and Asset Checks from Tobiko Cloud to Dagster
    - A `mirror` job that tracks a Cloud Scheduler run and mirrors the results to Dagster
      ![Dagster UI Jobs List](./dagster/job_list.png)
- A `Sensor` to monitor Tobiko Cloud for new cadence runs and trigger the `mirror` job when one is detected
  ![Dagster UI Sensor List](./dagster/sensor_list.png)

Once your Definitions are loaded by Dagster, these will become available in the Dagster UI.

## Monitor Tobiko Cloud actions

To get started, enable the Sensor if it is not already enabled:

![Enable the track sensor](./dagster/enable_sensor.png)

The Sensor is configured to run every 30 seconds. It will do the following:

- On the first run, it will trigger the `sync` job. This synchronises the materialization status of all Models and Audits from Tobiko Cloud to their corresponding Assets / Asset Checks in Dagster
- On subsequent runs, if there is a new Cloud Scheduler run, it will trigger the `mirror` job to mirror the outcome of that run locally.

![Job run records in the Dagster UI](./dagster/job_run_records.png)

!!! info "Why are there two jobs?"
    The Tobiko Cloud scheduler is optimised to prevent doing unnecessary work. In any given cadence run, it's possible only a subset of models are updated.
    Therefore, there would be no materialization information for the remaining models. There is also no materialization information for objects that are never part of a cadence run, such as [seeds](../../../concepts/models/seed_models.md).

    The `sync` jobs exists to copy across the current state of every object in the project as a base for the `mirror` job to build on.

If you need to manually refresh materialization information for all models, you can run the `sync` job manually from the Dagster UI:
![Manually run the sync job](./dagster/manual_sync_run.png)

## How it works

Tobiko Cloud uses a custom approach to Dagster integration - this section describes how it works.

The Mirror job mirrors the progress of the Tobiko Cloud scheduler run. Each local task reflects the outcome of its corresponding remote task. If an asset is materialized remotely, the job emits a Dagster materialization event.

This allows you to observe at a glance how your data pipeline is progressing, displayed alongside your other pipelines in Dagster. No need to navigate to Tobiko Cloud!

### Why a custom approach?

Tobiko Cloud's scheduler performs multiple optimizations to ensure that your pipelines run correctly and efficiently. Those optimizations are only possible within our SQLMesh-aware scheduler.

Our approach allows you to benefit from those optimizations while retaining the flexibility to attach extra tasks or logic to the Assets created by Tobiko Cloud in your broader pipeline orchestration context.

Because `run`s are still triggered by the Tobiko Cloud scheduler and tasks in the local DAG just reflect their remote equivalent in Tobiko Cloud, we call our custom approach a *facade*.

## Debugging

Each task in the mirror job writes logs that include a link to its corresponding remote task in Tobiko Cloud.

In the Dagster UI, find these logs on the job's Logs page:

![Dagster Job Logs](./dagster/job_logs.png)

Or alternatively, in the Asset Catalog, we write a link to the logs of the last evaluation as Metadata:

![Dagster Asset Metadata](./dagster/asset_latest_materialization_metadata.png)

Clicking the link opens the remote task in the Tobiko Cloud [Debugger View](../debugger_view.md), which provides information and tools to aid debugging:

![Tobiko Cloud UI debugger view](./airflow/cloud_debugger.png)

## Picking up new Models

By default, Dagster will not reload the Definitions defined [above](#create-dagster-objects) automatically.

This means that if you run a plan that adds or removes models from your Tobiko Cloud project, they will not show in Dagster by default.

### Automatic method

Dagster runs user code in an isolated sandbox. This means that the user code cannot easily communicate with the Dagster Webserver to trigger a reload of the Asset definitions.

However, it is possible to connect to the Dagster GraphQL API from within user code to trigger the reload of the Code Location containing the Tobiko Cloud asset definitions.

This is enabled by default if you specify the following extra parameters when instantiating `SQLMeshEnterpriseDagster`:

```python title="definitions.py" linenums="4-5"
sqlmesh = SQLMeshEnterpriseDagster(
    url=EnvVar("TOBIKO_CLOUD_BASE_URL").get_value(),
    token=EnvVar("TOBIKO_CLOUD_TOKEN").get_value(),
    dagster_graphql_host="localhost",
    dagster_graphql_port=3000
)
```

The `dagster_graphql_host` and `dagster_graphql_port` are deployment-specific. This should be the hostname/port that the Dagster GraphQL endpoint is available in.

In the case of local development when Dagster is started via `dagster dev`, this is typically `localhost:3000`.

When the Dagster GraphQL server location is known, the Sensor that triggers the `mirror` job will first issue a GraphQL request to reload the Code Location. This will pick up any new Asset and Asset Check definitions.

Once the Code Location is reloaded, the `mirror` job executes as usual with the updated definitions.

### Manual method

At any time, you can click the "Reload" button against the Code Location in Dagster. This will pull through any new Asset and Asset Check definitions from Tobiko Cloud.

![Dagster reload code location](./dagster/reload_code_location.png)

## Attaching custom logic

Dagster includes a robust events system. You can listen for events on any of the Assets or Jobs exposed by our Dagster integration and respond to them by running your own custom logic.

To listen for materialization events on Assets, you can use an [Asset Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/asset-sensors).

To listen for job runs, you can use a [Run Status Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#run-status-sensors).

Dagster also provides a framework called [Declarative Automation](https://docs.dagster.io/concepts/automation/declarative-automation) that builds on top of these.

### Examples

Here are some examples of running custom logic in response to Tobiko Cloud events.

Note that Dagster has a lot of flexibility in how it can be configured and the methods we describe below arent necessarily the right choice for every configuration. We recommend familiarizing yourself with Dagster's [Automation](https://docs.dagster.io/concepts/automation) features to get the most out of your Tobiko Cloud deployment with Dagster.

#### Response to run status

To listen for Tobiko Cloud run events, you can create a [Run Status Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#run-status-sensors) that listens for events on the `mirror` job and triggers your custom job in response.

Within your custom job, you can do anything you like as you have full access to Python and any libraries installed in your Dagster environment.

To define the custom job, you can decorate a function with `@op` to implement the task logic and a `@job` to group it into a Job:

```python
@op
def send_email():
    import smtplib

    with smtplib.SMTP("smtp.yourdomain.com") as server:
        server.sendmail(...)

@job
def send_email_job():
    send_email()
```

You can also fetch a reference to the Tobiko Cloud mirror job from the Definitions we created above like so:

```python
mirror_job = tobiko_cloud_definitions.get_job_def("tobiko_cloud_mirror_run_prod")
```

With this, you can create a `@run_status_sensor` that listens to the mirror job and triggers your job when the mirror job ends:

```python
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[mirror_job],
    request_job=send_email_job
)
def on_tobiko_cloud_start_run(context: RunStatusSensorContext):
    return RunRequest()
```

You can adjust the `run_status` argument to listen for different statuses, the `monitored_jobs` argument to monitor other jobs and the `request_job` argument to trigger a different job.

Here's an example that triggers a Slack notification:

```python
from dagster import run_status_sensor, job, DagsterRunStatus, EnvVar, RunRequest, RunStatusSensorContext
from dagster_slack import SlackResource

mirror_job = tobiko_cloud_definitions.get_job_def("tobiko_cloud_mirror_run_prod")

@op
def slack_op(slack: SlackResource):
    #note: see the dagster-slack docs here: https://docs.dagster.io/_apidocs/libraries/dagster-slack
    slack.get_client().chat_postMessage(channel="#notifications", ...)

@job
def notify_slack():
    slack_op()

@run_status_sensor(
    run_status=DagsterRunStatus.STARTED,
    monitored_jobs=[mirror_job],
    request_job=notify_slack
)
def on_tobiko_cloud_start_run(context: RunStatusSensorContext):
    return RunRequest()
```

!!! info

    If your Tobiko Cloud definitions live in their own code location, you can use a [JobSelector](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors#cross-code-location-run-status-sensors) to target the Mirror job instead of the direct reference.


#### Response to Asset Materialization

When Tobiko Cloud refreshes or adds new data to a model, a Materialization event occurs against the corresponding Asset in Dagster.

This gives you a hook to run custom logic as well. As before, you can do anything you want here, whether that is triggering the materialization of another Asset fully managed by Dagster or running some custom task.

To listen for Asset Materialization events, you can create an [Asset Sensor](https://docs.dagster.io/concepts/partitions-schedules-sensors/asset-sensors).

For example, let's say your Tobiko Cloud project has a model called "postgres.crm.customers" and it's showing in the Asset Catalog under "postgres / crm / customers".

You can listen for materialization events like so:

```python
from dagster import AssetKey, SensorEvaluationContext, EventLogEntry

@job
def my_job():
    # custom job logic

@asset_sensor(
    asset_key=AssetKey(["postgres", "crm", "customers"]),
    job=my_job
)
def my_asset_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    yield RunRequest()
```

The sensor will trigger every time the Asset with the key `postgres / crm / customers` is materialized.

To identify the `AssetKey`'s of your Assets, you can check the Asset Catalog. Each part of the path is a segment of the Asset Key.

![Dagster asset keys](./dagster/asset_keys.png)

In the above screenshot, the `AssetKey`'s can be constructed like so:

```python
from dagster import AssetKey

active_customers = AssetKey(["postgres", "sushi", "active_customers"])
customer_revenue_by_day = AssetKey(["postgres", "sushi", "customer_revenue_by_day"])
```

## Configuration

### `SQLMeshEnterpriseDagster` parameters

| Option                     | Description                                                                                                                                                                            | Type | Required |
|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----:|:--------:|
| `url`                      | The Base URL to your Tobiko Cloud instance                                                                                                                                             | str  | Y        |
| `token`                    | Your Tobiko Cloud API Token                                                                                                                                                            | str  | N        |
| `dagster_graphql_host`     | Hostname of the Dagster Webserver GraphQL endpoint                                                                                                                                     | str  | N        |
| `dagster_graphql_port`     | Port of the Dagster Webserver GraphQL endpoint                                                                                                                                         | int  | N        |
| `dagster_graphql_kwargs`   | Extra args to pass to the [DagsterGraphQLClient](https://docs.dagster.io/api/python-api/libraries/dagster-graphql#dagster_graphql.DagsterGraphQLClient) class when it is instantiated  | dict | N        |

### `create_definitions()` parameters

| Option                     | Description                                                                            | Type | Required |
|----------------------------|----------------------------------------------------------------------------------------|:----:|:--------:|
| `environment`              | Which SQLMesh environment to target. Default: `prod`                                   | str  | N        |
| `asset_prefix`             | Top-level category to nest Tobiko Cloud assets under                                   | str  | N        |
| `enable_sensor_by_default` | Whether the Sensor that polls for new runs should be enabled by default. Default: True | bool | N        |
