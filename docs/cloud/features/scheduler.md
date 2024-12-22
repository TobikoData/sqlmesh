# Scheduler
Tobiko Cloud utilises a robust internal scheduler to apply schema changes to your data warehouse and to ensure that data always remains up to date.

This allows us to do things like:

 - Merge concurrent operations triggered asynchronously by different users into single operations
 - Ensure safe concurrent execution of tasks with maximum parallelism
 - Vastly improve the debugging experience by having deep insight into the execution workflow

The Tobiko Cloud scheduler is an integral part of Tobiko Cloud. However, if you're already using a tool like Airflow or Dagster - we support integrations between Tobiko Cloud and third party schedulers so that your data pipelines can be monitored and viewed in-context with the rest of your pipelines.

# Third-party scheduler integrations

For supported third-party schedulers, we provide integrations that use the Tobiko Cloud API to build a platform-native DAG.

## How it works
When the platform native DAG runs, it tracks the progress of the equivalent Tobiko Cloud scheduler run. The local tasks reflect the outcome of the remote tasks which allows you to observe at a glance how your data pipeline is progressing, in context with your other pipelines without having to switch to Tobiko Cloud. 

This approach allows us to perform scheduling optimisations that are only possible within our SQLMesh-aware scheduler, while still allowing you to retain the flexibility to attach extra tasks or loop off extra logic within your usual orchestration environment.

Due to the fact that runs are still triggered by the Tobiko Cloud scheduler and the tasks in the local DAG just report on the progress of their remote equivalent in Tobiko Cloud, we call the integration a *facade*.