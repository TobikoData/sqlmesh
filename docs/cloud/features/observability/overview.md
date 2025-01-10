# Overview

Fixing problems with data pipelines is challenging because there are so many potential causes.

For transformation pipelines, those range from upstream source timeouts to SQL query errors to Python library conflicts (and more!).

A useful observation tool should make it easy to answer the following questions:

- Did a problem occur?
- When did it occur?
- What type of problem is it?
- Where is the problem coming from?
- What is causing the problem?

Tobiko Cloud supports answering these questions in four ways:

1. Automatically notifying users if a problem occurs
2. Capturing, storing, and displaying historical information that reveals when a problem occurred
3. Enabling easy navigation from aggregated to granular information about pipeline components to identify the problem source
4. Centralizing error information from multiple sources to debug the problem

## How it works

Tobiko Cloud captures detailed metadata throughout your data project's lifecycle.

During the execution of plans and runs, it collects information about model performance and system health to give you complete visibility into your system's operations.

This information allows you to:

- Monitor the health and performance of your data pipelines
- Track the status of current and historical runs
- Review a detailed version history of your models and transformations
- Creation of custom visualizations and metrics
- Troubleshoot problems and optimize inefficient operations

Observability features are seamlessly integrated into Tobiko Cloud, making it simple to monitor and understand your project's behavior.

Instead of digging through complex logs or piecing together information from multiple sources, you can quickly access the relevant information from any part of your project.

Learn more about Tobiko Cloud Observability features on these pages:

- [`prod` environment](prod_environment.md) health and recent activity
- [Development environment](development_environment.md) differences from `prod` and recent activity
- [Plan](plan.md) status and detailed model execution data
- [Run](run.md) status and detailed model execution data
- [Model](model.md) status and version history
- [Dashboards](measures_dashboards.md) and custom visualizations of observability data