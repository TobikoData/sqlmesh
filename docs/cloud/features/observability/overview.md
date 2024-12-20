######### Is anything before Marisa's draft section (titled "how it works" below) worth keeping?

# Overview

Data transformation systems cost time and money. Companies should understand the value they get in return, but that's surprisingly difficult to determine.

Why? Data transformation systems can be complex, and collecting the information you need to understand them is even more complex.

Tobiko Cloud solves this problem by automatically collecting, integrating, and displaying the information you need to understand your data transformation system.

## Observability in Tobiko Cloud

Tobiko Cloud's Observability features make it easy to understand your data transformation system.

They are deeply integrated with SQLMesh, collecting and collating the information you need to understand the system's usage and performance.

Beyond understanding what a system does, however, Tobiko Cloud provides the information you need to rapidly detect, understand, and remedy problems with your pipelines.

## Debugging with Tobiko Cloud

Remediating problems with data pipelines is challenging because there are so many potential causes.

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

####### DEFINITELY KEEP BELOW

## How it works

Tobiko Cloud captures detailed metadata throughout your data project's lifecycle.

During the execution of plans and runs, it collects information about model performance and system health to give you complete visibility into your system's operations.

This information allows you to:

- Monitor the health and performance of your data pipelines
- Track the status of current and historical runs
- Review a detailed version history of your models and transformations
- Creation of custom visualizations and metrics
- Troubleshoot problems and optimize inefficient operations

Observability features are seamlessly integrated into the Tobiko Cloud interface, making it simple to monitor and understand your project's behavior.

Instead of digging through complex logs or piecing together information from multiple sources, you can quickly access the relevant information from any part of your project.

Learn more about specific Tobiko Cloud Observability features on these pages:

- [`prod` environment](prod_environment.md) health and recent activity
- [Development environment](development_environment.md) differences from `prod` and recent activity
- [Plan](plan.md) status and detailed model execution data
- [Run](run.md) status and detailed model execution data
- [Model](model.md) status and version history
- [Dashboards](measures_dashboards.md) and custom visualizations of observability data