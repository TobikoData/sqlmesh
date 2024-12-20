# Run

From the Recent Activity section of an environment overview (such as [production](prod_environment.md) or [development](development_environment.md) environments), you can access comprehensive status information and detailed metadata about individual runs. This is done by locating a run and clicking the distinctive blue ID hash within the table, which serves as a direct link to the Run Observability Overview page.
![tcloud run information](./run/run_info.png)

This opens the detailed run overview page:

![tcloud run](./run/tcloud_run.png)

- At the top of the overview page, you'll find convenient at-a-glance summaries that provide immediate visibility into critical information, including the run's current status (whether it has completed successfully, is currently in progress, or has encountered failures) and a timestamp indicating when the information was last refreshed or updated.
- The run's detailed temporal information is clearly displayed, encompassing the precise start time of execution, the completion time, and a calculated total duration that helps you understand the run's performance characteristics
- A comprehensive comparison showing all relevant modifications and updates that have occurred since the previous run execution

The lower portion of the page contains the powerful Execution Explorer/Debugger interface, which organizes detailed information into three distinct and purposeful tabs:

- Model Executions: Provides a detailed overview of all executed models, complete with current status indicators, comprehensive error messages when applicable, and convenient access to detailed execution logs for troubleshooting
![tcloud run model executions](./run/run_model_executions.png)

- Audits: Offers a complete view of all audit execution statuses, clearly indicates whether specific audits are configured as blocking operations, and provides straightforward access to detailed audit logs for verification
![tcloud run model executions](./run/run_audits.png)

- Explore Executions: Delivers an extensive and interactive view of all included models, featuring an intuitive lineage graph for understanding dependencies, alongside detailed sections covering overview information, impact analysis, model definitions, comprehensive schema intervals, and easily accessible associated logs.
![tcloud run model executions](./run/run_explore_executions.png)