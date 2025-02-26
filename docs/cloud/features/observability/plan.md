# Plans

Plan pages serve as comprehensive control centres that provide detailed insights into individual plans executed across your various environments. These pages act as a central hub 
where team members can monitor and understand all aspects of plan execution, from start to finish.

In SQLMesh, artifacts are stored locally by default, which means team members don't have immediate visibility into changes or their current status. To address this limitation, we've 
created this comprehensive page that serves two essential purposes:

1. It introduces a universal and accessible concept where every team member can easily view, track, and understand all changes and their current status, promoting transparency and 
collaboration across the team
2. It maintains detailed historical records for your entire team, serving as a reliable reference point to track what changes were implemented and how your project has evolved 
over time, ensuring nothing gets lost or forgotten

## When you might use this:

**Team Collaboration**

Enables team collaboration through an easy-to-understand view of all changes, so everyone can see the latest updates made to the environment, ensuring every team member has access to
 up-to-date information about specific changes made to the environment. 

**Monitoring** 

This enables monitoring of plan execution status through intuitive indicators that clearly show whether plans are currently running, have completed successfully, or have encountered 
any issues that need attention

If you do encounter any issues, this page serves as an ideal starting point for debugging: 

- You no longer need to spend time searching through extensive log files trying to locate specific model changes or modifications you've made - everything is organized and easily 
accessible
- We've streamlined the process by creating a carefully curated log that captures everything that occurred during the plan execution. This allows you to efficiently examine any 
particular plan or model and access all relevant logs in one consolidated location, eliminating the need to parse through lengthy CLI output to find what you're specifically 
interested in
    1. For storage optimization, logs are retained for a one-week period before being automatically cleaned up from the system
- instead of terminal output alone, easy at a glance outstanding share with teammates

**Clarification** 

Delivers an extensive and easily digestible visualization of model changes, making it simple to understand and share complex modifications with team members who might not have 
direct access to the local development environment. The intuitive navigation system at the top of the page provides valuable context and assistance in understanding what might have 
gone wrong, making troubleshooting more efficient and systematic

- For example, share changes with teammates without opening a pull request (which could trigger an unwanted CI/CD pipeline)

## Navigating to a Plan page
Every SQLMesh `plan` is applied to a specific environment. To locate a `plan`, first navigate to its [Environment page](./development_environment.md).

The environment page's Recent Activity table includes a list of every recent `plan` and `run`. To learn more about a `plan`, locate the `plan` by application date and click on its 
blue ID link in the table's final column.

![tcloud plan information](./plan/plan_info.png)

Clicking the link opens the detailed plan overview page:

![tcloud plan](./plan/plan.png)

The top section provides an at-a-glance overview of the plan, including:
![tcloud plan](./plan/plan_top_section.png)

- `Status`: the plan's completion status (possible values: complete, in progress, failed)
- `When`: the times when the plan started and completed
- `Plan Type`: the plan's type classification. Possible values:
    - `Environment update`: the plan includes a modified model
    - `Restatement`: the plan included a restated model
    - `System`: the Tobiko Cloud team has made a upgrade to your system (no models or data were affected)
- `Backfill Dates`: dates for which the model was backfilled
- `Changes`: chart displaying counts of model change types (directly modified model count in blue, added models in green, removed models in red)

## Plan changes section

The middle section presents a detailed summary of all plan changes.
![plan example](./plan/plan_changes.png)

Each change category has its own vertical tab: `added` models, `directly modified` models, `metadata-only modified` models, `indirectly modified` models and `removed` models .

If you click on a model in this seciton it will bring you to the [individual model page](./model.md).


## Updates and Executions section

The final section provides horizantal tabs listing all the updates and executions you may want to view, `Physical Layer Updates`, `Model Executions`, `Audits` and `Virtual Updates`.
![tcloud plan audits section](./plan/plan_tabs.png)

Each tab is a table with detailed information on the model(s) that have been updated. The number of models for each category are shown to the right of the tab as the number. 

The Physical Layer Updates table contains summary information for models that have undergone a change in the physical layer and has a link to each model execution's page. 

The Model Executions table contains summary information and a link to each model execution's page, which contains detailed information and exeuction logs.

The Audits table contains a complete chronological listing of all audits that have been performed.

The Virtual Updates table contains a list of all models that were virtually updated by the plan.
