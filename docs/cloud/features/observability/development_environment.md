# Development Environment

The Tobiko Cloud Environments page provides an overview of all the environments that exist in your project.

![tcloud environment page](./development_environment/environments.png)

The page's table includes a link to each environment's page, along with the environment's creation date, the date it was last updated, and the date it will expire if not updated again.

## Environment page

Clicking an environment's name in the table takes you to its environment page.

The page begins with a at-a-glance summary of the most recent plan applied to the environment, including:

- Its completion status and time
- The latest time interval backfilled by the plan
- Count of models present in the environment
- An interactive visualization that summarizes the differences between the environment's models and the `prod` environment's models
    - The count of directly modified models is represented in blue
    - The count of added models is green
    - The count of removed models is red

![tcloud development environment](./development_environment/tcloud_development_environment.png)

## Differences from Prod section

Development environments are used to prepare and test changes before merging them to `prod`.

The `Differences From prod` section provides a summary of model differences between the environment and `prod`, with separate tabs for directly and indirectly modified models.

In the summary, each model's name is a link to [its model page](./model.md).

## Plan history information

The plan applications chart is a calendar visualization of all plans that have been applied to the environment in the previous 2 weeks.

The chart represents time across days on its `x-axis`, where each column represents one day. The date corresponding to each day is displayed at the top of the chart.

The chart represents time within a day on its `y-axis`, where each day begins at the top and ends at the bottom.

Each day displays zero or more vertical bars representing `plan` duration. If no `plan`s occurred on a day, no vertical bars will be displayed. If multiple `plan`s occurred on the same day, their vertical bars will be stacked.

The chart uses color to convey the changes made by a `plan` at a glance. Models added by a plan are green, removed models are red, directly modified models are blue, indirectly modified models are orange, and metadata changes are grey.

Hovering over a bar reveals summary information about the `plan`, including its completion status, start time, end time, total duration, and change summary. The summary includes a link to [the `plan`'s page](./plan.md).