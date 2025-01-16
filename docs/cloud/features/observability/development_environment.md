# Development Environment

The transition from CLI to Tobiko Cloud represents a significant advancement in workflow management for the development environment page view. You will benefit from an enhanced experience that seamlessly moves you from command-line operations to a cloud-based interface. This transition enables smooth integration of SQLMesh capabilities into the UI-driven environment, fundamentally improving how you interact with and manage your dev processes.

The new UI-driven approach enhances how you interact with the system. Instead of relying solely on command-line inputs like `tcloud sqlmesh plan dev`, you will now have clear visual feedback through an intuitive interface. This visual representation makes it significantly easier to understand and interpret changes to your environments, ***as well as teammate’s environments***.

At its core, the development environment observability features provides a comprehensive overview of all development environments. This integrated approach creates a single source of truth serves both your individual developer needs and team-oriented workflows, ensuring a seamless and productive experience.

### When you might use this

**Team Collaboration**

The platform excels in fostering team collaboration by providing clear visibility into team activities. Developers can easily see who is working on specific models, prevent workflow conflicts, and avoid duplicate efforts. This creates a truly collaborative development environment where team members can work together seamlessly, supporting a dynamic, multiplayer development experience.

**Performance Tracking**:

Performance monitoring is enhanced through robust historical tracking capabilities. Teams can monitor changes over time, review recent activities including successes and failures, and gain detailed insights into specific plan execution outcomes. This historical context proves invaluable for understanding system behaviour and identifying trends.

**Simplified Communication and Team Alignment**:

The development environment streamlines team communication through shareable URLs, eliminating the need for manual methods like PRs or DMs. These URLs serve as comprehensive summaries, displaying crucial information including last run times, data intervals for incremental models, and detailed change information such as metadata modifications and model removals.

## Using the page
The Environments page shows an overview of all the environments that exist in your project (both yours and any your teammates have created).

![tcloud environment page](./development_environment/environments.png)

The page's table includes a link to each environment's page, along with the environment's creation date, the date it was last updated, and the date it will expire if not updated again.

### Selecting and Individual Environment page

Clicking an environment's name from the main environments page takes you to its individual page.

The page begins with an at-a-glance summary of the most recent plan applied to the environment, including:

- Its completion status and time
- The latest time interval backfilled by the plan
- Count of models present in the environment
- An interactive visualization that summarizes the differences between the environment's models and the `prod` environment's models
    - The count of directly modified models is represented in blue
    - The count of added models is green
    - The count of removed models is red

![tcloud development environment](./development_environment/tcloud_development_environment.png)

??? "ProTip:"

    If a stakeholder or else anyone on your team is looking to understand an environment you own and are working on, you can share the URl link with them and they will be abel to access and see all of the information about your environment. 
    
    Its a great place to start to have open conversations about what was recently added, removed or changed in a environment! 


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