# Models

From the main environments list, you can access individual models to explore their comprehensive observability features and detailed summary information. This centralized view provides quick access to critical model metrics and performance data.

Click the environment you want to explore from the environments list

![Tobiko Cloud environment page](./model/tcloud_environments.png)

Navigate to the Models section and click "Explore" to view available models

![Tobiko Cloud environment page explore models link](./model/tcloud_environment_explore-models.png)

Browse through the model list and select your desired model to access its detailed information

![Tobiko Cloud environment models list](./model/tcloud_model_list.png)

Each model presents a comprehensive summary overview that includes several key components and metrics for monitoring and analysis.

The following detailed information outlines the different sections:

![Tobiko Cloud model status and metadata](./model/tcloud_model_status-metadata.png)

- Current status: Provides visual representations of model health through freshness indicators and detailed daily execution graphs

- Model details: Features comprehensive tabs that display summary statistics, complete source code documentation, and interactive model lineage visualizations

![Tobiko Cloud model version history](./model/tcloud_model_2.png)

- Version history: Delivers a comprehensive chronological view of all model versions, with detailed information including:
    - Precise timestamp of version promotion
    - Clear indication of change impact (breaking or non-breaking modifications)
    - Direct access to the complete implementation plan code
- Loaded intervals: these periods represent the time spans between consecutive cron job executions, from the start of one cron job to the end of the next cron job. These intervals are crucial for understanding the boundaries of data processing cycles
    - the table displays which specific model version was responsible for generating and processing data during each distinct cron interval, enabling precise tracking of version-specific outputs
    - helps track forward-only model changes by maintaining a clear chronological record of modifications, ensuring data consistency and preventing retroactive alterations
    - provides comprehensive visibility into completed data processing operations, allowing users to monitor progress and verify successful execution of scheduled tasks
- Recent activity: Maintains a detailed log of version executions and comprehensive version audits