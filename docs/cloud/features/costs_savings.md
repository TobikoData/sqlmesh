# Data Warehouse Costs and Savings with Tobiko

Tobiko Cloud tracks data warehouse cost estimates per model for BigQuery and Snowflake projects. It will also estimate cost savings achieved by SQLMesh intelligently detecting when a model doesn’t need to be rerun.

If you'd like to see cost and savings estimates in Cloud and use a supported pricing plan, Tobiko can estimate your data warehouse costs for you and display them in various places on Tobiko Cloud.

## Supported Data Warehouse Pricing Plans

- BigQuery On Demand
- Snowflake Credits

## Data Warehouse Cost Configuration

If you use one of the supported pricing plans, in order to configure Tobiko Cloud to show cost information you will need to visit Settings.

![Image highlighting location of the Settings link in the left site navigation](./costs_savings/costs-navigation.png)

On the General settings page (1), select your pricing plan (2), enter your costs, and then save (3).

![Annotated image showing locations of the general settings link, pricing plan form fields, and save button](./costs_savings/costs-steps.png)

## Where to find cost and savings estimates information

Estimated costs and savings are displayed on the homepage, production environment page, runs and plans pages, and individual model pages.

Cost information on each page will look similar to the following:

![Example of costs and savings data as seen on the Tobiko Cloud homepage](./costs_savings/costs-example.png)

### Savings Categories

When calculating your data warehouse costs, we will calculate the amount you saved by using Tobiko as well! Tobiko Cloud also comes with additional change categorization capabilities, such as advanced column-level impact analysis.

Cost savings are broken up into three main categories:

- **Prevented Reruns**: If SQLMesh has already run an execution for a change in one environment, we won't need to rerun it in another environment (such as when a model change is backfilled on a development environment and then applied to prod)
- **Unaffected Downstream**: Because SQLMesh understands SQL, we know if a downstream model is not affected by an upstream change and can avoid re-execution.
- **Virtual Environments**: Using the Virtual data environments feature means new environments can be created without backfilling models within the new environment.

## Where to find cost savings information

Most of the places where costs are displayed, cost savings will be shown as well. You can find how much you've saved using Tobiko by viewing the homepage, production environment page, and individual model pages.
