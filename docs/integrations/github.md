# GitHub Actions

SQLMesh's Github Actions integration will allow you to add a SQLMesh CI/CD bot to any Github project using [Github Actions](https://github.com/features/actions). The bot will automatically run [plan/apply](../concepts/plans.md) to an [environment](../concepts/environments.md) based on the code in a pull request.

This will be done without copying or rebuilding data using SQLMesh's [Virtual Data Mart technology](../concepts/glossary.md#virtual-data-marts). 
Once approved, the CI/CD bot will automatically run [plan/apply](../concepts/plans.md) to the production environment and merge the PR upon completion.
This allows you to always have your main branch and prod environments in sync.

We will be launching this CI/CD bot soon &mdash; in the meantime, please leave any feedback or questions in [our Slack channel](https://join.slack.com/t/tobiko-data/shared_invite/zt-1ma66d79v-a4dbf4DUpLAQJ8ptQrJygg)!