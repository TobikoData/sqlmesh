# GitHub

Our Github integration will allow you to add a CI/CD bot to any Github project using [Github Actions](https://github.com/features/actions). 
The bot will automatically run [plan/apply](../concepts/plans.md) to an [environment](../concepts/environments.md) for the pull request that will exactly match the code in the pull request.
This will be done without copying or rebuilding data using the [Virtual Data Mart technology](../concepts/glossary.md#virtual-data-marts). 
Once approved, the CI/CD bot will automatically run [plan/apply](../concepts/plans.md) to the production environment and merge the PR upon completion.
This allows you to have your main branch and prod environments always in sync.

We will be launching this CI/CD bot soon and please leave any feedback or question on [our slack channel](https://join.slack.com/t/tobiko-data/shared_invite/zt-1ma66d79v-a4dbf4DUpLAQJ8ptQrJygg) until then!