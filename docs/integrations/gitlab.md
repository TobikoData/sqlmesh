# GitLab Integration

SQLMesh can be integrated with GitLab to provide a seamless CI/CD experience for your data projects. This integration allows you to run SQLMesh commands directly from your GitLab merge requests, and to get feedback on the status of your data models.

## Setup

To set up the GitLab integration, you need to do the following:

1.  **Create a GitLab private token.** You can create a private token in your GitLab user settings. The token needs to have the `api` scope.
2.  **Add the private token to your GitLab CI/CD variables.** Add the token as a masked variable named `GITLAB_PRIVATE_TOKEN` in your project's CI/CD settings.
3.  **Configure the SQLMesh bot.** You need to add a `cicd_bot` section to your `config.yml` file. The following is an example configuration:

```yaml
cicd_bot:
  type: gitlab
  project_id: <your-project-id>
  enable_deploy_command: true
```

## Usage

Once the GitLab integration is set up, you can use the following commands in your merge request comments:

-   `/deploy`: Deploys the changes in the merge request to the production environment.

## CI/CD Pipeline

The following is an example of a GitLab CI/CD pipeline that uses the SQLMesh GitLab integration:

```yaml
stages:
  - test
  - deploy

sqlmesh_checks:
  stage: test
  script:
    - sqlmesh gitlab run-all-checks --token $GITLAB_PRIVATE_TOKEN

sqlmesh_deploy:
  stage: deploy
  script:
    - sqlmesh gitlab run-deploy-command --token $GITLAB_PRIVATE_TOKEN
  rules:
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event" && $CI_MERGE_REQUEST_EVENT_TYPE == "note"'
```
