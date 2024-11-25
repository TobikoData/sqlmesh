# Tobiko Cloud: Getting Started

Tobiko Cloud is a data platform that extends SQLMesh to make it easy to manage data at scale. We're here to make it easy to get started and feel confident that everything is working as expected. After you've completed the steps below, you'll have achieved the following:

- Login to Tobiko Cloud via the browser
- Connect Tobiko Cloud to your local machine via the CLI
- Connect Tobiko Cloud to your data warehouse
- Verify Tobiko Cloud interacts with your data warehouse as expected

## Prerequisites

Onboarding requires the below steps from the Tobiko team to be completed before Tobiko Cloud can be used.
  
The Solutions Architect will:

- Set up a 1 hour meeting with you to fully onboard
- Request a new Tobiko Cloud account will be created for you (single tenant by default)
- Share a temporary password link that expires in 7 days
- Make sure you save the password in your own password manager

You will need to:

- Ensure you have data warehouse admin rights to update permissions and create new users with create/update/delete permissions on a specific database (ex: `database.schema.table`)

For SQLMesh (open source) to Tobiko Cloud Migrations Only:

- Solutions Architect will send you a script to extract your current state to send to the Tobiko Cloud engineers to validate before migration
- After testing, Solutions Architect will schedule a migration date and call to move your state to Tobiko Cloud. There will be some downtime if you are running SQLMesh in a production environment.

> Note: if you must be on VPN to access your data warehouse or have specific security requirements, please let us know and we can discuss options to ensure Tobiko Cloud can securely connect.

Technical Requirements:
- Python 3.10+

## Login to Tobiko Cloud

1. Open a browser and navigate to the Tobiko Cloud URL (ex: https://cloud.tobikodata.com/sqlmesh/tobiko/public-demo/observer/)
2. Leave the username blank and use the temporary password you received from the Solutions Architect in the temporary password link
3. Once logged in, you should see the home page. Your view should be empty, but the below is a populated example with Tobiko Cloud running in production.

TODO: add screenshot of populated home page

## Install the `tcloud` CLI

- Create a new project directory and navigate into it.
- Open your terminal within your terminal/IDE (ex: VSCode)
- Follow the below steps to install the `tcloud` CLI

- Create a new file called `requirements.txt` and add the below:
  
```txt
# source: https://pypi.org/project/tcloud/
tcloud==1.3.0
```

- Run these commands in your terminal:

```bash
# you may need to run `python3` or `pip3` instead of `python` or `pip` depending on your python installation
python -m venv .venv # create a virtual environment
source .venv/bin/activate # activate the virtual environment
pip install -r requirements.txt # install the tcloud CLI
which tcloud # verify the tcloud CLI is installed in the venv in the path above
```

```bash
# you can alias the tcloud cli in your shell for UX familiarity: alias sqlmesh='tcloud sqlmesh'
# save this to your shell profile file (ex: ~/.zshrc or ~/.bashrc) so you don't have to run this command every time
alias sqlmesh='tcloud sqlmesh'
```

## Connect Tobiko Cloud to Data Warehouse

- Create a new file called `tcloud.yaml` and add the below:

```yaml
projects:
    public-demo: # TODO: update this for the project name in the URL
        url: https://cloud.tobikodata.com/sqlmesh/tobiko/public-demo/ # TODO: update for your unique URL
        gateway: tobiko_cloud
        extras: bigquery,web,github  # TODO: update bigquery for your data warehouse
default_project: public-demo # TODO: update this for the project name in the URL
```

- Export the token from the Solutions Architect in your shell

```bash
export TCLOUD_TOKEN=<your token> # ex: export TCLOUD_TOKEN='jiaowjifeoawj$22fe'
```

- Initialize a new SQLMesh project:

```bash
sqlmesh init <your data warehouse> # or `tcloud sqlmesh init` if you did NOT alias the tcloud cli for familiar UX
```

- In your project directory, update your `config.yaml` with your data warehouse configs, example below:

See more examples [here](../../docs/integrations/overview.md) 

```yaml
gateways:
    tobiko_cloud: # this will use the config in tcloud.yaml for state_connection
        connection:
            type: bigquery
            method: service-account-json
            concurrent_tasks: 5
            register_comments: true
            keyfile_json: {{ env_var('GOOGLE_SQLMESH_CREDENTIALS') }}
            project: sqlmesh-public-demo

default_gateway: tobiko_cloud

model_defaults:
    dialect: bigquery # TODO: update for your data warehouse
    start: 2024-08-19 # TODO: I recommend updating this to an earlier date representing the historical data you want to backfill

# make Tobiko Cloud only allow deploying to dev environments, use env var to override in CI/CD
# allow_prod_deploy: {{ env_var('ALLOW_PROD_DEPLOY', 'false') }}

# enables synchronized deployments to prod when a pull request is merged
cicd_bot:
    type: github
    merge_method: squash
    enable_deploy_command: true
    auto_categorize_changes:
      external: full
      python: full
      sql: full
      seed: full

# preview data for forward only models
plan:
  enable_preview: true

# list of users that are allowed to approve PRs for synchronized deployments
users:
- username: sung_sqlmesh_demo
  github_username: sungchun12
  roles:
    - required_approver
```

We recommend creating a `.env` file in your root project directory to store your environment variables and verify it is in your `.gitignore` file to prevent it from being committed and exposed in plain text.

```text
# TODO: add any other environment variables such as username, ports, etc. based on your data warehouse
DATA_WAREHOUSE_CREDENTIALS=<your data warehouse credentials>
TCLOUD_TOKEN=<your tcloud token>
```
Run these commands to set your environment variables:
Pro tip: you can include these in a `Makefile` to make it easier to run commands.

```bash
set -a        # Turn on auto-export
source .env   # Read the file, all variables are automatically exported
set +a        # Turn off auto-export
```

- Verify the connection to your data warehouse and the state is synced:

```bash
sqlmesh info
```

```shell
# example output
(.venv) ➜  tcloud_project git:(main) ✗ sqlmesh info
Models: 3
Macros: 0
Data warehouse connection succeeded
State backend connection succeeded
```

Run `sqlmesh plan` to verify everything is working as expected. Enter `y` to apply the changes. Example output below:

```bash
sqlmesh plan
```

```shell
(.venv) ➜  tcloud_project git:(main) ✗ sqlmesh plan
======================================================================
Successfully Ran 1 tests against duckdb
----------------------------------------------------------------------
New environment `prod` will be created from `prod`
Summary of differences against `prod`:
Models:
└── Added:
    ├── sqlmesh_example.full_model
    ├── sqlmesh_example.incremental_model
    └── sqlmesh_example.seed_model
Models needing backfill (missing dates):
├── sqlmesh_example.full_model: 2024-11-24 - 2024-11-24
├── sqlmesh_example.incremental_model: 2020-01-01 - 2024-11-24
└── sqlmesh_example.seed_model: 2024-11-24 - 2024-11-24
Apply - Backfill Tables [y/n]: y
Creating physical tables ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

All model versions have been created successfully

[1/1] sqlmesh_example.seed_model evaluated in 0.00s
[1/1] sqlmesh_example.incremental_model evaluated in 0.01s
[1/1] sqlmesh_example.full_model evaluated in 0.01s
Evaluating models ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00  
                                                                                   

All model batches have been executed successfully

Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

You should see your tcloud project directory look and feel like the below. From here, if you have an existing SQLMesh project, you can copy over your existing models and macros to the `models` and `macros` directories along with others as needed.

TODO: add screenshot of project directory

You are now fully onboarded with Tobiko Cloud. We recommend reviewing the helpful links below to get familiar with SQLMesh and Tobiko Cloud. Here's to having data transformation without the waste!

### Helpful Links
- [Walkthrough Example](https://sqlmesh.readthedocs.io/en/stable/examples/incremental_time_full_walkthrough/)
- [Quickstart](https://sqlmesh.readthedocs.io/en/stable/quick_start/)
- [Project Guide and getting setup](https://sqlmesh.readthedocs.io/en/stable/guides/projects/)
- [Models Guide](https://sqlmesh.readthedocs.io/en/stable/guides/models/)
- [GitHub Actions CI/CD bot](https://sqlmesh.readthedocs.io/en/stable/integrations/github/)
- [Testing Models](https://sqlmesh.readthedocs.io/en/stable/concepts/tests/)
- [SQLMesh Macros](https://sqlmesh.readthedocs.io/en/stable/concepts/macros/sqlmesh_macros/)