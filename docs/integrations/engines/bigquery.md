# BigQuery

## Introduction

This guide provides step-by-step instructions on how to connect SQLMesh to the BigQuery SQL engine.

It will walk you through the steps of installing SQLMesh and BigQuery connection libraries locally, configuring the connection in SQLMesh, and running the [quickstart project](../../quick_start.md).

## Prerequisites

This guide assumes the following about the BigQuery project being used with SQLMesh:

- The project already exists
- Project [CLI/API access is enabled](https://cloud.google.com/endpoints/docs/openapi/enable-api)
- Project [billing is configured](https://cloud.google.com/billing/docs/how-to/manage-billing-account) (i.e. it's not a sandbox project)
- SQLMesh can authenticate using an account with permissions to execute commands against the project

## Installation

Follow the [quickstart installation guide](../../installation.md) up to the step that [installs SQLMesh](../../installation.md#install-sqlmesh-core), where we deviate to also install the necessary BigQuery libraries.

Instead of installing just SQLMesh core, we will also include the BigQuery engine libraries:

```bash
> pip install "sqlmesh[bigquery]"
```

### Install Google Cloud SDK

SQLMesh connects to BigQuery via the Python [`google-cloud-bigquery` library](https://pypi.org/project/google-cloud-bigquery/), which uses the [Google Cloud SDK `gcloud` tool](https://cloud.google.com/sdk/docs) for [authenticating with BigQuery](https://googleapis.dev/python/google-api-core/latest/auth.html).

Follow these steps to install and configure the Google Cloud SDK on your computer:

- Download the appropriate installer for your system from the [Google Cloud installation guide](https://cloud.google.com/sdk/docs/install)
- Unpack the downloaded file with the `tar` command:

    ```bash
    > tar -xzvf google-cloud-cli-{SYSTEM_SPECIFIC_INFO}.tar.gz
    ```

- Run the installation script:

    ```bash
    > ./google-cloud-sdk/install.sh
    ```

- Reload your shell profile (e.g., for zsh):

    ```bash
    > source $HOME/.zshrc
    ```

- Run [`gcloud init` to setup authentication](https://cloud.google.com/sdk/gcloud/reference/init)

## Configuration

### Configure SQLMesh for BigQuery

Add the following gateway specification to your SQLMesh project's `config.yaml` file:

```yaml
bigquery:
  connection:
    type: bigquery
    project: <your_project_id>

default_gateway: bigquery
```

This creates a gateway named `bigquery` and makes it your project's default gateway.

It uses the [`oauth` authentication method](#authentication-methods), which does not specify a username or other information directly in the connection configuration. Other authentication methods are [described below](#authentication-methods).

In BigQuery, navigate to the dashboard and select the BigQuery project your SQLMesh project will use. From the Google Cloud dashboard, use the arrow to open the pop-up menu:  

![BigQuery Dashboard](./bigquery/bigquery-1.png)

Now we can identify the project ID needed in the `config.yaml` gateway specification above. Select the project that you want to work with, the project ID that you need to add to your yaml file is the ID label from the pop-up menu. 

![BigQuery Dashboard: selecting your project](./bigquery/bigquery-2.png)

For this guide, the Docs-Demo is the one we will use, thus the project ID for this example is `healthy-life-440919-s0`. 

## Usage

### Test the connection

Run the following command to verify that SQLMesh can connect to BigQuery:

```bash
> sqlmesh info
```

The output will look something like this:

![Terminal Output](./bigquery/bigquery-3.png)

- **Set quota project (optional)**

    You may see warnings like this when you run `sqlmesh info`:

    ![Terminal Output with warnings](./bigquery/bigquery-4.png)

    You can avoid these warnings about quota projects by running:

    ```bash
    > gcloud auth application-default set-quota-project <your_project_id>
    > gcloud config set project <your_project_id>
    ```


### Create and run a plan

We've verified our connection, so we're ready to create and execute a plan in BigQuery:

```bash
> sqlmesh plan
```

### View results in BigQuery Console

Let's confirm that our project models are as expected.

First, navigate to the BigQuery Studio Console:

![Steps to the Studio](./bigquery/bigquery-5.png)

Then use the left sidebar to find your project and the newly created models:

![New Models](./bigquery/bigquery-6.png)

We have confirmed that our SQLMesh project is running properly in BigQuery!

## Local/Built-in Scheduler

**Engine Adapter Type**: `bigquery`

### Installation
```
pip install "sqlmesh[bigquery]"
```

### Connection options

| Option                          | Description                                                                                                                                                       |  Type  | Required |
|---------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                          | Engine type name - must be `bigquery`                                                                                                                             | string |    Y     |
| `method`                        | Connection methods - see [allowed values below](#connection-methods). Default: `oauth`.                                                                           | string |    N     |
| `project`                       | The ID of the GCP project                                                                                                                                       | string |    N     |
| `location`                      | The location of for the datasets (can be regional or multi-regional)                                                                                              | string |    N     |
| `execution_project`             | The name of the GCP project to bill for the execution of the models. If not set, the project associated with the model will be used.                              | string |    N     |
| `quota_project`                 | The name of the GCP project used for the quota. If not set, the `quota_project_id` set within the credentials of the account is used to authenticate to BigQuery. | string |    N     |
| `keyfile`                       | Path to the keyfile to be used with service-account method                                                                                                        | string |    N     |
| `keyfile_json`                  | Keyfile information provided inline (not recommended)                                                                                                             |  dict  |    N     |
| `token`                         | OAuth 2.0 access token                                                                                                                                            | string |    N     |
| `refresh_token`                 | OAuth 2.0 refresh token                                                                                                                                           | string |    N     |
| `client_id`                     | OAuth 2.0 client ID                                                                                                                                               | string |    N     |
| `client_secret`                 | OAuth 2.0 client secret                                                                                                                                           | string |    N     |
| `token_uri`                     | OAuth 2.0 authorization server's toke endpoint URI                                                                                                                | string |    N     |
| `scopes`                        | The scopes used to obtain authorization                                                                                                                           |  list  |    N     |
| `job_creation_timeout_seconds`  | The maximum amount of time, in seconds, to wait for the underlying job to be created.                                                                             |  int   |    N     |
| `job_execution_timeout_seconds` | The maximum amount of time, in seconds, to wait for the underlying job to complete.                                                                               |  int   |    N     |
| `job_retries`                   | The number of times to retry the underlying job if it fails. (Default: `1`)                                                                                       |  int   |    N     |
| `priority`                      | The priority of the underlying job. (Default: `INTERACTIVE`)                                                                                                      | string |    N     |
| `maximum_bytes_billed`          | The maximum number of bytes to be billed for the underlying job.                                                                                                  |  int   |    N     |

## Airflow Scheduler
**Engine Name:** `bigquery`

In order to share a common implementation across local and Airflow, SQLMesh BigQuery implements its own hook and operator.

### Installation

To enable support for this operator, the Airflow BigQuery provider package should be installed on the target Airflow cluster along with SQLMesh with the BigQuery extra:
```
pip install "apache-airflow-providers-google"
pip install "sqlmesh[bigquery]"
```

### Connection info

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target BigQuery account. Please see [GoogleBaseHook](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/common/hooks/base_google/index.html#airflow.providers.google.common.hooks.base_google.GoogleBaseHook) and [GCP connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)for more details. Use the `sqlmesh_google_cloud_bigquery_default` (by default) connection ID instead of the `google_cloud_default` one in the Airflow guide.

By default, the connection ID is set to `sqlmesh_google_cloud_bigquery_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    default_catalog="<project id>",
    engine_operator_args={
        "bigquery_conn_id": "<Connection ID>"
    },
)
```

#### Optional Arguments

* `location`: Sets the default location for datasets and tables. If not set, BigQuery defaults to US for new datasets. See `location` in [Connection options](#connection-options) for more details.

```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    default_catalog="<project id>",
    engine_operator_args={
        "bigquery_conn_id": "<Connection ID>",
        "location": "<location>"
    },
)
```

## Authentication Methods
- [oauth](https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.default) (default)
    - Related Credential Configuration:
        - `scopes` (Optional)
- [oauth-secrets](https://google-auth.readthedocs.io/en/stable/reference/google.oauth2.credentials.html)
    - Related Credential Configuration:
        - `token` (Optional): Can be None if refresh information is provided.
        - `refresh_token` (Optional): If specified, credentials can be refreshed.
        - `client_id` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `client_secret` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `token_uri` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `scopes` (Optional): OAuth 2.0 credentials can not request additional scopes after authorization. The scopes must be derivable from the refresh token if refresh information is provided (e.g. The refresh token scopes are a superset of this or contain a wild card scope like 'https://www.googleapis.com/auth/any-api')
- [service-account](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_file)
    - Related Credential Configuration:
        - `keyfile` (Required)
        - `scopes` (Optional)
- [service-account-json](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_info)
    - Related Credential Configuration:
        - `keyfile_json` (Required)
        - `scopes` (Optional)

## Permissions Required
With any of the above connection methods, ensure these BigQuery permissions are enabled to allow SQLMesh to work correctly.

- [`BigQuery Data Editor`](https://cloud.google.com/bigquery/docs/access-control#bigquery.dataEditor)
- [`BigQuery User`](https://cloud.google.com/bigquery/docs/access-control#bigquery.user)
