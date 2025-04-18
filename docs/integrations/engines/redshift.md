# Redshift

## Local/Built-in Scheduler
**Engine Adapter Type**: `redshift`

### Installation
```
pip install "sqlmesh[redshift]"
```

### Connection options

| Option                  | Description                                                                                                 |  Type  | Required |
|-------------------------|-------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                  | Engine type name - must be `redshift`                                                                       | string |    Y     |
| `user`                  | The username to use for authentication with the Amazon Redshift cluster                                     | string |    N     |
| `password`              | The password to use for authentication with the Amazon Redshift cluster                                     | string |    N     |
| `database`              | The name of the database instance to connect to                                                             | string |    N     |
| `host`                  | The hostname of the Amazon Redshift cluster                                                                 | string |    N     |
| `port`                  | The port number of the Amazon Redshift cluster                                                              |  int   |    N     |
| `ssl`                   | Is SSL enabled. SSL must be enabled when authenticating using IAM (Default: `True`)                         |  bool  |    N     |
| `sslmode`               | The security of the connection to the Amazon Redshift cluster. `verify-ca` and `verify-full` are supported. | string |    N     |
| `timeout`               | The number of seconds before the connection to the server will timeout.                                     |  int   |    N     |
| `tcp_keepalive`         | Is [TCP keepalive](https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive) used. (Default: `True`)           |  bool  |    N     |
| `application_name`      | The name of the application                                                                                 | string |    N     |
| `preferred_role`        | The IAM role preferred for the current connection                                                           | string |    N     |
| `principal_arn`         | The ARN of the IAM entity (user or role) for which you are generating a policy                              | string |    N     |
| `credentials_provider`  | The class name of the IdP that will be used for authenticating with the Amazon Redshift cluster             | string |    N     |
| `region`                | The AWS region of the Amazon Redshift cluster                                                               | string |    N     |
| `cluster_identifier`    | The cluster identifier of the Amazon Redshift cluster                                                       | string |    N     |
| `iam`                   | If IAM authentication is enabled. IAM must be True when authenticating using an IdP                         |  dict  |    N     |
| `is_serverless`         | If the Amazon Redshift cluster is serverless (Default: `False`)                                             |  bool  |    N     |
| `serverless_acct_id`    | The account ID of the serverless cluster                                                                    | string |    N     |
| `serverless_work_group` | The name of work group for serverless end point                                                             | string |    N     |
| `enable_merge`         | Whether the incremental_by_unique_key model kind will use the native Redshift MERGE operation or SQLMesh's logical merge. (Default: `False`)           |  bool  |    N     |
