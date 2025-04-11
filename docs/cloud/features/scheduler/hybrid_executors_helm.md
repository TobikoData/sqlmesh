# Tobiko Cloud Hybrid Executors Helm Chart

This Helm chart deploys Tobiko Cloud hybrid executors, enabling your on-premise Kubernetes cluster to connect to Tobiko Cloud for operations.

Hybrid executors allow you to run operations on your own infrastructure while leveraging Tobiko Cloud for orchestration.

## What this chart does

This chart deploys two hybrid executors that pass work tasks from Tobiko Cloud to your data warehouse in a secure way:

- **Apply Executor**: Handles applying changes to the data warehouse
- **Run Executor**: Handles scheduled model execution

Both executors must be properly configured with environment variables to connect to Tobiko Cloud and your data warehouse.

## Prerequisites

- Access to a [data warehouse supported by Tobiko Cloud](../../../integrations/overview.md#execution-engines) (e.g., Postgres, Snowflake, BigQuery)
- Helm 3.8+
- A Tobiko Cloud account with [client ID and client secret](../single_sign_on.md#provisioning-client-credentials)

## Quick start guide

Create a `values.yaml` file with your Tobiko Cloud configuration.

```bash
# Create a values file
cat > my-values.yaml << EOF
global:
  cloud:
    org: "your-organization"
    project: "your-project"
    clientId: "your-client-id"
    clientSecret: "your-client-secret"
  sqlmesh:
    gateways:
      gateway_a:
        connection:
          type: postgres
          host: "your-database-host"
          port: 5432
          database: "your-database"
          user: "your-database-user"
EOF
```

### Option 1: Install Directly with Helm

```bash
# Install the chart from local directory
helm install executors oci://registry-1.docker.io/tobikodata/hybrid-executors -f my-values.yaml
```

### Option 2: Generate YAML files without installing

If you prefer to review and apply the Kubernetes YAML files manually:

```bash
# Generate YAML files without installing
helm template executors oci://registry-1.docker.io/tobikodata/hybrid-executors -f my-values.yaml > generated-manifests.yaml
```

```bash
# Review the generated files
cat generated-manifests.yaml
```

```bash
# Apply when ready
kubectl apply -f generated-manifests.yaml
```

## Basic configuration

The most important configuration values are:

| Parameter                   | Description                         | Required           |
|-----------------------------|-------------------------------------|--------------------|
| `global.cloud.org`          | Your Tobiko Cloud organization name | Yes                |
| `global.cloud.project`      | Your Tobiko Cloud project name      | Yes                |
| `global.cloud.clientId`     | Your Tobiko Cloud client ID         | Yes                |
| `global.cloud.clientSecret` | Your Tobiko Cloud client secret     | Yes                |
| `global.sqlmesh.gateways`   | Database connections configuration  | Yes                |

### Gateway configuration

Configure your gateway's SQL engine connection in the `global.sqlmesh.gateways` section:

```yaml
global:
  sqlmesh:
    gateways:
      gateway_a:  # Put the default gateway first if defining multiple gateways
        connection:
          type: postgres  # Or snowflake, bigquery, etc.
          host: "your-db-host"
          port: 5432
          database: "sqlmesh"
          user: "sqlmesh_user"
          # Password should be managed as a secret (see below)
```

## Secret management options

The chart provides multiple options for managing secrets. Use the one most aligned with your security requirements and deployment patterns.

### Context: Helm's dynamic secret detection

The chart automatically treats `global.cloud.clientSecret` and any gateway connection parameter with keywords `password`, `secret`, or `token` in its name as a secret:

```yaml
global:
  cloud:
    clientId: "your-client-id"  # Not a secret
    clientSecret: "your-client-secret"  # Automatically treated as a secret (contains keyword "secret")
  sqlmesh:
    gateways:
      gateway_a:
        connection:
          type: postgres          # Not a secret
          host: "my-db-host"      # Not a secret
          password: "p@ssw0rd"    # Automatically treated as a secret (contains keyword "password")
          client_secret: "xyz123" # Automatically treated as a secret (contains keyword "secret")
          api_token: "abc456"     # Automatically treated as a secret (contains keyword "token")
          access_key: "key123"    # Not a secret
```

Use the `secretParams` key to force parameters to be treated as secrets (even if their name doesn't contain a secret keyword):

```yaml
global:
  sqlmesh:
    # Force these parameters to be treated as secrets regardless of name
    secretParams: ["access_key", "certificate"]
    # Force these parameters to NOT be treated as secrets even if they contain secret keywords
    nonSecretParams: ["token_endpoint", "password_policy"]
```

### Option 1: Secrets directly in values.yaml (development only)

!!! warning "Development only"

    This approach is only recommended for development environments and testing.

    Never store secrets in plain text in version control.

Define secrets directly in the values file:

```yaml
global:
  cloud:
    clientId: "your-tobiko-cloud-client-id"  # Not a secret
    clientSecret: "your-tobiko-cloud-client-secret"  # Automatically treated as a secret
```

### Option 2: Existing Kubernetes Secrets

Reference an existing Kubernetes Secret:

```yaml
secrets:
  existingSecret: "my-existing-secret"
```

The existing secret must contain the required keys:

- `TCLOUD_CLIENT_SECRET`
- `SQLMESH__GATEWAYS__<GATEWAY_NAME>__CONNECTION__<SECRET KEY PARAMETER NAME>` for each gateway secret

If your executors use different secrets, you can specify secrets at the executor level:

```yaml
apply:
  envFromSecret: "apply-executor-secrets"
run:
  envFromSecret: "run-executor-secrets"
```

### Option 3: External Secrets Operator

If you're using [External Secrets Operator](https://external-secrets.io/), you can pull secrets from your secret store:

```yaml
secrets:
  externalSecrets:
    enabled: true
    secretStore: "aws-secretsmanager"
    keyPrefix: "sqlmesh/"
```

This will create an ExternalSecret resource that pulls secrets from your configured secret provider.

### Example: Creating a secret manually

Create a secret with all required credentials:

```bash
kubectl create secret generic my-sqlmesh-secrets \
  --from-literal=TCLOUD_CLIENT_SECRET=your-client-secret \
  --from-literal=SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__PASSWORD=your-password \
  --from-literal=SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__API_TOKEN=your-token \
  --from-literal=SQLMESH__GATEWAYS__GATEWAY_B__CONNECTION__CLIENT_SECRET=another-secret
```

Then reference it in your values:

```yaml
secrets:
  existingSecret: "my-sqlmesh-secrets"

global:
  cloud:
    clientId: "your-client-id"  # Not a secret
    # Secret values will be loaded from the secret
    # clientSecret: (loaded from secret)
  sqlmesh:
    gateways:
      gateway_a:
        connection:
          # Define all non-secret parameters here
          type: postgres
          host: "my-db-host"
          port: 5432
          database: "sqlmesh"
          user: "sqlmesh_user"
          # Secret values will be loaded from the secret
          # password: (loaded from secret)
          # api_token: (loaded from secret)
      gateway_b:
        connection:
          type: snowflake
          # Other non-secret parameters
          # client_secret: (loaded from secret)
```

### Example: Using an existing secret

```yaml
# values.yaml
secrets:
  existingSecret: "my-sqlmesh-secrets"

global:
  image:
    repository: tobikodata/tcloud
    tag: latest
  cloud:
    org: "my-organization"
    project: "my-project"
    clientId: "your-client-id"
  sqlmesh:
    gateways:
      gateway_a:
        connection:
          type: postgres
          host: "my-db-host"
          port: 5432
          database: "sqlmesh"
          user: "sqlmesh_user"

apply:
  replicaCount: 1

run:
  replicaCount: 2
```

## Customizing resources

You can customize CPU, memory, and ephemeral-storage for each executor. This sets the resources for the `apply` executor:

```yaml
apply:
  resources:
    requests:
      memory: "2Gi"
      cpu: "1"
      ephemeral-storage: "10Gi"
    limits:
      memory: "4Gi"
      cpu: "2"
      ephemeral-storage: "10Gi"
```

## Verifying the installation

After installation, check that the executors are running:

```bash
kubectl get pods -l app.kubernetes.io/instance=my-executors
```

You should see pods for both apply and run executors:
```
NAME                                  READY   STATUS    RESTARTS   AGE
my-executors-apply-7b6c9d8f9-abc12    1/1     Running   0          1m
my-executors-run-6d5b8c7e8-def34     1/1     Running   0          1m
```

## Troubleshooting

If your executors aren't starting properly, check the logs:

```bash
kubectl logs -l app.kubernetes.io/instance=my-executors,app.kubernetes.io/component=apply-executor
kubectl logs -l app.kubernetes.io/instance=my-executors,app.kubernetes.io/component=run-executor
```

Common issues:

- Incorrect client ID or client secret
- SQL engine connection issues
- Insufficient permissions