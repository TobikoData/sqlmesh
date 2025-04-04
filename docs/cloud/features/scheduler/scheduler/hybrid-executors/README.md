# Tobiko Cloud Hybrid Executors Helm Chart

This Helm chart deploys Tobiko Cloud hybrid executors, enabling your on-premise Kubernetes cluster to connect to Tobiko Cloud for operations. Hybrid executors allow you to run operations on your own infrastructure while leveraging Tobiko Cloud for orchestration.

## What This Chart Does

The hybrid executors connect your data warehouse to Tobiko Cloud in a secure way:

- **Apply Executor**: Handles applying changes to the data warehouse
- **Run Executor**: Handles scheduled model execution

Both executors need to be properly configured with environment variables to connect to Tobiko Cloud and your data warehouse.

## Prerequisites

- Helm 3.0+
- A Tobiko Cloud account with client ID and client secret
- Access to a data warehouse (e.g., Postgres, Snowflake, BigQuery)

## Quick Start Guide

Since this chart is not published to a Helm repository, you'll need to clone the repository first:

```bash
# Clone the repository
git clone https://github.com/TobikoData/sqlmesh.git
cd sqlmesh/docs/cloud/features/scheduler/scheduler/hybrid-executors
```

### Option 1: Install Directly With Helm

```bash
# Install the chart from local directory
helm install my-executors . \
  --set global.cloud.org=your-organization \
  --set global.cloud.project=your-project \
  --set global.cloud.clientId=your-client-id \
  --set secrets.tcloudClientSecret=your-client-secret
```

### Option 2: Generate YAML Files Without Installing

If you prefer to review and apply the Kubernetes YAML files manually:

```bash
# Create a values file
cat > my-values.yaml << EOF
global:
  cloud:
    org: "your-organization"
    project: "your-project"
    clientId: "your-client-id"
  sqlmesh:
    gateways:
      GATEWAY_A:
        connection:
          type: postgres
          host: "your-database-host"
          port: 5432
          database: "your-database"
          user: "your-database-user"

secrets:
  tcloudClientSecret: "your-client-secret"
EOF

# Generate YAML files without installing
helm template my-executors . -f my-values.yaml > generated-manifests.yaml

# Review the generated files
cat generated-manifests.yaml

# Apply when ready
kubectl apply -f generated-manifests.yaml
```

## Basic Configuration

The most important configuration values:

| Parameter                    | Description                         | Required           |
|------------------------------|-------------------------------------|--------------------|
| `global.cloud.org`           | Your Tobiko Cloud organization name | Yes                |
| `global.cloud.project`       | Your Tobiko Cloud project name      | Yes                |
| `global.cloud.clientId`      | Your Tobiko Cloud client ID         | Yes                |
| `secrets.tcloudClientSecret` | Your Tobiko Cloud client secret     | Yes                |
| `global.sqlmesh.gateways`    | Database connections configuration  | Yes                |
| `apply.enabled`              | Enable Apply executor               | No (default: true) |
| `run.enabled`                | Enable Run executor                 | No (default: true) |

### Database Configuration

Configure your database connection in the `global.sqlmesh.gateways` section:

```yaml
global:
  sqlmesh:
    defaultGateway: "GATEWAY_A"  # The default gateway to use (will be automatically converted to uppercase)
    gateways:
      GATEWAY_A:  # You can name this anything; it will be converted to uppercase in environment variables
        connection:
          type: postgres  # Or snowflake, bigquery, etc.
          host: "your-db-host"
          port: 5432
          database: "sqlmesh"
          user: "sqlmesh_user"
          # Password should be managed as a secret (see below)
```

### Default Gateway Auto-Detection

If you don't specify a `defaultGateway`, the chart will automatically use the first gateway defined in your configuration.

Example with auto-detection (no `defaultGateway` specified):

```yaml
global:
  sqlmesh:
    gateways:
      MY_GATEWAY:  # This will automatically be used as the default gateway and converted to uppercase
        connection:
          type: postgres
          # ...other connection parameters
```

## Secret Management Options

The chart provides multiple options for managing secrets to facilitate different security requirements and deployment patterns:

### Dynamic Secret Detection

The chart automatically treats any gateway connection parameter with `password`, `secret`, or `token` in its name as a secret:

```yaml
global:
  sqlmesh:
    gateways:
      GATEWAY_A:  # Gateway names will be converted to uppercase in environment variables
        connection:
          type: postgres          # Not a secret
          host: "my-db-host"      # Not a secret
          password: "p@ssw0rd"    # Treated as a secret
          client_secret: "xyz123" # Treated as a secret
          api_token: "abc456"     # Treated as a secret
          access_key: "key123"    # Not a secret (doesn't contain a secret keyword)
```

You can explicitly control which parameters should be treated as secrets:

```yaml
global:
  sqlmesh:
    # Force these parameters to be treated as secrets regardless of name
    secretParams: ["access_key", "certificate"] 
    # Force these parameters to NOT be treated as secrets even if they contain secret keywords
    nonSecretParams: ["token_endpoint", "password_policy"]
```

### Option 1: Directly in values.yaml (Development Only)

Secrets can be defined directly in the values file:

```yaml
secrets:
  tcloudClientSecret: "your-tobiko-cloud-client-secret"
  data:
    CUSTOM_SECRET: "custom-secret-value"

global:
  cloud:
    clientId: "your-tobiko-cloud-client-id"  # Not a secret
```

⚠️ **WARNING**: This approach is only recommended for development environments. Never store secrets in plain text in version control.

### Option 2: Using Existing Kubernetes Secrets

Reference an existing Kubernetes Secret:

```yaml
secrets:
  existingSecret: "my-existing-secret"
```

The existing secret must contain the required keys:
- `TCLOUD_CLIENT_SECRET`
- `SQLMESH__GATEWAYS__<GATEWAY_NAME>__CONNECTION__PASSWORD` for each gateway

Alternatively, specify secrets at the executor level:

```yaml
apply:
  envFromSecret: "apply-executor-secrets"
run:
  envFromSecret: "run-executor-secrets"
```

### Option 3: External Secrets Operator

If you're using [External Secrets Operator](https://external-secrets.io/):

```yaml
secrets:
  externalSecrets:
    enabled: true
    secretStore: "aws-secretsmanager"
    keyPrefix: "sqlmesh/"
```

This will create an ExternalSecret resource that pulls secrets from your configured secret provider.

### Option 4: Sealed Secrets

For [Bitnami Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets):

```yaml
secrets:
  sealedSecrets:
    enabled: true
```

You'll need to generate and provide your own SealedSecret manifest based on the sealed-secrets-example.yaml template.

## Environment Variables

Regardless of which secret management option you choose, the following environment variables are set automatically:

- `TCLOUD_CLIENT_ID`: Client ID for Tobiko Cloud authentication (not a secret)
- `TCLOUD_CLIENT_SECRET`: Client Secret for Tobiko Cloud authentication
- `TCLOUD_URL`: Generated from the cloud.baseUrl, org, and project values
- `SQLMESH__DEFAULT_GATEWAY`: The default gateway to use (first gateway if not specified explicitly, always in uppercase)
- `SQLMESH__GATEWAYS__<NAME>__CONNECTION__*`: Connection parameters for each gateway (gateway names and parameter names are converted to uppercase)

## Example: Creating a Secret Manually

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
  sqlmesh:
    defaultGateway: "GATEWAY_A"  # Will be automatically converted to uppercase
    gateways:
      GATEWAY_A:  # Gateway names will be converted to uppercase in environment variables
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
      GATEWAY_B:
        connection:
          type: snowflake
          # Other non-secret parameters
          # client_secret: (loaded from secret)
```

## Customizing Resources

You can customize CPU and memory for each executor:

```yaml
apply:
  resources:
    requests:
      memory: "2Gi"
      cpu: "1"
    limits:
      memory: "4Gi"
      cpu: "2"
```

## Verifying the Installation

After installation, check that the executors are running:

```bash
kubectl get pods -l app.kubernetes.io/instance=my-executors
```

You should see pods for both apply and run executors (if enabled):
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
- Database connection issues
- Insufficient permissions

## Full Example

```yaml
# values.yaml
secrets:
  existingSecret: "my-sqlmesh-secrets"

global:
  image:
    repository: tobikodata/tcloud
    tag: latest
  cloud:
    baseUrl: "https://cloud.tobikodata.com"
    org: "my-organization"
    project: "my-project"
    clientId: "your-client-id"  # Not a secret
  sqlmesh:
    gateways:
      GATEWAY_A:
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