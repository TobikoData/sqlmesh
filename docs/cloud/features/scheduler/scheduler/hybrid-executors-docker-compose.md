# Tobiko Cloud Hybrid Executors - Docker Compose Setup

This Docker Compose configuration allows you to run Tobiko Cloud hybrid executors locally or on any server that supports Docker Compose. Hybrid executors enable your own infrastructure to connect to Tobiko Cloud for operations while keeping your data warehouse credentials within your environment.

## What This Setup Provides

The hybrid executors connect your data warehouse to Tobiko Cloud in a secure way:

- **Apply Executor**: Executes operations that change state, like creating and updating models
- **Run Executor**: Runs read-only operations like planning and analysis

## Prerequisites

- Docker and Docker Compose
- A Tobiko Cloud account with client ID and client secret
- Access to a data warehouse (e.g., Postgres, Snowflake, BigQuery)

## Quick Start Guide

1. **Create the necessary files**:

   Create a `docker compose.yml` file:

   ```yaml
   version: '3.8'

   services:
     apply-executor:
       image: tobikodata/tcloud:latest
       command: executor apply
       restart: unless-stopped
       environment:
         # Tobiko Cloud connection
         TCLOUD_URL: https://cloud.tobikodata.com/sqlmesh/${ORGANIZATION}/${PROJECT}
         TCLOUD_CLIENT_ID: ${TCLOUD_CLIENT_ID}
         TCLOUD_CLIENT_SECRET: ${TCLOUD_CLIENT_SECRET}
         
         # SQLMesh configuration
         SQLMESH__DEFAULT_GATEWAY: ${DEFAULT_GATEWAY:-GATEWAY_A}
         
         # Example database configuration for Postgres (adjust for your database)
         # All database parameters below should be customized for your specific setup
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__TYPE: ${DB_TYPE:-postgres}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__HOST: ${DB_HOST}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__PORT: ${DB_PORT:-5432}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__DATABASE: ${DB_NAME}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__USER: ${DB_USER}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__PASSWORD: ${DB_PASSWORD}
       volumes:
         # Optional volume for persistent storage if needed
         - apply-executor-data:/app/data
       deploy:
         resources:
           limits:
             memory: ${APPLY_MEMORY_LIMIT:-4g}
             cpus: ${APPLY_CPU_LIMIT:-2}
           reservations:
             memory: ${APPLY_MEMORY_REQUEST:-2g}
             cpus: ${APPLY_CPU_REQUEST:-1}

     run-executor:
       image: tobikodata/tcloud:latest
       command: executor run
       restart: unless-stopped
       environment:
         # Tobiko Cloud connection
         TCLOUD_URL: https://cloud.tobikodata.com/sqlmesh/${ORGANIZATION}/${PROJECT}
         TCLOUD_CLIENT_ID: ${TCLOUD_CLIENT_ID}
         TCLOUD_CLIENT_SECRET: ${TCLOUD_CLIENT_SECRET}
         
         # SQLMesh configuration
         SQLMESH__DEFAULT_GATEWAY: ${DEFAULT_GATEWAY:-GATEWAY_A}
         
         # Example database configuration for Postgres (adjust for your database)
         # All database parameters below should be customized for your specific setup
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__TYPE: ${DB_TYPE:-postgres}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__HOST: ${DB_HOST}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__PORT: ${DB_PORT:-5432}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__DATABASE: ${DB_NAME}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__USER: ${DB_USER}
         SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__PASSWORD: ${DB_PASSWORD}
       volumes:
         # Optional volume for persistent storage if needed
         - run-executor-data:/app/data
       deploy:
         resources:
           limits:
             memory: ${RUN_MEMORY_LIMIT:-4g}
             cpus: ${RUN_CPU_LIMIT:-2}
           reservations:
             memory: ${RUN_MEMORY_REQUEST:-2g}
             cpus: ${RUN_CPU_REQUEST:-1}

   volumes:
     apply-executor-data: {}
     run-executor-data: {}
   ```

   Create a `.env.example` file:

   ```
   # Tobiko Cloud Configuration
   ORGANIZATION=your-organization
   PROJECT=your-project
   TCLOUD_CLIENT_ID=your-client-id
   TCLOUD_CLIENT_SECRET=your-client-secret

   # Database Configuration
   DEFAULT_GATEWAY=GATEWAY_A
   DB_TYPE=postgres
   DB_HOST=your-database-host
   DB_PORT=5432
   DB_NAME=your-database-name
   DB_USER=your-database-user
   DB_PASSWORD=your-database-password

   # Optional: Resource Limits
   APPLY_MEMORY_LIMIT=4g
   APPLY_CPU_LIMIT=2
   APPLY_MEMORY_REQUEST=2g
   APPLY_CPU_REQUEST=1
   RUN_MEMORY_LIMIT=4g
   RUN_CPU_LIMIT=2
   RUN_MEMORY_REQUEST=2g
   RUN_CPU_REQUEST=1
   ```

2. **Create your environment file**:

   ```bash
   cp .env.example .env
   ```

3. **Edit the .env file** with your specific configuration:
   - Set your Tobiko Cloud organization, project, client ID, and client secret
   - Configure your database connection details
   - Adjust resource limits if needed

4. **Start the executors**:

   ```bash
   docker compose up -d
   ```

5. **Check the logs**:

   ```bash
   docker compose logs -f
   ```

## Configuration Options

### Database Configuration

The default configuration is set up for PostgreSQL, but you can use any supported database by adjusting the connection parameters in your `.env` file:

### Multiple Gateways

To configure multiple gateways, edit the docker compose.yml file to add additional environment variables for each gateway:

```yaml
environment:
  # First gateway
  SQLMESH__GATEWAYS__GATEWAY_A__CONNECTION__TYPE: ${DB_TYPE:-postgres}
  # ... other GATEWAY_A configuration ...
  
  # Second gateway
  SQLMESH__GATEWAYS__GATEWAY_B__CONNECTION__TYPE: snowflake
  SQLMESH__GATEWAYS__GATEWAY_B__CONNECTION__ACCOUNT: ${SNOWFLAKE_ACCOUNT}
  # ... other GATEWAY_B configuration ...
```

## Health Checking

You can verify executor health by running:

```bash
docker compose exec apply-executor executor apply --check
docker compose exec run-executor executor run --check
```

## Stopping the Executors

To stop the executors:

```bash
docker compose down
```

## Troubleshooting

If you encounter issues:

1. Check the logs: `docker compose logs -f`
2. Verify your connection settings in the `.env` file
3. Ensure your client ID and client secret are correct
4. Check that your database is accessible from the Docker containers

## Security Considerations

- The `.env` file contains sensitive information. Never commit it to version control.
- Consider using Docker secrets or a secrets management solution in production environments.
- For production deployments, consider using the Kubernetes Helm chart instead, which offers more robust reliability and secret management options. 