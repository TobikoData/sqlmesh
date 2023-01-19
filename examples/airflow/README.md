# SQLMesh Airflow Examples

## Requirements
1. Docker
2. Docker Compose

**Note:** the Docker instance must be configured to use 4GB of memory for all containers to run properly.

## Install and Run
Initialize the Airflow environment first. This should only be done once:
```bash
make init
```
Run the Airflow cluster in Docker:
```bash
make run
```
The UI should now become available at [http://localhost:8080/](http://localhost:8080/). The account created has the login `airflow` and the password `airflow`.

Terminate the Airflow cluster:
```bash
make stop
```
Clean the environment:
```bash
make clean
```
Re-create and re-launch the environment in one command:
```bash
make clean init run
```
Access the Postgres instance with psql:
```bash
make psql
```
Run the Spark SQL REPL on a running cluster:
```bash
make spark-sql
```

## CLI
After installation is complete the Airflow CLI script will become available:
```bash
./airflow.sh
```
