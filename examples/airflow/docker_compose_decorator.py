import os

from ruamel.yaml import YAML

DOCKER_COMPOSE_YAML = "docker-compose.yaml"

yaml = YAML(typ="safe")
yaml.default_flow_style = False


with open(DOCKER_COMPOSE_YAML, "r", encoding="utf-8") as fd:
    docker_compose = yaml.load(fd)

docker_compose["x-airflow-common"]["volumes"].extend(
    [
        "./spark_conf:/opt/spark/conf",
        "./spark_conf:/opt/hive/conf",
        "./warehouse:/opt/warehouse",
        "../../:/opt/sqlmesh",
    ]
)

# Dont load Airflow example DAGs because they cause visual pollution
docker_compose["x-airflow-common"]["environment"]["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"

docker_compose["services"]["postgres"]["ports"] = ["5432:5432"]

docker_compose["services"]["create-metastore-db"] = {
    "command": [
        "psql",
        "-U",
        "airflow",
        "--host",
        "postgres",
        "-c",
        "CREATE DATABASE metastore_db",
    ],
    "environment": {
        "PGPASSWORD": "airflow",
    },
    "image": docker_compose["services"]["postgres"]["image"],
    "depends_on": {
        "postgres": {
            "condition": "service_healthy",
        },
    },
    "profiles": [
        "sqlmesh-warehouse-init",
    ],
}

docker_compose["services"]["provision-metastore-tables"] = {
    "entrypoint": "/bin/bash",
    "command": [
        "-c",
        "/opt/hive/bin/schematool -dbType postgres -initSchema",
    ],
    "image": "airflow-sqlmesh",
    "user": "airflow",
    "volumes": [
        "./spark_conf:/opt/spark/conf",
        "./spark_conf:/opt/hive/conf",
        "./warehouse:/opt/warehouse",
    ],
    "depends_on": {
        "postgres": {
            "condition": "service_healthy",
        },
    },
    "profiles": [
        "sqlmesh-warehouse-init",
    ],
}

docker_compose["services"]["sqlmesh-tests"] = {
    "entrypoint": "/bin/bash",
    "command": [
        "-c",
        "make install-dev && pytest -m 'airflow and docker'",
    ],
    "image": "airflow-sqlmesh",
    "user": "airflow",
    "volumes": [
        "./spark_conf:/opt/spark/conf",
        "./spark_conf:/opt/hive/conf",
        "./warehouse:/opt/warehouse",
        "../../:/opt/sqlmesh",
    ],
    "environment": {
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "postgresql+psycopg2://airflow:airflow@postgres/airflow",
        "IS_DOCKER": "true",
    },
    "working_dir": "/opt/sqlmesh",
    "profiles": [
        "sqlmesh-tests",
    ],
}

engine_operator = os.environ.get("AIRFLOW_ENGINE_OPERATOR", "spark").lower()
for airflow_component in ["airflow-scheduler", "airflow-worker"]:
    environment_variables = {"AIRFLOW_ENGINE_OPERATOR": engine_operator}
    if engine_operator == "databricks":
        if not all(
            variable in os.environ
            for variable in [
                "DATABRICKS_SERVER_HOSTNAME",
                "DATABRICKS_TOKEN",
                "DATABRICKS_HTTP_PATH",
            ]
        ):
            raise RuntimeError(
                "Tried to use Databricks Airflow Engine operator but did not define `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`"
            )
        environment_variables["AIRFLOW_CONN_DATABRICKS_DEFAULT"] = (
            "databricks://${DATABRICKS_SERVER_HOSTNAME}?token=${DATABRICKS_TOKEN}&http_path=${DATABRICKS_HTTP_PATH}"
        )
    if os.getenv("DEMO_GITHUB_PAT"):
        environment_variables["AIRFLOW_CONN_GITHUB_DEFAULT"] = (
            '{"conn_type": "github", "password": "${DEMO_GITHUB_PAT}"}'
        )
    docker_compose["services"][airflow_component]["environment"].update(environment_variables)


with open(DOCKER_COMPOSE_YAML, "w", encoding="utf-8") as fd:
    yaml.dump(docker_compose, fd)
