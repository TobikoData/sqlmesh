import logging
import os

import pytest
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.config import AirflowSchedulerConfig
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils import str_to_bool

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def is_docker() -> bool:
    return str_to_bool(os.environ.get("IS_DOCKER"))


@pytest.fixture(scope="session")
def airflow_host(is_docker: bool) -> str:
    return "airflow-webserver" if is_docker else "localhost"


@pytest.fixture(scope="session")
def airflow_scheduler_backend(airflow_host: str) -> AirflowSchedulerConfig:
    return _get_airflow_scheduler_backend(airflow_host)


@pytest.fixture(scope="session")
def airflow_client(airflow_scheduler_backend: AirflowSchedulerConfig) -> AirflowClient:
    return airflow_scheduler_backend.get_client()


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10), reraise=True)
def _get_airflow_scheduler_backend(airflow_host: str) -> AirflowSchedulerConfig:
    backend = AirflowSchedulerConfig(airflow_url=f"http://{airflow_host}:8080/")
    client = backend.get_client()

    try:
        client.get_all_dags()
    except Exception:
        logger.info(
            "Failed to fetch the list of DAGs from Airflow. Make sure the test Airflow cluster is running"
        )
        raise

    logger.info("The Airflow Client is ready")

    return backend
