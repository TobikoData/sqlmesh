import logging

import pytest
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.config import AirflowSchedulerBackend
from sqlmesh.schedulers.airflow.client import AirflowClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def airflow_client() -> AirflowClient:
    return _get_airflow_client()


@retry(wait=wait_fixed(3), stop=stop_after_attempt(10), reraise=True)
def _get_airflow_client() -> AirflowClient:
    client = AirflowSchedulerBackend().get_client()

    try:
        client.get_all_dags()
    except Exception:
        logger.info(
            "Failed to fetch the list of DAGs from Airflow. Make sure the test Airflow cluster is running"
        )
        raise

    logger.info("The Airflow Client is ready")
    return client
