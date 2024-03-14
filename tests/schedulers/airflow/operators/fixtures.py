import os
from unittest import mock

import pytest


@pytest.fixture
def set_airflow_as_library():
    with mock.patch.dict(os.environ, {"_AIRFLOW__AS_LIBRARY": "1"}):
        yield
