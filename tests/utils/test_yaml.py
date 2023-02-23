import pytest

import sqlmesh.utils.yaml as yaml
from sqlmesh.utils.errors import SQLMeshError


def test_yaml() -> None:
    contents = """profile:
  target: prod
  outputs:
    prod:
      type: postgres
      host: 127.0.0.1
      # IMPORTANT: Make sure to quote the entire Jinja string here
      user: "{{ env_var('__SQLMESH_TEST_ENV_USER__') }}"
      password: "{{ env_var('__SQLMESH_TEST_ENV_PASSWORD__') }}"
"""

    assert contents == yaml.dumps(yaml.load(contents))

    with pytest.raises(SQLMeshError) as ex:
        yaml.load("")
    assert "YAML source can't be empty." in str(ex.value)
