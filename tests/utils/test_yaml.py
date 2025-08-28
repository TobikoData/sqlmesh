import os

import pytest
from decimal import Decimal

import sqlmesh.utils.yaml as yaml
from sqlmesh.utils.errors import SQLMeshError


def test_yaml() -> None:
    contents = """profile:
  target: prod
  outputs:
    prod:
      type: postgres
      host: 127.0.0.1
      user: "{{ env_var('__SQLMESH_TEST_ENV_USER__') }}"
      password: "{{ env_var('__SQLMESH_TEST_ENV_PASSWORD__') }}"
"""

    assert contents == yaml.dump(yaml.load(contents, render_jinja=False))

    expected_contents = """profile:
  target: prod
  outputs:
    prod:
      type: postgres
      host: 127.0.0.1
      user: user
      password: password
"""

    os.environ["__SQLMESH_TEST_ENV_USER__"] = "user"
    os.environ["__SQLMESH_TEST_ENV_PASSWORD__"] = "password"

    assert expected_contents == yaml.dump(yaml.load(contents))

    # Return the environment to its previous state
    del os.environ["__SQLMESH_TEST_ENV_USER__"]
    del os.environ["__SQLMESH_TEST_ENV_PASSWORD__"]

    with pytest.raises(SQLMeshError) as ex:
        yaml.load("")
    assert "YAML source can't be empty." in str(ex.value)

    decimal_value = Decimal(123.45)
    assert yaml.load(yaml.dump(decimal_value)) == str(decimal_value)


def test_load_keep_last_duplicate_key() -> None:
    input_str = """
name: first_name
name: second_name
name: third_name

foo: bar

mapping:
    key: first_value
    key: second_value
    key: third_value

sequence:
    - one
    - two
"""
    # Default behavior of ruamel is to keep the first key encountered
    assert yaml.load(input_str, allow_duplicate_keys=True) == {
        "name": "first_name",
        "foo": "bar",
        "mapping": {"key": "first_value"},
        "sequence": ["one", "two"],
    }

    # Test keeping last key
    assert yaml.load(input_str, allow_duplicate_keys=True, keep_last_duplicate_key=True) == {
        "name": "third_name",
        "foo": "bar",
        "mapping": {"key": "third_value"},
        "sequence": ["one", "two"],
    }
