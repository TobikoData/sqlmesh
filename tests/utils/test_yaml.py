import os

import sqlmesh.utils.yaml as yaml


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

    # Without Jinja rendering (uses ruamel.yaml.YAML methods directly)
    assert contents == yaml.dumps(yaml.yaml.load(contents))

    expected_contents = """profile:
  target: prod
  outputs:
    prod:
      type: postgres
      host: 127.0.0.1
      # IMPORTANT: Make sure to quote the entire Jinja string here
      user: user
      password: password
"""

    os.environ["__SQLMESH_TEST_ENV_USER__"] = "user"
    os.environ["__SQLMESH_TEST_ENV_PASSWORD__"] = "password"

    print(yaml.dumps(yaml.load(contents)))
    assert expected_contents == yaml.dumps(yaml.load(contents))

    # Return the environment to its previous state
    del os.environ["__SQLMESH_TEST_ENV_USER__"]
    del os.environ["__SQLMESH_TEST_ENV_PASSWORD__"]
