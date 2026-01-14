from sqlmesh.core.config.base import BaseConfig


class DbtConfig(BaseConfig):
    """
    Represents dbt-specific options on the SQLMesh root config.

    These options are only taken into account for dbt projects and are ignored on native projects
    """

    infer_state_schema_name: bool = False
    """If set, indicates to the dbt loader that the state schema should be inferred based on the profile/target
    so that each target gets its own isolated state"""
