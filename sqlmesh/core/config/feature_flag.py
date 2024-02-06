from sqlmesh.utils.pydantic import PydanticModel


class DbtFeatureFlag(PydanticModel):
    scd_type_2_support: bool = True


class FeatureFlag(PydanticModel):
    dbt: DbtFeatureFlag = DbtFeatureFlag()
