from setuptools import find_packages, setup

setup(
    name="custom_materializations",
    packages=find_packages(include=["custom_materializations"]),
    entry_points={
        "sqlmesh.materializations": [
            "custom_full_materialization = custom_materializations.full:CustomFullMaterialization",
            "custom_full_with_custom_kind = custom_materializations.custom_kind:CustomFullWithCustomKindMaterialization",
        ],
    },
    install_requires=[
        "sqlmesh",
    ],
)
