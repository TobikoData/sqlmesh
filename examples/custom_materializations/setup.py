from setuptools import find_packages, setup

setup(
    name="custom_materializations",
    packages=find_packages(include=["custom_materializations"]),
    entry_points={
        "sqlmesh.materializations": [
            "custom_full_materialization = custom_materializations.full:CustomFullMaterialization",
        ],
    },
    install_requires=[
        "sqlmesh",
    ],
)
