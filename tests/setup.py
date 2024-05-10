import os

import setuptools

os.chdir(os.path.join(os.path.dirname(__file__), ".."))
sqlmesh_dist = setuptools.distutils.core.run_setup("setup.py", stop_after="init")
requirements = sqlmesh_dist.install_requires + sqlmesh_dist.extras_require["dev"]  # type: ignore
os.chdir(os.path.dirname(__file__))

setuptools.setup(
    name="sqlmesh-tests",
    description="Tests for SQLMesh",
    url="https://github.com/TobikoData/sqlmesh",
    author="TobikoData Inc.",
    author_email="engineering@tobikodata.com",
    license="Apache License 2.0",
    package_dir={"sqlmesh_tests": ""},
    package_data={"": ["fixtures/**"]},
    use_scm_version={
        "root": "..",
        "write_to": "_version.py",
        "fallback_version": "0.0.0",
        "local_scheme": "no-local-version",
    },
    setup_requires=["setuptools_scm"],
    install_requires=requirements,
)
