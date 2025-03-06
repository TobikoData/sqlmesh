import setuptools
from pathlib import Path
import toml  # type: ignore

# This relies on `make package-tests` copying the sqlmesh pyproject.toml into tests/ so we can reference it
# Otherwise, it's not available in the build environment
sqlmesh_pyproject = Path(__file__).parent / "sqlmesh_pyproject.toml"
parsed = toml.load(sqlmesh_pyproject)["project"]
install_requires = parsed["dependencies"] + parsed["optional-dependencies"]["dev"]

# this is just so we can have a dynamic install_requires, everything else is defined in pyproject.toml
setuptools.setup(install_requires=install_requires)
