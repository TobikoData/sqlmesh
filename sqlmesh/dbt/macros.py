from __future__ import annotations

from sqlmesh.dbt.common import Dependencies
from sqlmesh.utils.metaprogramming import Executable
from sqlmesh.utils.pydantic import PydanticModel


class MacroConfig(PydanticModel):
    """Container class for macro configuration"""

    macro: Executable
    dependencies: Dependencies = Dependencies()
