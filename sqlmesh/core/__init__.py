import sys
import typing as t

from pydantic import Field

from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.model import Model
from sqlmesh.core.node import NodeType

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated

Node = Annotated[t.Union[Model, StandaloneAudit], Field(descriminator="source_type")]
