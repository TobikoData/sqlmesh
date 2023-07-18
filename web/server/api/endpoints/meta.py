from fastapi import APIRouter

from sqlmesh.cli.main import _sqlmesh_version
from web.server import models

router = APIRouter()


@router.get(
    "",
    response_model=models.Meta,
    response_model_exclude_unset=True,
)
def get_api_meta() -> models.Meta:
    """Get the metadata"""

    return models.Meta(
        version=_sqlmesh_version(),
    )
