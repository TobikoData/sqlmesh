from __future__ import annotations

import typing as t

from fastapi import APIRouter, Depends

from web.server import models
from web.server.settings import Settings, get_settings

router = APIRouter()


@router.get(
    "",
    response_model=t.List[models.Modules],
    response_model_exclude_unset=True,
)
def get_api_modules(
    settings: Settings = Depends(get_settings),
) -> t.List[models.Modules]:
    """Get the modules"""
    return list(settings.modules)
