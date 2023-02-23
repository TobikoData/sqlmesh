from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from web.server.models import Model, Models
from web.server.settings import Settings, get_loaded_context, get_settings

router = APIRouter()


@router.get("", response_model=Models)
def get_models(
    context: Context = Depends(get_loaded_context),
    settings: Settings = Depends(get_settings),
) -> Models:
    """Get a mapping of model names to model metadata"""
    models = {
        model.name: Model(
            name=model.name,
            path=str(model._path.relative_to(context.path)),
            description=model.description,
            owner=model.owner,
        )
        for model in context.models.values()
    }
    return Models(models=models)
