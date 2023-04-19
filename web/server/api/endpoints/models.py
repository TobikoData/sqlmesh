import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("", response_model=t.List[models.Model])
def get_models(
    context: Context = Depends(get_loaded_context),
) -> t.List[models.Model]:
    """Get a mapping of model names to model metadata"""
    return [
        models.Model(
            name=model.name,
            path=str(model._path.relative_to(context.path)),
            description=model.description,
            owner=model.owner,
            dialect=model.dialect,
            columns=[
                models.Column(
                    name=name, type=str(data_type), description=model.column_descriptions.get(name)
                )
                for name, data_type in model.columns_to_types.items()
            ],
        )
        for model in context.models.values()
    ]
