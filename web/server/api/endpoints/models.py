import typing as t
from pathlib import Path

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context, Model
from web.server import models
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("", response_model=t.List[models.Model])
def get_models(
    context: Context = Depends(get_loaded_context),
) -> t.List[models.Model]:
    """Get a mapping of model names to model metadata"""

    return get_models_with_columns(context)


def get_models_with_columns(context: Context) -> t.List[models.Model]:
    return [get_model_with_columns(model, context.path) for model in context.models.values()]


def get_model_with_columns(model: Model, path: Path) -> models.Model:
    columns = [
        models.Column(
            name=name, type=str(data_type), description=model.column_descriptions.get(name)
        )
        for name, data_type in model.columns_to_types.items()
    ]
    return models.Model(
        name=model.name,
        path=str(model._path.relative_to(path)),
        description=model.description,
        owner=model.owner,
        dialect=model.dialect,
        columns=columns,
    )
