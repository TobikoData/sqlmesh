import typing as t

from fastapi import APIRouter, Depends

from sqlmesh.core.context import Context
from web.server.models import Column, Model
from web.server.settings import get_loaded_context

router = APIRouter()


@router.get("", response_model=t.List[Model])
def get_models(
    context: Context = Depends(get_loaded_context),
) -> t.List[Model]:
    """Get a mapping of model names to model metadata"""

    return get_models_with_columns(context)


def get_models_with_columns(context: Context) -> t.List[Model]:
    return [get_model_with_columns(context, model.name) for model in context.models.values()]


def get_model_with_columns(context: Context, model_name: str) -> Model:
    model = context.models[model_name]

    return Model(
        name=model.name,
        path=str(model._path.relative_to(context.path)),
        description=model.description,
        owner=model.owner,
        dialect=model.dialect,
        columns=[
            Column(name=name, type=str(data_type), description=model.column_descriptions.get(name))
            for name, data_type in model.columns_to_types.items()
        ],
    )
