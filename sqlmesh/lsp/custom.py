from lsprotocol import types
import typing as t
from sqlmesh.utils.pydantic import PydanticModel

ALL_MODELS_FEATURE = "sqlmesh/all_models"


class AllModelsRequest(PydanticModel):
    """
    Request to get all the models that are in the current project.
    """

    textDocument: types.TextDocumentIdentifier


class AllModelsResponse(PydanticModel):
    """
    Response to get all the models that are in the current project.
    """

    models: t.List[str]
    keywords: t.List[str]


RENDER_MODEL_FEATURE = "sqlmesh/render_model"


class RenderModelRequest(PydanticModel):
    textDocumentUri: str


class RenderModelEntry(PydanticModel):
    """
    An entry in the rendered model.
    """

    name: str
    fqn: str
    description: t.Optional[str] = None
    rendered_query: str


class RenderModelResponse(PydanticModel):
    """
    Response to render a model.
    """

    models: t.List[RenderModelEntry]


ALL_MODELS_FOR_RENDER_FEATURE = "sqlmesh/all_models_for_render"


class ModelForRendering(PydanticModel):
    """
    A model that is available for rendering.
    """

    name: str
    fqn: str
    description: t.Optional[str] = None
    uri: str


class AllModelsForRenderRequest(PydanticModel):
    pass


class AllModelsForRenderResponse(PydanticModel):
    """
    Response to get all the models that are in the current project for rendering purposes.
    """

    models: t.List[ModelForRendering]
