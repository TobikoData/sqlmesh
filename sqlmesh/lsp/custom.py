from lsprotocol import types
import typing as t
from sqlmesh.utils.pydantic import PydanticModel


class CustomMethodRequestBaseClass(PydanticModel):
    pass


class CustomMethodResponseBaseClass(PydanticModel):
    # Prefixing, so guaranteed not to collide
    response_error: t.Optional[str] = None


ALL_MODELS_FEATURE = "sqlmesh/all_models"


class AllModelsRequest(CustomMethodRequestBaseClass):
    """
    Request to get all the models that are in the current project.
    """

    textDocument: types.TextDocumentIdentifier


class MacroCompletion(PydanticModel):
    """Information about a macro for autocompletion."""

    name: str
    description: t.Optional[str] = None


class ModelCompletion(PydanticModel):
    """Information about a model for autocompletion."""

    name: str
    description: t.Optional[str] = None


class AllModelsResponse(CustomMethodResponseBaseClass):
    """Response to get all models that are in the current project."""

    #: Deprecated: use ``model_completions`` instead
    models: t.List[str]
    model_completions: t.List[ModelCompletion]
    keywords: t.List[str]
    macros: t.List[MacroCompletion]


RENDER_MODEL_FEATURE = "sqlmesh/render_model"


class RenderModelRequest(CustomMethodRequestBaseClass):
    textDocumentUri: str


class RenderModelEntry(PydanticModel):
    """
    An entry in the rendered model.
    """

    name: str
    fqn: str
    description: t.Optional[str] = None
    rendered_query: str


class RenderModelResponse(CustomMethodResponseBaseClass):
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


class AllModelsForRenderRequest(CustomMethodRequestBaseClass):
    pass


class AllModelsForRenderResponse(CustomMethodResponseBaseClass):
    """
    Response to get all the models that are in the current project for rendering purposes.
    """

    models: t.List[ModelForRendering]


SUPPORTED_METHODS_FEATURE = "sqlmesh/supported_methods"


class SupportedMethodsRequest(PydanticModel):
    """
    Request to get all supported custom LSP methods.
    """

    pass


class CustomMethod(PydanticModel):
    """
    Information about a custom LSP method.
    """

    name: str


class SupportedMethodsResponse(CustomMethodResponseBaseClass):
    """
    Response containing all supported custom LSP methods.
    """

    methods: t.List[CustomMethod]


FORMAT_PROJECT_FEATURE = "sqlmesh/format_project"


class FormatProjectRequest(CustomMethodRequestBaseClass):
    """
    Request to format all models in the current project.
    """

    pass


class FormatProjectResponse(CustomMethodResponseBaseClass):
    """
    Response to format project request.
    """

    pass
