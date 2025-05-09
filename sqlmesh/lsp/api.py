"""
This module maps the LSP custom API calls to the SQLMesh web api.

Allowing the LSP to call the web api without having to know the details of the web api
and thus passing through the details of the web api to the LSP, so that both the LSP
and the web api can communicate with the same process, avoiding the need to have a
separate process for the web api.
"""

import typing as t
from sqlmesh.utils.pydantic import PydanticModel
from web.server.models import Model

API_FEATURE = "sqlmesh/api"


class ApiRequest(PydanticModel):
    """
    Request to call the SQLMesh API.
    This is a generic request that can be used to call any API endpoint.
    """

    requestId: str
    url: str
    method: t.Optional[str] = "GET"
    params: t.Optional[t.Dict[str, t.Any]] = None
    body: t.Optional[t.Dict[str, t.Any]] = None


class ApiResponse(PydanticModel):
    """
    Response from the SQLMesh API.
    This is a generic base class that can be used to return data from any API endpoint.
    Specific API responses should inherit from this class and specify the data type more precisely.
    """

    data: t.Union[t.Dict[str, t.Any], t.List[t.Any]]


class ApiResponseGetModels(ApiResponse):
    """
    Response from the SQLMesh API for the get_models endpoint.
    Specifies the data type more precisely as a list of models.
    """

    data: t.List[Model]

    def __init__(self, **data: t.Any) -> None:
        # Convert datetime objects to strings before passing to parent constructor
        if "data" in data and isinstance(data["data"], list):
            for model in data["data"]:
                if hasattr(model, "details") and model.details:
                    # Convert datetime fields to None or string to avoid serialization issues
                    for field in ["stamp", "start", "cron_prev", "cron_next"]:
                        if (
                            hasattr(model.details, field)
                            and getattr(model.details, field) is not None
                        ):
                            setattr(model.details, field, None)

        super().__init__(**data)


class ApiResponseGetLineage(ApiResponse):
    """
    Response from the SQLMesh API for the get_lineage endpoint.
    Specifies the data type more precisely as a list of models.
    """

    data: t.Dict[str, t.List[str]]
