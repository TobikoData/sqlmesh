"""
This module maps the LSP custom API calls to the SQLMesh web api.

Allowing the LSP to call the web api without having to know the details of the web api
and thus passing through the details of the web api to the LSP, so that both the LSP
and the web api can communicate with the same process, avoiding the need to have a
separate process for the web api.
"""

import typing as t
from pydantic import field_validator
from sqlmesh.lsp.custom import (
    CustomMethodRequestBaseClass,
    CustomMethodResponseBaseClass,
)
from web.server.models import LineageColumn, Model

API_FEATURE = "sqlmesh/api"


class ApiRequest(CustomMethodRequestBaseClass):
    """
    Request to call the SQLMesh API.
    This is a generic request that can be used to call any API endpoint.
    """

    requestId: str
    url: str
    method: t.Optional[str] = "GET"
    params: t.Optional[t.Dict[str, t.Any]] = None
    body: t.Optional[t.Dict[str, t.Any]] = None


class BaseAPIResponse(CustomMethodResponseBaseClass):
    error: t.Optional[str] = None


class ApiResponseGetModels(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_models endpoint.
    """

    data: t.List[Model]

    @field_validator("data", mode="before")
    def sanitize_datetime_fields(cls, data: t.List[Model]) -> t.List[Model]:
        """
        Convert datetime objects to None to avoid serialization issues.
        """
        if isinstance(data, list):
            for model in data:
                if hasattr(model, "details") and model.details:
                    # Convert datetime fields to None to avoid serialization issues
                    for field in ["stamp", "start", "cron_prev", "cron_next"]:
                        if (
                            hasattr(model.details, field)
                            and getattr(model.details, field) is not None
                        ):
                            setattr(model.details, field, None)
        return data


class ApiResponseGetLineage(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_lineage endpoint.
    """

    data: t.Dict[str, t.List[str]]


class ApiResponseGetColumnLineage(BaseAPIResponse):
    """
    Response from the SQLMesh API for the get_column_lineage endpoint.
    """

    data: t.Dict[str, t.Dict[str, LineageColumn]]
