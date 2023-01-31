from __future__ import annotations

import typing as t

from airflow.api_connexion import security
from airflow.models import Variable
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.www.app import csrf
from flask import Blueprint, Response, jsonify, make_response, request
from sqlalchemy.orm import Session

from sqlmesh.core import constants as c
from sqlmesh.core.state_sync import StateSync
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.plan import create_plan_dag_spec
from sqlmesh.schedulers.airflow.state_sync.variable import VariableStateSync
from sqlmesh.utils.pydantic import PydanticModel

sqlmesh_api_v1 = Blueprint(
    c.SQLMESH,
    __name__,
    url_prefix=f"/{c.SQLMESH}/api/v1",
)


@sqlmesh_api_v1.route("/plans", methods=["POST"])
@csrf.exempt
@security.requires_access(
    [(permissions.ACTION_CAN_CREATE, permissions.RESOURCE_VARIABLE)]
)
def apply_plan() -> Response:
    try:
        plan = common.PlanApplicationRequest.parse_obj(request.json or {})
        spec = create_plan_dag_spec(plan)
    except Exception as ex:
        return _error(str(ex))

    Variable.set(common.plan_dag_spec_key(spec.request_id), spec.json())

    return make_response(jsonify(request_id=spec.request_id), 201)


@sqlmesh_api_v1.route("/environments/<name>", methods=["GET"])
@csrf.exempt
@security.requires_access(
    [(permissions.ACTION_CAN_READ, permissions.RESOURCE_VARIABLE)]
)
def get_environment(name: str) -> Response:
    environment = _get_state_sync().get_environment(name)
    if environment is None:
        return _error(f"Environment '{name}' was not found", 404)
    return _success(environment)


T = t.TypeVar("T", bound=PydanticModel)


def _success(data: T, status_code: int = 200) -> Response:
    return make_response(jsonify(**data.dict()), status_code)


def _error(message: str, status_code: int = 400) -> Response:
    return make_response(jsonify(message=message), status_code)


@provide_session
def _get_state_sync(session: Session = util.PROVIDED_SESSION) -> StateSync:
    return VariableStateSync(session)
