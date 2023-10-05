from __future__ import annotations

import json
import typing as t
from functools import wraps

from airflow.api_connexion import security
from airflow.www.app import csrf
from flask import Blueprint, Response, jsonify, make_response, request

from sqlmesh.core import constants as c
from sqlmesh.core.snapshot import SnapshotId, SnapshotNameVersion
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.plan import PlanDagState, create_plan_dag_spec
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

sqlmesh_api_v1 = Blueprint(
    c.SQLMESH,
    __name__,
    url_prefix=f"/{common.SQLMESH_API_BASE_PATH}",
)


def check_authentication(func: t.Callable) -> t.Callable:
    @wraps(func)
    def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
        security.check_authentication()
        return func(*args, **kwargs)

    return wrapper


@sqlmesh_api_v1.route("/plans", methods=["POST"])
@csrf.exempt
@check_authentication
def apply_plan() -> Response:
    try:
        plan = common.PlanApplicationRequest.parse_obj(request.json or {})
        with util.scoped_state_sync() as state_sync:
            spec = create_plan_dag_spec(plan, state_sync)
            PlanDagState.from_state_sync(state_sync).add_dag_spec(spec)
            return make_response(jsonify(request_id=spec.request_id), 201)
    except Exception as ex:
        return _error(str(ex))


@sqlmesh_api_v1.route("/environments/<name>")
@csrf.exempt
@check_authentication
def get_environment(name: str) -> Response:
    with util.scoped_state_sync() as state_sync:
        environment = state_sync.get_environment(name)
    if environment is None:
        return _error(f"Environment '{name}' was not found", 404)
    return _success(environment)


@sqlmesh_api_v1.route("/environments")
@csrf.exempt
@check_authentication
def get_environments() -> Response:
    with util.scoped_state_sync() as state_sync:
        environments = state_sync.get_environments()
    return _success(common.EnvironmentsResponse(environments=environments))


@sqlmesh_api_v1.route("/environments/<name>/max_interval_end")
@csrf.exempt
@check_authentication
def get_max_interval_end(name: str) -> Response:
    with util.scoped_state_sync() as state_sync:
        max_interval_end = state_sync.max_interval_end_for_environment(name)
        response = common.MaxIntervalEndResponse(
            environment=name, max_interval_end=max_interval_end
        )
        return _success(response)


@sqlmesh_api_v1.route("/environments/<name>", methods=["DELETE"])
@csrf.exempt
@check_authentication
def invalidate_environment(name: str) -> Response:
    with util.scoped_state_sync() as state_sync:
        try:
            state_sync.invalidate_environment(name)
        except SQLMeshError as ex:
            return _error(str(ex), 400)

    return _success(common.InvalidateEnvironmentResponse(name=name))


@sqlmesh_api_v1.route("/snapshots")
@csrf.exempt
@check_authentication
def get_snapshots() -> Response:
    with util.scoped_state_sync() as state_sync:
        snapshot_ids = _snapshot_ids_from_request()

        if "check_existence" in request.args:
            existing_snapshot_ids = (
                state_sync.snapshots_exist(snapshot_ids) if snapshot_ids is not None else set()
            )
            return _success(common.SnapshotIdsResponse(snapshot_ids=existing_snapshot_ids))

        hydrate_seeds = "hydrate_seeds" in request.args
        snapshots = list(
            state_sync.get_snapshots(snapshot_ids, hydrate_seeds=hydrate_seeds).values()
        )

        return _success(common.SnapshotsResponse(snapshots=snapshots))


@sqlmesh_api_v1.route("/models")
@csrf.exempt
@check_authentication
def nodes_exist() -> Response:
    with util.scoped_state_sync() as state_sync:
        names = _csv_arg("names")
        exclude_external = "exclude_external" in request.args
        existing_models = state_sync.nodes_exist(names, exclude_external=exclude_external)
        return _success(common.ExistingModelsResponse(names=list(existing_models)))


@sqlmesh_api_v1.route("/versions")
@csrf.exempt
@check_authentication
def get_versions() -> Response:
    with util.scoped_state_sync() as state_sync:
        versions = state_sync.get_versions()
        assert versions
    return _success(versions)


T = t.TypeVar("T", bound=PydanticModel)


def _success(data: T, status_code: int = 200) -> Response:
    response = make_response(data.json(), status_code)
    response.mimetype = "application/json"
    return response


def _error(message: str, status_code: int = 400) -> Response:
    return make_response(jsonify(message=message), status_code)


def _snapshot_ids_from_request() -> t.Optional[t.List[SnapshotId]]:
    if "ids" not in request.args:
        return None

    raw_ids = json.loads(request.args["ids"])
    return [SnapshotId.parse_obj(i) for i in raw_ids]


def _snapshot_name_versions_from_request() -> t.Optional[t.List[SnapshotNameVersion]]:
    if "versions" not in request.args:
        return None

    raw_versions = json.loads(request.args["versions"])
    return [SnapshotNameVersion.parse_obj(v) for v in raw_versions]


def _csv_arg(arg: str) -> t.List[str]:
    if arg not in request.args:
        return []
    return [v.strip() for v in request.args[arg].split(",")]
